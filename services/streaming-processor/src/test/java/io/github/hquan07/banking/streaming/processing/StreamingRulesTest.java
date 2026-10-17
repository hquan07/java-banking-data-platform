package io.github.hquan07.banking.streaming.processing;

import io.github.hquan07.banking.common.event.FraudAlertEvent;
import io.github.hquan07.banking.common.event.FraudRule;
import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.common.event.TransactionAggregateEvent;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class StreamingRulesTest {

    @Test
    void highValueRuleOnlyAlertsForVndAtOrAboveThreshold() throws Exception {
        try (OneInputStreamOperatorTestHarness<ValidatedTransaction, FraudAlertEvent> harness =
                     ProcessFunctionTestHarnesses.forProcessFunction(
                             new HighValueFraudFunction(new BigDecimal("100000000.00")))) {
            ValidatedTransaction transaction = transaction(
                    UUID.randomUUID(), new BigDecimal("100000000.00"), "VND", TransactionStatus.SUCCESS, 0
            );

            harness.processElement(new StreamRecord<>(transaction));

            FraudAlertEvent alert = harness.extractOutputValues().getFirst();
            assertEquals(FraudRule.HIGH_VALUE, alert.rule());
            assertEquals(transaction.event().eventId(), alert.eventId());
            assertEquals(1, alert.observedCount());
        }
    }

    @Test
    void velocityRuleRequiresMoreThanTenDeduplicatedEvents() {
        List<ValidatedTransaction> ten = transactions(10);
        List<ValidatedTransaction> eleven = transactions(11);

        assertNull(VelocityFraudWindowFunction.evaluate("ACC10001", 0, 60_000, ten, 10));
        FraudAlertEvent alert = VelocityFraudWindowFunction.evaluate(
                "ACC10001", 0, 60_000, eleven, 10
        );

        assertNotNull(alert);
        assertEquals(FraudRule.VELOCITY_60S, alert.rule());
        assertEquals(11, alert.observedCount());
    }

    @Test
    void aggregateKeepsCurrencyBoundaryAndCountsFailures() {
        List<ValidatedTransaction> transactions = List.of(
                transaction(UUID.randomUUID(), new BigDecimal("10.00"), "VND", TransactionStatus.SUCCESS, 0),
                transaction(UUID.randomUUID(), new BigDecimal("20.00"), "VND", TransactionStatus.FAILED, 1)
        );

        TransactionAggregateEvent aggregate = AggregateWindowFunction.aggregate(
                "VND", 0, 60_000, transactions
        );

        assertNotNull(aggregate);
        assertEquals(2, aggregate.transactionCount());
        assertEquals(new BigDecimal("30.00"), aggregate.totalAmount());
        assertEquals(1, aggregate.failedCount());
        assertEquals("VND:0", aggregate.windowKey());
    }

    @Test
    void eventBeyondAllowedLatenessUsesInvalidSideOutput() throws Exception {
        try (OneInputStreamOperatorTestHarness<ValidatedTransaction, ValidatedTransaction> harness =
                     ProcessFunctionTestHarnesses.forProcessFunction(
                             new LateEventProcessFunction(Duration.ofSeconds(30)))) {
            ValidatedTransaction late = transaction(
                    UUID.randomUUID(), new BigDecimal("10.00"), "VND", TransactionStatus.SUCCESS, 0
            );
            harness.processWatermark(new Watermark(late.event().occurredAt().plusSeconds(60).toEpochMilli()));
            harness.processElement(new StreamRecord<>(late, late.event().occurredAt().toEpochMilli()));

            List<DeadLetterEvent> failures = harness
                    .getSideOutput(LateEventProcessFunction.LATE_EVENTS)
                    .stream()
                    .map(StreamRecord::getValue)
                    .toList();
            assertEquals(1, failures.size());
            assertEquals("EVENT_TOO_LATE", failures.getFirst().failureCode());
            assertEquals(0, harness.extractOutputValues().size());
        }
    }

    @Test
    void deduplicationStateSurvivesCheckpointRestore() throws Exception {
        ValidatedTransaction original = transaction(
                UUID.randomUUID(), new BigDecimal("10.00"), "VND", TransactionStatus.SUCCESS, 0
        );
        OperatorSubtaskState snapshot;

        try (KeyedOneInputStreamOperatorTestHarness<String, ValidatedTransaction, ValidatedTransaction> first =
                     harness()) {
            first.setup();
            first.open();
            first.setProcessingTime(1_000);
            first.processElement(new StreamRecord<>(original));
            first.processElement(new StreamRecord<>(original));
            assertEquals(1, first.extractOutputValues().size());
            snapshot = first.snapshot(1, 1_000);
        }

        try (KeyedOneInputStreamOperatorTestHarness<String, ValidatedTransaction, ValidatedTransaction> restored =
                     harness()) {
            restored.setup();
            restored.initializeState(snapshot);
            restored.open();
            restored.setProcessingTime(2_000);
            restored.processElement(new StreamRecord<>(original));
            restored.processElement(new StreamRecord<>(transaction(
                    UUID.randomUUID(), new BigDecimal("20.00"), "VND", TransactionStatus.SUCCESS, 1
            )));

            assertEquals(1, restored.extractOutputValues().size());
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<String, ValidatedTransaction, ValidatedTransaction> harness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DeduplicateByEventId(Duration.ofHours(1))),
                value -> value.event().eventId().toString(),
                BasicTypeInfo.STRING_TYPE_INFO
        );
    }

    private List<ValidatedTransaction> transactions(int count) {
        List<ValidatedTransaction> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(transaction(
                    UUID.randomUUID(), new BigDecimal("10.00"), "VND", TransactionStatus.SUCCESS, index
            ));
        }
        return values;
    }

    private ValidatedTransaction transaction(
            UUID eventId,
            BigDecimal amount,
            String currency,
            TransactionStatus status,
            int secondOffset
    ) {
        Instant occurredAt = Instant.parse("2026-10-17T02:30:00Z").plusSeconds(secondOffset);
        TransactionCreatedEvent event = new TransactionCreatedEvent(
                eventId,
                TransactionCreatedEvent.EVENT_TYPE,
                TransactionCreatedEvent.SCHEMA_VERSION,
                UUID.randomUUID(),
                "ACC10001",
                TransactionType.PAYMENT,
                amount,
                currency,
                status,
                occurredAt,
                occurredAt.plusMillis(100),
                "phase5-test"
        );
        return new ValidatedTransaction(
                event,
                "banking.transactions.raw",
                0,
                secondOffset,
                occurredAt.toEpochMilli(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }
}
