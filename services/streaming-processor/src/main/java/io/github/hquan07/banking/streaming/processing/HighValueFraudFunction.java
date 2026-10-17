package io.github.hquan07.banking.streaming.processing;

import io.github.hquan07.banking.common.event.FraudAlertEvent;
import io.github.hquan07.banking.common.event.FraudRule;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import io.github.hquan07.banking.streaming.util.DeterministicIds;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

public final class HighValueFraudFunction
        extends ProcessFunction<ValidatedTransaction, FraudAlertEvent> {

    private final BigDecimal threshold;

    public HighValueFraudFunction(BigDecimal threshold) {
        this.threshold = threshold;
    }

    @Override
    public void processElement(
            ValidatedTransaction transaction,
            ProcessFunction<ValidatedTransaction, FraudAlertEvent>.Context context,
            Collector<FraudAlertEvent> collector
    ) {
        if (!"VND".equals(transaction.event().currency())
                || transaction.event().amount().compareTo(threshold) < 0) {
            return;
        }

        Instant occurredAt = transaction.event().occurredAt();
        collector.collect(new FraudAlertEvent(
                DeterministicIds.from("fraud-high-value", transaction.event().eventId().toString()),
                FraudRule.HIGH_VALUE,
                FraudAlertEvent.RULE_VERSION,
                transaction.event().eventId(),
                transaction.event().accountId(),
                transaction.event().amount(),
                transaction.event().currency(),
                1,
                occurredAt,
                occurredAt.plus(Duration.ofMillis(1)),
                transaction.event().producedAt(),
                FraudAlertEvent.SCHEMA_VERSION
        ));
    }
}
