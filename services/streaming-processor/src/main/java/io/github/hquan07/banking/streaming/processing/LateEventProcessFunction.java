package io.github.hquan07.banking.streaming.processing;

import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.common.event.FailureClass;
import io.github.hquan07.banking.common.event.ReplayStatus;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;

public final class LateEventProcessFunction
        extends ProcessFunction<ValidatedTransaction, ValidatedTransaction> {

    public static final OutputTag<DeadLetterEvent> LATE_EVENTS = new OutputTag<>("late-transactions") { };

    private final long allowedLatenessMillis;

    public LateEventProcessFunction(Duration allowedLateness) {
        this.allowedLatenessMillis = allowedLateness.toMillis();
    }

    @Override
    public void processElement(
            ValidatedTransaction transaction,
            ProcessFunction<ValidatedTransaction, ValidatedTransaction>.Context context,
            Collector<ValidatedTransaction> collector
    ) {
        long watermark = context.timerService().currentWatermark();
        long eventTimestamp = transaction.event().occurredAt().toEpochMilli();
        if (watermark != Long.MIN_VALUE && eventTimestamp < watermark - allowedLatenessMillis) {
            context.output(LATE_EVENTS, lateFailure(transaction));
            return;
        }
        collector.collect(transaction);
    }

    private DeadLetterEvent lateFailure(ValidatedTransaction transaction) {
        Instant failedAt = transaction.sourceTimestamp() >= 0
                ? Instant.ofEpochMilli(transaction.sourceTimestamp())
                : transaction.event().producedAt();
        return new DeadLetterEvent(
                transaction.event().eventId(),
                transaction.sourceTopic(),
                transaction.sourcePartition(),
                transaction.sourceOffset(),
                FailureClass.BUSINESS,
                "EVENT_TOO_LATE",
                "event timestamp is older than the configured watermark and allowed lateness",
                failedAt,
                0,
                DeadLetterEvent.SCHEMA_VERSION,
                transaction.payloadHash(),
                ReplayStatus.PENDING
        );
    }
}
