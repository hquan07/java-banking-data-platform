package io.github.hquan07.banking.streaming.processing;

import io.github.hquan07.banking.common.event.FraudAlertEvent;
import io.github.hquan07.banking.common.event.FraudRule;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import io.github.hquan07.banking.streaming.util.DeterministicIds;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public final class VelocityFraudWindowFunction
        extends ProcessWindowFunction<ValidatedTransaction, FraudAlertEvent, String, TimeWindow> {

    private final int threshold;

    public VelocityFraudWindowFunction(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void process(
            String accountId,
            ProcessWindowFunction<ValidatedTransaction, FraudAlertEvent, String, TimeWindow>.Context context,
            Iterable<ValidatedTransaction> transactions,
            Collector<FraudAlertEvent> collector
    ) {
        FraudAlertEvent alert = evaluate(
                accountId,
                context.window().getStart(),
                context.window().getEnd(),
                transactions,
                threshold
        );
        if (alert != null) {
            collector.collect(alert);
        }
    }

    static FraudAlertEvent evaluate(
            String accountId,
            long windowStartMillis,
            long windowEndMillis,
            Iterable<ValidatedTransaction> transactions,
            int threshold
    ) {
        long count = 0;
        ValidatedTransaction latest = null;
        for (ValidatedTransaction transaction : transactions) {
            count++;
            if (latest == null || transaction.event().occurredAt().isAfter(latest.event().occurredAt())) {
                latest = transaction;
            }
        }
        if (count <= threshold || latest == null) {
            return null;
        }

        Instant windowStart = Instant.ofEpochMilli(windowStartMillis);
        Instant windowEnd = Instant.ofEpochMilli(windowEndMillis);
        String identity = accountId + ":" + windowStartMillis + ":" + windowEndMillis;
        return new FraudAlertEvent(
                DeterministicIds.from("fraud-velocity-60s", identity),
                FraudRule.VELOCITY_60S,
                FraudAlertEvent.RULE_VERSION,
                null,
                accountId,
                null,
                latest.event().currency(),
                count,
                windowStart,
                windowEnd,
                windowEnd,
                FraudAlertEvent.SCHEMA_VERSION
        );
    }
}
