package io.github.hquan07.banking.streaming.processing;

import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public final class DeduplicateByEventId
        extends KeyedProcessFunction<String, ValidatedTransaction, ValidatedTransaction> {

    private final long retentionMillis;
    private transient ValueState<Long> expiresAt;

    public DeduplicateByEventId(Duration retention) {
        this.retentionMillis = retention.toMillis();
    }

    @Override
    public void open(OpenContext context) {
        expiresAt = getRuntimeContext().getState(
                new ValueStateDescriptor<>("event-id-expires-at", Long.class)
        );
    }

    @Override
    public void processElement(
            ValidatedTransaction transaction,
            KeyedProcessFunction<String, ValidatedTransaction, ValidatedTransaction>.Context context,
            Collector<ValidatedTransaction> collector
    ) throws Exception {
        long now = context.timerService().currentProcessingTime();
        Long currentExpiry = expiresAt.value();
        if (currentExpiry != null && currentExpiry > now) {
            return;
        }

        collector.collect(transaction);
        long newExpiry = Math.addExact(now, retentionMillis);
        expiresAt.update(newExpiry);
        context.timerService().registerProcessingTimeTimer(newExpiry);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<String, ValidatedTransaction, ValidatedTransaction>.OnTimerContext context,
            Collector<ValidatedTransaction> collector
    ) throws Exception {
        Long currentExpiry = expiresAt.value();
        if (currentExpiry != null && currentExpiry == timestamp) {
            expiresAt.clear();
        }
    }
}
