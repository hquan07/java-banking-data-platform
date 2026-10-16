package io.github.hquan07.banking.generator.metrics;

import io.github.hquan07.banking.generator.model.AttemptResult;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

@Component
public class GeneratorMetrics {

    private final LongAdder requests = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder transportFailures = new LongAdder();
    private final ConcurrentLinkedQueue<Long> latencyNanos = new ConcurrentLinkedQueue<>();

    public void record(AttemptResult result) {
        requests.increment();
        latencyNanos.add(result.latencyNanos());
        if (result.transportFailure()) {
            transportFailures.increment();
        } else if (result.accepted()) {
            accepted.increment();
        } else {
            rejected.increment();
        }
    }

    public GeneratorSummary summarize(Duration elapsed) {
        long total = requests.sum();
        long acceptedCount = accepted.sum();
        long[] sortedLatencies = latencyNanos.stream().mapToLong(Long::longValue).sorted().toArray();
        double elapsedSeconds = Math.max(elapsed.toNanos() / 1_000_000_000.0, 0.000_001);
        return new GeneratorSummary(
                total,
                acceptedCount,
                rejected.sum(),
                transportFailures.sum(),
                total == 0 ? 0 : acceptedCount * 100.0 / total,
                total / elapsedSeconds,
                percentileMillis(sortedLatencies, 0.50),
                percentileMillis(sortedLatencies, 0.95),
                percentileMillis(sortedLatencies, 0.99)
        );
    }

    private double percentileMillis(long[] sortedValues, double percentile) {
        if (sortedValues.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(percentile * sortedValues.length) - 1;
        long nanos = sortedValues[Math.max(0, Math.min(index, sortedValues.length - 1))];
        return nanos / 1_000_000.0;
    }
}
