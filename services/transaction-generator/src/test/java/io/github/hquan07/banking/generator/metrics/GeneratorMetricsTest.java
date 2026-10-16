package io.github.hquan07.banking.generator.metrics;

import io.github.hquan07.banking.generator.model.AttemptResult;
import io.github.hquan07.banking.generator.model.GeneratorScenario;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GeneratorMetricsTest {

    @Test
    void summarizesSuccessThroughputAndLatencyPercentiles() {
        GeneratorMetrics metrics = new GeneratorMetrics();
        for (int index = 1; index <= 100; index++) {
            int status = index <= 95 ? 201 : 409;
            metrics.record(new AttemptResult(
                    GeneratorScenario.NORMAL,
                    status,
                    Duration.ofMillis(index).toNanos(),
                    null
            ));
        }
        metrics.record(new AttemptResult(
                GeneratorScenario.NORMAL,
                0,
                Duration.ofMillis(101).toNanos(),
                "ConnectException"
        ));

        GeneratorSummary summary = metrics.summarize(Duration.ofSeconds(2));

        assertEquals(101, summary.requests());
        assertEquals(95, summary.accepted());
        assertEquals(5, summary.rejected());
        assertEquals(1, summary.transportFailures());
        assertEquals(50.5, summary.throughputPerSecond());
        assertEquals(51.0, summary.p50LatencyMs());
        assertEquals(96.0, summary.p95LatencyMs());
        assertEquals(100.0, summary.p99LatencyMs());
    }
}
