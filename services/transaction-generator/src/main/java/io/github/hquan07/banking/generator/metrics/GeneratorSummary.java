package io.github.hquan07.banking.generator.metrics;

public record GeneratorSummary(
        long requests,
        long accepted,
        long rejected,
        long transportFailures,
        double successRate,
        double throughputPerSecond,
        double p50LatencyMs,
        double p95LatencyMs,
        double p99LatencyMs
) {
}
