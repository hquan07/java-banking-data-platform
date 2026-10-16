package io.github.hquan07.banking.publisher.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class PublisherMetrics {

    private final Counter published;
    private final Counter failures;
    private final Counter retries;
    private final Timer publishLatency;
    private final AtomicLong backlog = new AtomicLong();

    public PublisherMetrics(MeterRegistry meterRegistry) {
        published = Counter.builder("banking.outbox.published")
                .description("Outbox events acknowledged by Kafka and marked published")
                .register(meterRegistry);
        failures = Counter.builder("banking.outbox.publish.failures")
                .description("Outbox publish attempts that failed")
                .register(meterRegistry);
        retries = Counter.builder("banking.outbox.retries")
                .description("Outbox claims after the first publish attempt")
                .register(meterRegistry);
        publishLatency = Timer.builder("banking.outbox.publish.latency")
                .description("Kafka acknowledgement latency for outbox events")
                .register(meterRegistry);
        Gauge.builder("banking.outbox.backlog", backlog, AtomicLong::get)
                .description("Current unpublished outbox row count")
                .register(meterRegistry);
    }

    public void recordPublished(Duration latency) {
        published.increment();
        publishLatency.record(latency);
    }

    public void recordFailure() {
        failures.increment();
    }

    public void recordRetry() {
        retries.increment();
    }

    public void updateBacklog(long value) {
        backlog.set(value);
    }
}
