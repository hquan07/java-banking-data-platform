package io.github.hquan07.banking.warehouse;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class WarehouseRuntimeState {

    private final MeterRegistry registry;
    private final Counter failures;
    private final Counter rejected;
    private final Map<String, String> currentFailures = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastSuccess = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> lastSuccessEpochSeconds = new ConcurrentHashMap<>();

    public WarehouseRuntimeState(MeterRegistry registry) {
        this.registry = registry;
        this.failures = Counter.builder("banking_warehouse_batch_failures_total").register(registry);
        this.rejected = Counter.builder("banking_warehouse_rejected_records_total").register(registry);
    }

    public void success(String stream, int records, int rejectedRecords) {
        Counter.builder("banking_warehouse_processed_records_total")
                .tag("stream", stream).register(registry).increment(records);
        if (rejectedRecords > 0) {
            rejected.increment(rejectedRecords);
        }
        Instant completedAt = Instant.now();
        lastSuccess.put(stream, completedAt);
        lastSuccessGauge(stream).set(completedAt.getEpochSecond());
        currentFailures.remove(stream);
    }

    public void failure(String stream, Exception exception) {
        failures.increment();
        currentFailures.put(stream, exception.getClass().getSimpleName() + ": " + exception.getMessage());
    }

    public void recovered(String stream) {
        currentFailures.remove(stream);
    }

    public Map<String, String> currentFailures() {
        return Map.copyOf(currentFailures);
    }

    public Map<String, Instant> lastSuccess() {
        return Map.copyOf(lastSuccess);
    }

    private AtomicLong lastSuccessGauge(String stream) {
        return lastSuccessEpochSeconds.computeIfAbsent(stream, key -> {
            AtomicLong value = new AtomicLong();
            io.micrometer.core.instrument.Gauge.builder(
                            "banking_warehouse_last_success_timestamp_seconds", value, AtomicLong::get
                    )
                    .description("Unix timestamp of the latest committed warehouse batch")
                    .tag("stream", key)
                    .register(registry);
            return value;
        });
    }
}
