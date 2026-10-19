package io.github.hquan07.banking.lake;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class LakeRuntimeState {

    private final Counter bronzeBatches;
    private final Counter curatedBatches;
    private final Counter failures;
    private final MeterRegistry registry;
    private final Map<String, String> currentFailures = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastSuccess = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> lastSuccessEpochSeconds = new ConcurrentHashMap<>();

    public LakeRuntimeState(MeterRegistry registry) {
        this.registry = registry;
        bronzeBatches = Counter.builder("banking_lake_batches_total").tag("layer", "bronze").register(registry);
        curatedBatches = Counter.builder("banking_lake_batches_total").tag("layer", "curated").register(registry);
        failures = Counter.builder("banking_lake_batch_failures_total").register(registry);
    }

    public void success(String layer) {
        if ("bronze".equals(layer)) {
            bronzeBatches.increment();
        } else {
            curatedBatches.increment();
        }
        Instant completedAt = Instant.now();
        lastSuccess.put(layer, completedAt);
        lastSuccessGauge(layer).set(completedAt.getEpochSecond());
        currentFailures.remove(layer);
    }

    public void failure(String layer, Exception exception) {
        failures.increment();
        currentFailures.put(layer, exception.getClass().getSimpleName() + ": " + exception.getMessage());
    }

    public Map<String, String> currentFailures() {
        return Map.copyOf(currentFailures);
    }

    public Map<String, Instant> lastSuccess() {
        return Map.copyOf(lastSuccess);
    }

    private AtomicLong lastSuccessGauge(String layer) {
        return lastSuccessEpochSeconds.computeIfAbsent(layer, key -> {
            AtomicLong value = new AtomicLong();
            io.micrometer.core.instrument.Gauge.builder(
                            "banking_lake_last_success_timestamp_seconds", value, AtomicLong::get
                    )
                    .description("Unix timestamp of the latest committed lake batch")
                    .tag("layer", key)
                    .register(registry);
            return value;
        });
    }
}
