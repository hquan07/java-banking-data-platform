package io.github.hquan07.banking.lake;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LakeRuntimeState {

    private final Counter bronzeBatches;
    private final Counter curatedBatches;
    private final Counter failures;
    private final Map<String, String> currentFailures = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastSuccess = new ConcurrentHashMap<>();

    public LakeRuntimeState(MeterRegistry registry) {
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
        lastSuccess.put(layer, Instant.now());
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
}
