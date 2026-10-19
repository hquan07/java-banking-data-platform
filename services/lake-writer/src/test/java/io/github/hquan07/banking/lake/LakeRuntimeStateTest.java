package io.github.hquan07.banking.lake;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LakeRuntimeStateTest {

    @Test
    void successOnlyClearsFailureForTheSameLayer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        LakeRuntimeState state = new LakeRuntimeState(registry);

        state.failure("bronze", new IllegalStateException("storage unavailable"));
        state.success("curated");

        assertTrue(state.currentFailures().containsKey("bronze"));
        assertFalse(state.currentFailures().containsKey("curated"));

        state.success("bronze");

        assertTrue(state.currentFailures().isEmpty());
        var gauge = registry.find("banking_lake_last_success_timestamp_seconds")
                .tag("layer", "bronze").gauge();
        assertNotNull(gauge);
        assertTrue(gauge.value() > 0);
    }
}
