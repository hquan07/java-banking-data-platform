package io.github.hquan07.banking.lake;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LakeRuntimeStateTest {

    @Test
    void successOnlyClearsFailureForTheSameLayer() {
        LakeRuntimeState state = new LakeRuntimeState(new SimpleMeterRegistry());

        state.failure("bronze", new IllegalStateException("storage unavailable"));
        state.success("curated");

        assertTrue(state.currentFailures().containsKey("bronze"));
        assertFalse(state.currentFailures().containsKey("curated"));

        state.success("bronze");

        assertTrue(state.currentFailures().isEmpty());
    }
}
