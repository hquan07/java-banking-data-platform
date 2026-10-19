package io.github.hquan07.banking.warehouse;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WarehouseRuntimeStateTest {

    @Test
    void exposesTheLastSuccessfulCommitTimestampPerStream() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        WarehouseRuntimeState state = new WarehouseRuntimeState(registry);

        state.failure("validated", new IllegalStateException("temporary failure"));
        state.success("validated", 3, 1);

        assertTrue(state.currentFailures().isEmpty());
        var gauge = registry.find("banking_warehouse_last_success_timestamp_seconds")
                .tag("stream", "validated").gauge();
        assertNotNull(gauge);
        assertTrue(gauge.value() > 0);
    }
}
