package io.github.hquan07.banking.lake;

import io.github.hquan07.banking.lake.storage.LakeObjectStore;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("lakeHealthIndicator")
public class LakeHealthIndicator implements HealthIndicator {

    private final LakeWriterProperties properties;
    private final LakeObjectStore objectStore;
    private final LakeRuntimeState runtimeState;

    public LakeHealthIndicator(
            LakeWriterProperties properties,
            LakeObjectStore objectStore,
            LakeRuntimeState runtimeState
    ) {
        this.properties = properties;
        this.objectStore = objectStore;
        this.runtimeState = runtimeState;
    }

    @Override
    public Health health() {
        if (!properties.enabled()) {
            return Health.up().withDetail("lakeWriter", "disabled").build();
        }
        try {
            objectStore.verifyBucket();
            Health.Builder health = runtimeState.currentFailures().isEmpty() ? Health.up() : Health.down();
            health.withDetail("bucket", properties.bucket());
            if (!runtimeState.lastSuccess().isEmpty()) {
                health.withDetail("lastSuccess", runtimeState.lastSuccess());
            }
            if (!runtimeState.currentFailures().isEmpty()) {
                health.withDetail("currentFailures", runtimeState.currentFailures());
            }
            return health.build();
        } catch (Exception exception) {
            return Health.down(exception).build();
        }
    }
}
