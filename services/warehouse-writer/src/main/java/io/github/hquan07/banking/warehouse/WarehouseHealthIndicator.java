package io.github.hquan07.banking.warehouse;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component("warehouseHealthIndicator")
public class WarehouseHealthIndicator implements HealthIndicator {

    private final WarehouseWriterProperties properties;
    private final WarehouseRuntimeState runtimeState;
    private final JdbcTemplate jdbc;

    public WarehouseHealthIndicator(
            WarehouseWriterProperties properties,
            WarehouseRuntimeState runtimeState,
            JdbcTemplate jdbc
    ) {
        this.properties = properties;
        this.runtimeState = runtimeState;
        this.jdbc = jdbc;
    }

    @Override
    public Health health() {
        if (!properties.enabled()) {
            return Health.up().withDetail("warehouseWriter", "disabled").build();
        }
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
            Health.Builder builder = runtimeState.currentFailures().isEmpty() ? Health.up() : Health.down();
            builder.withDetail("database", "reachable");
            if (!runtimeState.lastSuccess().isEmpty()) {
                builder.withDetail("lastSuccess", runtimeState.lastSuccess());
            }
            if (!runtimeState.currentFailures().isEmpty()) {
                builder.withDetail("currentFailures", runtimeState.currentFailures());
            }
            return builder.build();
        } catch (Exception exception) {
            return Health.down(exception).build();
        }
    }
}
