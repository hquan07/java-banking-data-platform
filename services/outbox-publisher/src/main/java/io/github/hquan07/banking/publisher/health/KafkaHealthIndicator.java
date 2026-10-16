package io.github.hquan07.banking.publisher.health;

import io.github.hquan07.banking.publisher.config.PublisherProperties;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component("kafkaHealthIndicator")
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;
    private final PublisherProperties properties;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin, PublisherProperties properties) {
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
    }

    @Override
    public Health health() {
        if (!properties.enabled()) {
            return Health.up().withDetail("publisher", "disabled").build();
        }
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            String clusterId = admin.describeCluster().clusterId().get(3, TimeUnit.SECONDS);
            return Health.up().withDetail("clusterId", clusterId).build();
        } catch (Exception exception) {
            return Health.down(exception).build();
        }
    }
}
