package io.github.hquan07.banking.warehouse;

import io.github.hquan07.banking.warehouse.persistence.WarehouseBatchRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(prefix = "warehouse", name = "enabled", havingValue = "true")
public class WarehouseDlqPublisher implements SmartLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarehouseDlqPublisher.class);

    private final WarehouseWriterProperties properties;
    private final WarehouseBatchRepository repository;
    private final WarehouseRuntimeState runtimeState;
    private volatile boolean running;
    private ExecutorService executor;

    public WarehouseDlqPublisher(
            WarehouseWriterProperties properties,
            WarehouseBatchRepository repository,
            WarehouseRuntimeState runtimeState
    ) {
        this.properties = properties;
        this.repository = repository;
        this.runtimeState = runtimeState;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        executor = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("warehouse-dlq-publisher").factory());
        executor.submit(this::publishLoop);
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 90;
    }

    private void publishLoop() {
        while (running) {
            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties())) {
                while (running) {
                    var pending = repository.pendingDlq(100);
                    for (var record : pending) {
                        try {
                            producer.send(new ProducerRecord<>(properties.dlqTopic(), record.messageKey(), record.payload()))
                                    .get(10, TimeUnit.SECONDS);
                            repository.markDlqPublished(record.failureId());
                            runtimeState.recovered("dlq");
                        } catch (Exception exception) {
                            runtimeState.failure("dlq", exception);
                            repository.markDlqFailed(record.failureId(), exception);
                            LOGGER.error("Warehouse DLQ publish failed for {}", record.failureId(), exception);
                            sleep(properties.dlqPublishInterval());
                        }
                    }
                    if (pending.isEmpty()) {
                        sleep(properties.dlqPublishInterval());
                    }
                }
            } catch (Exception exception) {
                runtimeState.failure("dlq", exception);
                LOGGER.error("Warehouse DLQ publisher session failed; recreating producer", exception);
                sleep(properties.retryBackoff());
            }
        }
    }

    private Properties producerProperties() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.kafkaBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "warehouse-dlq-publisher");
        return config;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
