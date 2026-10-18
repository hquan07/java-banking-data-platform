package io.github.hquan07.banking.warehouse;

import io.github.hquan07.banking.warehouse.model.RecordKind;
import io.github.hquan07.banking.warehouse.model.WarehouseRecord;
import io.github.hquan07.banking.warehouse.persistence.WarehouseBatchRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(prefix = "warehouse", name = "enabled", havingValue = "true")
public class WarehouseConsumerCoordinator implements SmartLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarehouseConsumerCoordinator.class);

    private final WarehouseWriterProperties properties;
    private final WarehouseBatchRepository repository;
    private final WarehouseRuntimeState runtimeState;
    private volatile boolean running;
    private ExecutorService executor;

    public WarehouseConsumerCoordinator(
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
        executor = Executors.newFixedThreadPool(3, Thread.ofPlatform().name("warehouse-consumer-", 0).factory());
        executor.submit(() -> consume("validated", properties.validatedTopic(), properties.validatedGroupId(), RecordKind.TRANSACTION));
        executor.submit(() -> consume("fraud", properties.fraudTopic(), properties.fraudGroupId(), RecordKind.FRAUD_ALERT));
        executor.submit(() -> consume("aggregates", properties.aggregatesTopic(), properties.aggregatesGroupId(), RecordKind.AGGREGATE));
        LOGGER.info("Warehouse writer started with three isolated Kafka consumer groups");
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    private void consume(String stream, String topic, String groupId, RecordKind kind) {
        while (running) {
            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties(stream, groupId))) {
                consumer.subscribe(List.of(topic));
                while (running) {
                    var polled = consumer.poll(properties.pollTimeout());
                    if (polled.isEmpty()) {
                        continue;
                    }
                    List<WarehouseRecord> batch = new ArrayList<>(polled.count());
                    polled.forEach(record -> batch.add(WarehouseRecord.from(record)));
                    try {
                        var result = repository.process(kind, stream, batch);
                        consumer.commitSync(commitOffsets(batch));
                        runtimeState.success(stream, batch.size(), result.invalid());
                        LOGGER.info("Warehouse {} batch committed: raw={}, valid={}, invalid={}, inserted={}, updated={}, duplicateDelivery={}",
                                stream, batch.size(), result.valid(), result.invalid(), result.inserted(), result.updated(), result.duplicates());
                    } catch (Exception exception) {
                        runtimeState.failure(stream, exception);
                        LOGGER.error("Warehouse {} batch failed; Kafka offsets remain uncommitted", stream, exception);
                        rewind(consumer, batch);
                        pause();
                    }
                }
            } catch (Exception exception) {
                runtimeState.failure(stream, exception);
                LOGGER.error("Warehouse {} consumer session failed; recreating consumer", stream, exception);
                pause();
            }
        }
    }

    private Properties consumerProperties(String stream, String groupId) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.kafkaBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "warehouse-" + stream);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.startingOffsets());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.maxPollRecords());
        return config;
    }

    private static Map<TopicPartition, OffsetAndMetadata> commitOffsets(List<WarehouseRecord> records) {
        Map<TopicPartition, Long> highest = new HashMap<>();
        for (WarehouseRecord record : records) {
            highest.merge(new TopicPartition(record.topic(), record.partition()), record.offset(), Math::max);
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        highest.forEach((partition, offset) -> offsets.put(partition, new OffsetAndMetadata(offset + 1)));
        return offsets;
    }

    private static void rewind(KafkaConsumer<String, byte[]> consumer, List<WarehouseRecord> records) {
        Map<TopicPartition, Long> earliest = new HashMap<>();
        for (WarehouseRecord record : records) {
            earliest.merge(new TopicPartition(record.topic(), record.partition()), record.offset(), Math::min);
        }
        earliest.forEach(consumer::seek);
    }

    private void pause() {
        try {
            Thread.sleep(properties.retryBackoff());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
