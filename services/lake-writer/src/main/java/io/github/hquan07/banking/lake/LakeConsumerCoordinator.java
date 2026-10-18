package io.github.hquan07.banking.lake;

import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.storage.LakeObjectStore;
import io.github.hquan07.banking.lake.writer.BronzeBatchWriter;
import io.github.hquan07.banking.lake.writer.CuratedBatchWriter;
import io.github.hquan07.banking.lake.writer.LakeBatchSink;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(prefix = "lake", name = "enabled", havingValue = "true")
public class LakeConsumerCoordinator implements SmartLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(LakeConsumerCoordinator.class);

    private final LakeWriterProperties properties;
    private final LakeObjectStore objectStore;
    private final BronzeBatchWriter bronzeWriter;
    private final CuratedBatchWriter curatedWriter;
    private final LakeRuntimeState runtimeState;
    private volatile boolean running;
    private ExecutorService executor;

    public LakeConsumerCoordinator(
            LakeWriterProperties properties,
            LakeObjectStore objectStore,
            BronzeBatchWriter bronzeWriter,
            CuratedBatchWriter curatedWriter,
            LakeRuntimeState runtimeState
    ) {
        this.properties = properties;
        this.objectStore = objectStore;
        this.bronzeWriter = bronzeWriter;
        this.curatedWriter = curatedWriter;
        this.runtimeState = runtimeState;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        objectStore.verifyBucket();
        running = true;
        executor = Executors.newFixedThreadPool(2, Thread.ofPlatform().name("lake-consumer-", 0).factory());
        executor.submit(() -> consume("bronze", properties.rawTopic(), properties.rawGroupId(), bronzeWriter));
        executor.submit(() -> consume(
                "curated", properties.validatedTopic(), properties.curatedGroupId(), curatedWriter
        ));
        LOGGER.info("Lake writer started with micro-batches up to {} records and {} flush interval",
                properties.maxBatchRecords(), properties.flushInterval());
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
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

    private void consume(String layer, String topic, String groupId, LakeBatchSink sink) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties(layer, groupId))) {
            consumer.subscribe(List.of(topic));
            List<KafkaEnvelope> buffer = new ArrayList<>();
            long firstBufferedAt = 0;
            while (running) {
                try {
                    var polled = consumer.poll(properties.pollTimeout());
                    for (var record : polled) {
                        buffer.add(KafkaEnvelope.from(record));
                    }
                    if (firstBufferedAt == 0 && !buffer.isEmpty()) {
                        firstBufferedAt = System.nanoTime();
                    }
                    boolean full = buffer.size() >= properties.maxBatchRecords();
                    boolean expired = firstBufferedAt != 0
                            && Duration.ofNanos(System.nanoTime() - firstBufferedAt)
                            .compareTo(properties.flushInterval()) >= 0;
                    if (full || expired) {
                        flush(layer, consumer, sink, buffer);
                        buffer.clear();
                        firstBufferedAt = 0;
                    }
                } catch (Exception exception) {
                    runtimeState.failure(layer, exception);
                    LOGGER.error("{} lake batch failed; offsets remain uncommitted", layer, exception);
                    rewind(consumer, buffer);
                    buffer.clear();
                    firstBufferedAt = 0;
                    pause(properties.retryBackoff());
                }
            }
            if (!buffer.isEmpty()) {
                flush(layer, consumer, sink, buffer);
            }
        } catch (Exception exception) {
            runtimeState.failure(layer, exception);
            LOGGER.error("{} lake consumer stopped unexpectedly", layer, exception);
            running = false;
        }
    }

    private void flush(
            String layer,
            KafkaConsumer<String, byte[]> consumer,
            LakeBatchSink sink,
            List<KafkaEnvelope> records
    ) {
        sink.write(List.copyOf(records));
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (KafkaEnvelope record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            offsets.compute(partition, (ignored, current) -> {
                long next = record.offset() + 1;
                return current == null || next > current.offset() ? new OffsetAndMetadata(next) : current;
            });
        }
        consumer.commitSync(offsets);
        runtimeState.success(layer);
        LOGGER.info("Committed {} lake batch with {} Kafka records", layer, records.size());
    }

    private void rewind(KafkaConsumer<String, byte[]> consumer, List<KafkaEnvelope> records) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();
        for (KafkaEnvelope record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            firstOffsets.merge(partition, record.offset(), Math::min);
        }
        firstOffsets.forEach((partition, offset) -> {
            if (consumer.assignment().contains(partition)) {
                consumer.seek(partition, offset);
            }
        });
    }

    private Properties consumerProperties(String layer, String groupId) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.kafkaBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "banking-lake-" + layer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.startingOffsets());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.maxBatchRecords());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        return config;
    }

    private void pause(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
