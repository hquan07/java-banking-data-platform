package io.github.hquan07.banking.streaming.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public final class JsonKafkaRecordSerializer<T> implements KafkaRecordSerializationSchema<T> {

    private final String topic;
    private final KeyExtractor<T> keyExtractor;
    private transient ObjectMapper objectMapper;

    public JsonKafkaRecordSerializer(String topic, KeyExtractor<T> keyExtractor) {
        this.topic = topic;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context,
            KafkaSinkContext sinkContext
    ) {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T value, KafkaSinkContext context, Long timestamp) {
        try {
            String key = keyExtractor.key(value);
            return new ProducerRecord<>(
                    topic,
                    null,
                    timestamp,
                    key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                    mapper().writeValueAsBytes(value)
            );
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize Kafka record", exception);
        }
    }

    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }
}
