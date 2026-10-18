package io.github.hquan07.banking.lake.model;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;

public record KafkaEnvelope(
        String topic,
        int partition,
        long offset,
        long timestamp,
        String key,
        byte[] payload
) {
    public KafkaEnvelope {
        payload = Arrays.copyOf(payload, payload.length);
    }

    public static KafkaEnvelope from(ConsumerRecord<String, byte[]> record) {
        return new KafkaEnvelope(
                record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value()
        );
    }

    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }
}
