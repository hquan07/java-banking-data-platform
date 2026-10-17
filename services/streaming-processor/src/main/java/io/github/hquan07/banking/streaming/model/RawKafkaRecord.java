package io.github.hquan07.banking.streaming.model;

import java.io.Serializable;

public record RawKafkaRecord(
        String topic,
        int partition,
        long offset,
        long timestamp,
        String key,
        byte[] payload
) implements Serializable {
}
