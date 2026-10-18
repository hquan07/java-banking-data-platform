package io.github.hquan07.banking.warehouse.model;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public record WarehouseRecord(
        String topic,
        int partition,
        long offset,
        long timestamp,
        byte[] payload
) {
    public static WarehouseRecord from(ConsumerRecord<String, byte[]> record) {
        return new WarehouseRecord(
                record.topic(), record.partition(), record.offset(), record.timestamp(), record.value()
        );
    }
}
