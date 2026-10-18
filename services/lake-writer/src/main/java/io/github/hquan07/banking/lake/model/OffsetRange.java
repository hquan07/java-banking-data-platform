package io.github.hquan07.banking.lake.model;

public record OffsetRange(String topic, int partition, long firstOffset, long lastOffset, long recordCount) {
}
