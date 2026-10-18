package io.github.hquan07.banking.lake.writer;

import io.github.hquan07.banking.lake.model.KafkaEnvelope;

import java.util.List;

@FunctionalInterface
public interface LakeBatchSink {
    void write(List<KafkaEnvelope> records);
}
