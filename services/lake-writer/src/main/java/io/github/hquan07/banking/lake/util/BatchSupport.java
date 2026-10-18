package io.github.hquan07.banking.lake.util;

import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.model.OffsetRange;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class BatchSupport {

    public static final Comparator<KafkaEnvelope> KAFKA_ORDER = Comparator
            .comparing(KafkaEnvelope::topic)
            .thenComparingInt(KafkaEnvelope::partition)
            .thenComparingLong(KafkaEnvelope::offset);

    private BatchSupport() {
    }

    public static String batchId(String layer, List<KafkaEnvelope> records) {
        String canonical = records.stream()
                .sorted(KAFKA_ORDER)
                .map(record -> record.topic() + ":" + record.partition() + ":" + record.offset()
                        + ":" + Hashes.sha256(record.payload()))
                .collect(Collectors.joining("\n", layer + "\n", ""));
        return Hashes.sha256(canonical.getBytes(StandardCharsets.UTF_8)).substring(0, 32);
    }

    public static List<OffsetRange> offsetRanges(List<KafkaEnvelope> records) {
        record TopicPartition(String topic, int partition) {
        }
        Map<TopicPartition, List<KafkaEnvelope>> grouped = records.stream()
                .collect(Collectors.groupingBy(
                        record -> new TopicPartition(record.topic(), record.partition())
                ));
        return grouped.entrySet().stream()
                .map(entry -> {
                    var offsets = entry.getValue().stream().mapToLong(KafkaEnvelope::offset).sorted().toArray();
                    return new OffsetRange(
                            entry.getKey().topic(), entry.getKey().partition(), offsets[0],
                            offsets[offsets.length - 1], offsets.length
                    );
                })
                .sorted(Comparator.comparing(OffsetRange::topic).thenComparingInt(OffsetRange::partition))
                .toList();
    }

    public static Instant deterministicCommitTime(List<KafkaEnvelope> records) {
        return Instant.ofEpochMilli(records.stream().mapToLong(KafkaEnvelope::timestamp).max().orElseThrow());
    }
}
