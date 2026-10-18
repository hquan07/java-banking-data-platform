package io.github.hquan07.banking.lake.writer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.lake.model.DailyAggregatePartial;
import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.model.LakeObjectDescriptor;
import io.github.hquan07.banking.lake.model.ValidatedLakeEvent;
import io.github.hquan07.banking.lake.storage.LakeObjectStore;
import io.github.hquan07.banking.lake.storage.ManifestCommitter;
import io.github.hquan07.banking.lake.util.BatchSupport;
import io.github.hquan07.banking.lake.util.Hashes;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class CuratedBatchWriter implements LakeBatchSink {

    private final LakeObjectStore objectStore;
    private final ManifestCommitter manifestCommitter;
    private final ParquetBatchCodec parquet;
    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public CuratedBatchWriter(
            LakeObjectStore objectStore,
            ManifestCommitter manifestCommitter,
            ParquetBatchCodec parquet
    ) {
        this.objectStore = objectStore;
        this.manifestCommitter = manifestCommitter;
        this.parquet = parquet;
    }

    @Override
    public void write(List<KafkaEnvelope> input) {
        if (input.isEmpty()) {
            return;
        }
        List<KafkaEnvelope> records = input.stream().sorted(BatchSupport.KAFKA_ORDER).toList();
        String batchId = BatchSupport.batchId("curated", records);
        List<ParquetBatchCodec.CuratedRecord> deduplicated = parseAndDeduplicate(records);
        Map<LocalDate, List<ParquetBatchCodec.CuratedRecord>> byDate = deduplicated.stream()
                .collect(Collectors.groupingBy(
                        row -> row.validated().event().occurredAt().atZone(ZoneOffset.UTC).toLocalDate(),
                        TreeMap::new,
                        Collectors.toList()
                ));

        List<LakeObjectDescriptor> objects = new ArrayList<>();
        Instant generatedAt = BatchSupport.deterministicCommitTime(records);
        byDate.forEach((date, dateRows) -> {
            byte[] silver = parquet.writeSilver(dateRows);
            String silverKey = "silver/transactions/event_date=" + date + "/part-" + batchId + ".parquet";
            objects.add(objectStore.putIfAbsent(
                    silverKey, silver, "application/vnd.apache.parquet", null, dateRows.size(),
                    ParquetBatchCodec.SILVER_SCHEMA_VERSION
            ));

            List<DailyAggregatePartial> goldRows = aggregate(date, batchId, dateRows);
            byte[] gold = parquet.writeGold(goldRows, generatedAt);
            String goldKey = "gold/daily_transactions/report_date=" + date + "/part-" + batchId + ".parquet";
            objects.add(objectStore.putIfAbsent(
                    goldKey, gold, "application/vnd.apache.parquet", null, goldRows.size(),
                    ParquetBatchCodec.GOLD_SCHEMA_VERSION
            ));
        });
        manifestCommitter.commit("curated", batchId, records, objects);
    }

    private List<ParquetBatchCodec.CuratedRecord> parseAndDeduplicate(List<KafkaEnvelope> records) {
        Map<UUID, ParquetBatchCodec.CuratedRecord> unique = new LinkedHashMap<>();
        for (KafkaEnvelope record : records) {
            try {
                TransactionCreatedEvent event = objectMapper.readValue(record.payload(), TransactionCreatedEvent.class);
                ValidatedLakeEvent validated = new ValidatedLakeEvent(
                        event,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.timestamp(),
                        Hashes.sha256(record.payload())
                );
                unique.putIfAbsent(
                        event.eventId(),
                        new ParquetBatchCodec.CuratedRecord(record, validated)
                );
            } catch (Exception exception) {
                throw new IllegalStateException(
                        "Validated topic contains an unreadable record at " + record.topic() + "-"
                                + record.partition() + "@" + record.offset(),
                        exception
                );
            }
        }
        return List.copyOf(unique.values());
    }

    private List<DailyAggregatePartial> aggregate(
            LocalDate date,
            String batchId,
            List<ParquetBatchCodec.CuratedRecord> records
    ) {
        record Key(String accountId, String currency) {
        }
        Map<Key, List<TransactionCreatedEvent>> grouped = records.stream()
                .map(row -> row.validated().event())
                .collect(Collectors.groupingBy(
                        event -> new Key(event.accountId(), event.currency()),
                        () -> new TreeMap<>(Comparator.comparing(Key::accountId).thenComparing(Key::currency)),
                        Collectors.toList()
                ));
        return grouped.entrySet().stream().map(entry -> {
            EnumMap<TransactionStatus, Long> counts = new EnumMap<>(TransactionStatus.class);
            for (TransactionStatus status : TransactionStatus.values()) {
                counts.put(status, 0L);
            }
            BigDecimal amount = BigDecimal.ZERO.setScale(2);
            for (TransactionCreatedEvent event : entry.getValue()) {
                counts.compute(event.status(), (ignored, count) -> count + 1);
                amount = amount.add(event.amount());
            }
            return new DailyAggregatePartial(
                    date,
                    entry.getKey().accountId(),
                    entry.getKey().currency(),
                    entry.getValue().size(),
                    amount,
                    counts.get(TransactionStatus.SUCCESS),
                    counts.get(TransactionStatus.FAILED),
                    counts.get(TransactionStatus.PENDING),
                    counts.get(TransactionStatus.REVERSED),
                    batchId
            );
        }).toList();
    }
}
