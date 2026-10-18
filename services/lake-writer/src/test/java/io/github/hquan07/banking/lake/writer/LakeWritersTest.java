package io.github.hquan07.banking.lake.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;
import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.model.LakeObjectDescriptor;
import io.github.hquan07.banking.lake.storage.LakeObjectStore;
import io.github.hquan07.banking.lake.storage.ManifestCommitter;
import io.github.hquan07.banking.lake.util.Hashes;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LakeWritersTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void bronzePreservesPayloadAndKafkaCoordinatesInOneMicroBatch() throws Exception {
        InMemoryStore store = new InMemoryStore();
        BronzeBatchWriter writer = new BronzeBatchWriter(store, new ManifestCommitter(store));
        List<KafkaEnvelope> records = List.of(
                envelope("banking.transactions.raw", 1, 41, "ACC10001", "{\"event\":1}"),
                envelope("banking.transactions.raw", 1, 42, "ACC10001", "{\"event\":2}")
        );

        writer.write(records);
        writer.write(records);

        List<String> parts = store.list("bronze/transactions/");
        assertEquals(1, parts.size());
        String jsonl;
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(store.read(parts.getFirst())))) {
            jsonl = new String(gzip.readAllBytes());
        }
        List<JsonNode> lines = jsonl.lines().map(line -> {
            try {
                return objectMapper.readTree(line);
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        }).toList();
        assertEquals(2, lines.size());
        assertEquals(41, lines.getFirst().get("offset").asLong());
        assertEquals("banking.transactions.raw", lines.getFirst().get("topic").asText());
        assertEquals("{\"event\":1}", lines.getFirst().get("payload_utf8").asText());
        assertEquals(1, store.list("_manifests/bronze/").size());
    }

    @Test
    void curatedWritesTypedPartitionedParquetAndDeduplicatesEventIds() throws Exception {
        InMemoryStore store = new InMemoryStore();
        CuratedBatchWriter writer = new CuratedBatchWriter(
                store,
                new ManifestCommitter(store),
                new ParquetBatchCodec()
        );
        TransactionCreatedEvent first = event(
                "85d8145e-912b-4127-9580-d369bbe67c0e", "100.00", TransactionStatus.SUCCESS
        );
        TransactionCreatedEvent second = event(
                "9bb12cb6-071a-4f69-8b76-d49635b49535", "25.50", TransactionStatus.FAILED
        );
        List<KafkaEnvelope> records = List.of(
                validatedEnvelope(0, 100, first),
                validatedEnvelope(0, 101, first),
                validatedEnvelope(0, 102, second)
        );

        writer.write(records);
        writer.write(records);

        String silverKey = store.list("silver/transactions/event_date=2026-10-17/").getFirst();
        String goldKey = store.list("gold/daily_transactions/report_date=2026-10-17/").getFirst();
        List<GenericRecord> silverRows = readParquet(store.read(silverKey));
        List<GenericRecord> goldRows = readParquet(store.read(goldKey));

        assertEquals(2, silverRows.size());
        assertEquals("ACC10001", silverRows.getFirst().get("account_id").toString());
        assertEquals(2L, goldRows.getFirst().get("transaction_count"));
        assertEquals(1L, goldRows.getFirst().get("success_count"));
        assertEquals(1L, goldRows.getFirst().get("failed_count"));
        assertEquals(true, goldRows.getFirst().get("is_partial"));
        assertEquals(1, store.list("_manifests/curated/").size());
    }

    @Test
    void objectStoreRejectsDifferentContentForCommittedKey() {
        InMemoryStore store = new InMemoryStore();
        store.putIfAbsent("fixed", new byte[]{1}, "test/plain", null, 1, "v1");

        assertThrows(IllegalStateException.class, () ->
                store.putIfAbsent("fixed", new byte[]{2}, "test/plain", null, 1, "v1"));
    }

    private KafkaEnvelope validatedEnvelope(int partition, long offset, TransactionCreatedEvent event)
            throws Exception {
        return new KafkaEnvelope(
                "banking.transactions.validated",
                partition,
                offset,
                Instant.parse("2026-10-17T02:31:00Z").toEpochMilli(),
                event.accountId(),
                objectMapper.writeValueAsBytes(event)
        );
    }

    private KafkaEnvelope envelope(String topic, int partition, long offset, String key, String payload) {
        return new KafkaEnvelope(
                topic,
                partition,
                offset,
                Instant.parse("2026-10-17T02:30:00Z").plusSeconds(offset).toEpochMilli(),
                key,
                payload.getBytes()
        );
    }

    private TransactionCreatedEvent event(String eventId, String amount, TransactionStatus status) {
        return new TransactionCreatedEvent(
                UUID.fromString(eventId),
                TransactionCreatedEvent.EVENT_TYPE,
                TransactionCreatedEvent.SCHEMA_VERSION,
                UUID.randomUUID(),
                "ACC10001",
                TransactionType.PAYMENT,
                new BigDecimal(amount),
                "VND",
                status,
                Instant.parse("2026-10-17T02:30:00Z"),
                Instant.parse("2026-10-17T02:30:01Z"),
                "phase6-test"
        );
    }

    private List<GenericRecord> readParquet(byte[] content) throws Exception {
        Path file = Files.createTempFile("lake-test-", ".parquet");
        try {
            Files.write(file, content);
            List<GenericRecord> rows = new ArrayList<>();
            HadoopInputFile inputFile = HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(file.toUri()), new Configuration(false)
            );
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                GenericRecord row;
                while ((row = reader.read()) != null) {
                    rows.add(row);
                }
            }
            return rows;
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static final class InMemoryStore implements LakeObjectStore {
        private final Map<String, byte[]> objects = new HashMap<>();
        private final Map<String, LakeObjectDescriptor> descriptors = new HashMap<>();

        @Override
        public void verifyBucket() {
        }

        @Override
        public LakeObjectDescriptor putIfAbsent(
                String key,
                byte[] content,
                String contentType,
                String contentEncoding,
                long rowCount,
                String schemaVersion
        ) {
            String hash = Hashes.sha256(content);
            LakeObjectDescriptor existing = descriptors.get(key);
            if (existing != null) {
                if (!existing.sha256().equals(hash)) {
                    throw new IllegalStateException("content mismatch");
                }
                return existing;
            }
            LakeObjectDescriptor descriptor = new LakeObjectDescriptor(
                    key, contentType, contentEncoding, content.length, hash, rowCount, schemaVersion
            );
            objects.put(key, content.clone());
            descriptors.put(key, descriptor);
            return descriptor;
        }

        @Override
        public byte[] read(String key) {
            return objects.get(key).clone();
        }

        @Override
        public List<String> list(String prefix) {
            return objects.keySet().stream().filter(key -> key.startsWith(prefix)).sorted().toList();
        }
    }
}
