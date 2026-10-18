package io.github.hquan07.banking.lake.writer;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.model.LakeObjectDescriptor;
import io.github.hquan07.banking.lake.storage.LakeObjectStore;
import io.github.hquan07.banking.lake.storage.ManifestCommitter;
import io.github.hquan07.banking.lake.util.BatchSupport;
import io.github.hquan07.banking.lake.util.Hashes;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

@Component
public class BronzeBatchWriter implements LakeBatchSink {

    private static final String SCHEMA_VERSION = "bronze-transaction-v1";

    private final LakeObjectStore objectStore;
    private final ManifestCommitter manifestCommitter;
    private final ObjectMapper objectMapper = JsonMapper.builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();

    public BronzeBatchWriter(LakeObjectStore objectStore, ManifestCommitter manifestCommitter) {
        this.objectStore = objectStore;
        this.manifestCommitter = manifestCommitter;
    }

    @Override
    public void write(List<KafkaEnvelope> input) {
        if (input.isEmpty()) {
            return;
        }
        List<KafkaEnvelope> records = input.stream().sorted(BatchSupport.KAFKA_ORDER).toList();
        String batchId = BatchSupport.batchId("bronze", records);
        Map<String, List<KafkaEnvelope>> byDate = records.stream().collect(Collectors.groupingBy(
                record -> Instant.ofEpochMilli(record.timestamp()).atZone(ZoneOffset.UTC).toLocalDate().toString(),
                TreeMap::new,
                Collectors.toList()
        ));

        List<LakeObjectDescriptor> objects = new ArrayList<>();
        byDate.forEach((date, dateRecords) -> {
            byte[] content = jsonLinesGzip(dateRecords);
            String key = "bronze/transactions/ingest_date=" + date + "/part-" + batchId + ".jsonl.gz";
            objects.add(objectStore.putIfAbsent(
                    key, content, "application/x-ndjson", "gzip", dateRecords.size(), SCHEMA_VERSION
            ));
        });
        manifestCommitter.commit("bronze", batchId, records, objects);
    }

    private byte[] jsonLinesGzip(List<KafkaEnvelope> records) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
                for (KafkaEnvelope record : records) {
                    Map<String, Object> line = new LinkedHashMap<>();
                    line.put("ingested_at", Instant.ofEpochMilli(record.timestamp()).toString());
                    line.put("kafka_key", record.key());
                    line.put("offset", record.offset());
                    line.put("partition", record.partition());
                    line.put("payload_base64", Base64.getEncoder().encodeToString(record.payload()));
                    line.put("payload_sha256", Hashes.sha256(record.payload()));
                    line.put("payload_utf8", new String(record.payload(), StandardCharsets.UTF_8));
                    line.put("schema_version", SCHEMA_VERSION);
                    line.put("topic", record.topic());
                    gzip.write(objectMapper.writeValueAsBytes(line));
                    gzip.write('\n');
                }
            }
            return output.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to encode Bronze JSONL batch", exception);
        }
    }
}
