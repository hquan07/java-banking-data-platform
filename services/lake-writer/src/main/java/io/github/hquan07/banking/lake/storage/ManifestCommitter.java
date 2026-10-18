package io.github.hquan07.banking.lake.storage;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.lake.model.CommitManifest;
import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.model.LakeObjectDescriptor;
import io.github.hquan07.banking.lake.util.BatchSupport;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

@Component
public class ManifestCommitter {

    private final LakeObjectStore objectStore;
    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();

    public ManifestCommitter(LakeObjectStore objectStore) {
        this.objectStore = objectStore;
    }

    public String commit(
            String layer,
            String batchId,
            List<KafkaEnvelope> records,
            List<LakeObjectDescriptor> objects
    ) {
        try {
            CommitManifest manifest = new CommitManifest(
                    batchId,
                    layer,
                    "COMMITTED",
                    BatchSupport.deterministicCommitTime(records).toString(),
                    BatchSupport.offsetRanges(records),
                    objects.stream().sorted(Comparator.comparing(LakeObjectDescriptor::key)).toList()
            );
            byte[] content = objectMapper.writeValueAsBytes(manifest);
            String key = "_manifests/" + layer + "/batch_id=" + batchId + ".json";
            objectStore.putIfAbsent(key, content, "application/json", null, objects.size(), "manifest-v1");
            return key;
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to write commit manifest for batch " + batchId, exception);
        }
    }
}
