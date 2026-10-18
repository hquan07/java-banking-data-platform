package io.github.hquan07.banking.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.batch.model.BatchOutcome;
import io.github.hquan07.banking.batch.model.BatchRun;
import io.github.hquan07.banking.batch.model.PublishedObject;
import io.github.hquan07.banking.batch.model.SourceSnapshot;
import io.github.hquan07.banking.batch.persistence.BatchRepository;
import io.github.hquan07.banking.batch.quality.DailyDataQuality;
import io.github.hquan07.banking.batch.storage.GoldObjectStore;
import io.github.hquan07.banking.batch.storage.GoldParquetCodec;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class DailyBatchService {

    private final BatchRepository repository;
    private final DailyDataQuality dataQuality;
    private final GoldParquetCodec parquet;
    private final GoldObjectStore objectStore;
    private final BatchProcessorProperties properties;
    private final ObjectMapper objectMapper;

    public DailyBatchService(
            BatchRepository repository,
            DailyDataQuality dataQuality,
            GoldParquetCodec parquet,
            GoldObjectStore objectStore,
            BatchProcessorProperties properties,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.dataQuality = dataQuality;
        this.parquet = parquet;
        this.objectStore = objectStore;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> checkSource(LocalDate businessDate) {
        SourceSnapshot source = repository.sourceSnapshot(businessDate);
        Map<String, Object> checks = dataQuality.validateSource(source);
        return Map.of(
                "businessDate", businessDate,
                "businessTimezone", properties.businessTimezone(),
                "sourceRowCount", source.sourceRowCount(),
                "sourceFingerprint", source.fingerprint(),
                "checks", checks
        );
    }

    public BatchOutcome runDaily(LocalDate businessDate) {
        UUID runId = repository.createRun(businessDate);
        try {
            return repository.inTransaction(() -> {
                repository.acquireDailyLock(businessDate);
                SourceSnapshot source = repository.sourceSnapshot(businessDate);
                Map<String, Object> checks = dataQuality.validateSource(source);
                objectStore.verifyBucket();
                byte[] content = parquet.write(source.rows(), source.fingerprint());
                PublishedObject object = objectStore.publishDaily(
                        businessDate,
                        content,
                        source.rows().size(),
                        GoldParquetCodec.SCHEMA_VERSION,
                        source.fingerprint()
                );
                repository.replaceDailyAggregates(runId, source);
                repository.markSucceeded(runId, source, object, checks);
                return outcome(runId, source, object);
            });
        } catch (Exception exception) {
            repository.markFailed(runId, failureCode(exception), rootMessage(exception));
            throw exception;
        }
    }

    public Map<String, Object> validateDaily(LocalDate businessDate) {
        SourceSnapshot source = repository.sourceSnapshot(businessDate);
        Map<String, Object> checks = dataQuality.validateTarget(source, repository.dailyTargets(businessDate));
        BatchRun run = repository.latestSuccessful(businessDate);
        if (!source.fingerprint().equals(run.sourceFingerprint())) {
            throw new IllegalStateException("Warehouse facts changed after the latest successful batch for " + businessDate);
        }
        objectStore.verify(run.goldObject());
        return Map.of(
                "batchRunId", run.batchRunId(),
                "businessDate", businessDate,
                "sourceFingerprint", source.fingerprint(),
                "goldObjectKey", run.goldObject().key(),
                "goldObjectSha256", run.goldObject().sha256(),
                "checks", checks
        );
    }

    public Map<String, Object> publishReportMetadata(LocalDate businessDate) {
        validateDaily(businessDate);
        BatchRun run = repository.latestSuccessful(businessDate);
        try {
            Map<String, Object> manifest = new LinkedHashMap<>();
            manifest.put("schemaVersion", "batch-report-v1");
            manifest.put("status", "PUBLISHED");
            manifest.put("jobName", run.jobName());
            manifest.put("batchRunId", run.batchRunId());
            manifest.put("businessDate", run.businessDate());
            manifest.put("businessTimezone", run.businessTimezone());
            manifest.put("sourceRowCount", run.sourceRowCount());
            manifest.put("outputRowCount", run.outputRowCount());
            manifest.put("sourceFingerprint", run.sourceFingerprint());
            manifest.put("goldObject", run.goldObject());
            manifest.put("startedAt", run.startedAt());
            manifest.put("finishedAt", run.finishedAt());
            byte[] content = (objectMapper.writeValueAsString(manifest) + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            String manifestKey = objectStore.publishManifest(businessDate, content);
            repository.markPublished(run.batchRunId());
            return Map.of(
                    "batchRunId", run.batchRunId(),
                    "businessDate", businessDate,
                    "manifestKey", manifestKey,
                    "goldObjectKey", run.goldObject().key()
            );
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to publish report metadata for " + businessDate, exception);
        }
    }

    private BatchOutcome outcome(UUID runId, SourceSnapshot source, PublishedObject object) {
        return new BatchOutcome(
                runId,
                source.businessDate(),
                source.sourceRowCount(),
                source.rows().size(),
                source.fingerprint(),
                object.key(),
                object.sha256()
        );
    }

    private String failureCode(Exception exception) {
        String message = rootMessage(exception);
        if (message.contains("already running")) {
            return "CONCURRENT_RUN";
        }
        if (message.contains("data-quality")) {
            return "DQ_FAILED";
        }
        return "BATCH_FAILED";
    }

    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
