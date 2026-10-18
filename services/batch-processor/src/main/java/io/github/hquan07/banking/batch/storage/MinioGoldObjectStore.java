package io.github.hquan07.banking.batch.storage;

import io.github.hquan07.banking.batch.BatchProcessorProperties;
import io.github.hquan07.banking.batch.model.PublishedObject;
import io.minio.BucketExistsArgs;
import io.minio.Http;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Component
public class MinioGoldObjectStore implements GoldObjectStore {

    private static final String GOLD_SCHEMA_PATH = "gold/daily_transactions/report_date=";

    private final MinioClient client;
    private final String bucket;

    public MinioGoldObjectStore(MinioClient client, BatchProcessorProperties properties) {
        this.client = client;
        this.bucket = properties.minioBucket();
    }

    @Override
    public void verifyBucket() {
        try {
            if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
                throw new IllegalStateException("Required MinIO bucket does not exist: " + bucket);
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to verify MinIO bucket " + bucket, exception);
        }
    }

    @Override
    public PublishedObject publishDaily(
            LocalDate businessDate,
            byte[] content,
            long rowCount,
            String schemaVersion,
            String sourceFingerprint
    ) {
        String key = GOLD_SCHEMA_PATH + businessDate + "/daily-transactions-v1.parquet";
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("sha256", ContentHashes.sha256(content));
        metadata.put("schema-version", schemaVersion);
        metadata.put("row-count", Long.toString(rowCount));
        metadata.put("source-fingerprint", sourceFingerprint);
        StatObjectResponse existing = statIfPresent(key);
        if (existing == null || !matches(existing, metadata.get("sha256"), content.length)) {
            put(key, content, "application/vnd.apache.parquet", metadata);
        }
        StatObjectResponse uploaded = requiredStat(key);
        if (!matches(uploaded, metadata.get("sha256"), content.length)) {
            throw new IllegalStateException("MinIO checksum verification failed after Gold publication: " + key);
        }
        return new PublishedObject(key, metadata.get("sha256"), content.length, rowCount, schemaVersion);
    }

    @Override
    public void verify(PublishedObject object) {
        StatObjectResponse stat = requiredStat(object.key());
        if (!matches(stat, object.sha256(), object.size())) {
            throw new IllegalStateException("Published Gold object no longer matches its audit record: " + object.key());
        }
    }

    @Override
    public String publishManifest(LocalDate businessDate, byte[] content) {
        String key = GOLD_SCHEMA_PATH + businessDate + "/_SUCCESS.json";
        String sha256 = ContentHashes.sha256(content);
        StatObjectResponse existing = statIfPresent(key);
        if (existing == null || !matches(existing, sha256, content.length)) {
            put(key, content, "application/json", Map.of("sha256", sha256, "schema-version", "batch-report-v1"));
        }
        if (!matches(requiredStat(key), sha256, content.length)) {
            throw new IllegalStateException("MinIO report manifest verification failed: " + key);
        }
        return key;
    }

    private void put(String key, byte[] content, String contentType, Map<String, String> metadata) {
        try {
            client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(new ByteArrayInputStream(content), (long) content.length, -1L)
                    .contentType(contentType)
                    .userMetadata(metadata)
                    .build());
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to publish MinIO object " + key, exception);
        }
    }

    private StatObjectResponse requiredStat(String key) {
        StatObjectResponse result = statIfPresent(key);
        if (result == null) {
            throw new IllegalStateException("Required MinIO object does not exist: " + key);
        }
        return result;
    }

    private StatObjectResponse statIfPresent(String key) {
        try {
            return client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
        } catch (ErrorResponseException exception) {
            String code = exception.errorResponse().code();
            if ("NoSuchKey".equals(code) || "NoSuchObject".equals(code) || "NotFound".equals(code)) {
                return null;
            }
            throw new IllegalStateException("Unable to stat MinIO object " + key, exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to stat MinIO object " + key, exception);
        }
    }

    private boolean matches(StatObjectResponse stat, String expectedHash, long expectedSize) {
        return expectedSize == stat.size() && expectedHash.equals(metadataValue(stat.userMetadata(), "sha256"));
    }

    private String metadataValue(Http.Headers metadata, String name) {
        return metadata.entrySet().stream()
                .filter(entry -> entry.getKey().toLowerCase(Locale.ROOT).equals(name)
                        || entry.getKey().toLowerCase(Locale.ROOT).endsWith("-" + name))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}
