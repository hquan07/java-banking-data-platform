package io.github.hquan07.banking.lake.storage;

import io.github.hquan07.banking.lake.LakeWriterProperties;
import io.github.hquan07.banking.lake.model.LakeObjectDescriptor;
import io.github.hquan07.banking.lake.util.Hashes;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.Http;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class MinioLakeObjectStore implements LakeObjectStore {

    private final MinioClient client;
    private final String bucket;

    public MinioLakeObjectStore(MinioClient client, LakeWriterProperties properties) {
        this.client = client;
        this.bucket = properties.bucket();
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
    public LakeObjectDescriptor putIfAbsent(
            String key,
            byte[] content,
            String contentType,
            String contentEncoding,
            long rowCount,
            String schemaVersion
    ) {
        String sha256 = Hashes.sha256(content);
        StatObjectResponse existing = statIfPresent(key);
        if (existing != null) {
            String storedHash = metadataValue(existing.userMetadata(), "sha256");
            if (!sha256.equals(storedHash) || existing.size() != content.length) {
                throw new IllegalStateException("Committed object differs from deterministic batch output: " + key);
            }
            return new LakeObjectDescriptor(
                    key, existing.contentType(), contentEncoding, existing.size(), sha256, rowCount, schemaVersion
            );
        }

        try {
            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(new ByteArrayInputStream(content), (long) content.length, -1L)
                    .contentType(contentType)
                    .userMetadata(Map.of(
                            "sha256", sha256,
                            "schema-version", schemaVersion,
                            "row-count", Long.toString(rowCount)
                    ));
            if (contentEncoding != null && !contentEncoding.isBlank()) {
                builder.headers(Map.of("Content-Encoding", contentEncoding));
            }
            client.putObject(builder.build());
            StatObjectResponse uploaded = client.statObject(
                    StatObjectArgs.builder().bucket(bucket).object(key).build()
            );
            String uploadedHash = metadataValue(uploaded.userMetadata(), "sha256");
            if (!sha256.equals(uploadedHash) || uploaded.size() != content.length) {
                throw new IllegalStateException("MinIO checksum verification failed after upload: " + key);
            }
            return new LakeObjectDescriptor(
                    key, contentType, contentEncoding, content.length, sha256, rowCount, schemaVersion
            );
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to upload MinIO object " + key, exception);
        }
    }

    @Override
    public byte[] read(String key) {
        try (var stream = client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build())) {
            return stream.readAllBytes();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to read MinIO object " + key, exception);
        }
    }

    @Override
    public List<String> list(String prefix) {
        try {
            List<String> keys = new ArrayList<>();
            for (var result : client.listObjects(ListObjectsArgs.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .recursive(true)
                    .build())) {
                Item item = result.get();
                keys.add(item.objectName());
            }
            return keys.stream().sorted().toList();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to list MinIO prefix " + prefix, exception);
        }
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

    private String metadataValue(Http.Headers metadata, String name) {
        return metadata.entrySet().stream()
                .filter(entry -> entry.getKey().toLowerCase(Locale.ROOT).equals(name)
                        || entry.getKey().toLowerCase(Locale.ROOT).endsWith("-" + name))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}
