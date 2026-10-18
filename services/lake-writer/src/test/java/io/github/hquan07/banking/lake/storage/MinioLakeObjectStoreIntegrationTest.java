package io.github.hquan07.banking.lake.storage;

import io.github.hquan07.banking.lake.LakeWriterProperties;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class MinioLakeObjectStoreIntegrationTest {

    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "phase6-test-secret";

    private static final ImageFromDockerfile MINIO_IMAGE = new ImageFromDockerfile(
            "banking-minio-phase6-test:2025-10-15", false
    ).withFileFromPath("Dockerfile", minioDockerfile());

    @Container
    static final GenericContainer<?> MINIO = new GenericContainer<>(MINIO_IMAGE)
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server", "/data")
            .withExposedPorts(9000);

    private static Path minioDockerfile() {
        Path root = Path.of(System.getProperty("user.dir"));
        Path direct = root.resolve("infrastructure/minio/Dockerfile");
        if (Files.exists(direct)) {
            return direct;
        }
        return root.resolve("../../infrastructure/minio/Dockerfile").normalize();
    }

    @Test
    void uploadsListsReadsAndSafelyRetriesCommittedObject() throws Exception {
        String endpoint = "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000);
        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(ACCESS_KEY, SECRET_KEY)
                .build();
        client.makeBucket(MakeBucketArgs.builder().bucket("banking-lake").build());
        LakeWriterProperties properties = new LakeWriterProperties(
                true,
                endpoint,
                ACCESS_KEY,
                SECRET_KEY,
                "banking-lake",
                "unused:9092",
                "raw",
                "validated",
                "raw-group",
                "curated-group",
                10,
                Duration.ofSeconds(1),
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                "earliest"
        );
        MinioLakeObjectStore store = new MinioLakeObjectStore(client, properties);
        byte[] payload = "phase6".getBytes();

        store.verifyBucket();
        store.putIfAbsent("bronze/test.txt", payload, "text/plain", null, 1, "v1");
        store.putIfAbsent("bronze/test.txt", payload, "text/plain", null, 1, "v1");

        assertEquals(1, store.list("bronze/").size());
        assertArrayEquals(payload, store.read("bronze/test.txt"));
    }
}
