package io.github.hquan07.banking.lake;

import io.github.hquan07.banking.lake.storage.LakeObjectStore;
import io.github.hquan07.banking.lake.storage.ParquetInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class LakeVerificationRunner implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(LakeVerificationRunner.class);

    private final String verifyKey;
    private final LakeObjectStore objectStore;
    private final ParquetInspector parquetInspector;
    private final ConfigurableApplicationContext context;

    public LakeVerificationRunner(
            @Value("${LAKE_VERIFY_KEY:}") String verifyKey,
            LakeObjectStore objectStore,
            ParquetInspector parquetInspector,
            ConfigurableApplicationContext context
    ) {
        this.verifyKey = verifyKey;
        this.objectStore = objectStore;
        this.parquetInspector = parquetInspector;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (verifyKey == null || verifyKey.isBlank()) {
            return;
        }
        objectStore.verifyBucket();
        byte[] content = objectStore.read(verifyKey);
        if (!verifyKey.endsWith(".parquet")) {
            throw new IllegalArgumentException("LAKE_VERIFY_KEY must reference a Parquet object");
        }
        long rows = parquetInspector.rowCount(content);
        if (rows <= 0) {
            throw new IllegalStateException("Parquet object contains no rows: " + verifyKey);
        }
        LOGGER.info("LAKE_VERIFY_OK key={} rows={} bytes={}", verifyKey, rows, content.length);
        context.close();
    }
}
