package io.github.hquan07.banking.transaction;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MigrationResourceTest {

    @Test
    void migrationsAreForwardOnlyAndContainReliabilityConstraints() throws IOException {
        String v1 = resource("/db/migration/V1__create_core_banking_tables.sql");
        String v2 = resource("/db/migration/V2__create_fraud_and_reconciliation_tables.sql");

        assertTrue(v1.contains("uq_transactions_idempotency_key"));
        assertTrue(v1.contains("idx_outbox_ready_to_publish"));
        assertTrue(v1.contains("chk_outbox_claim"));
        assertTrue(v2.contains("uq_fraud_alert_business_key"));
        assertTrue(v2.contains("processing_runs"));
        assertFalse((v1 + v2).toUpperCase().contains("DROP TABLE"));
        assertFalse((v1 + v2).toUpperCase().contains("TRUNCATE"));
    }

    private String resource(String path) throws IOException {
        try (InputStream stream = getClass().getResourceAsStream(path)) {
            if (stream == null) {
                throw new IllegalStateException("missing migration: " + path);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
