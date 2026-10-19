package io.github.hquan07.banking.analytics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@AutoConfigureMockMvc
class AnalyticsApiIntegrationTest {

    private static final LocalDate DATE = LocalDate.parse("2026-10-18");
    private static final UUID BATCH_RUN_ID = UUID.fromString("91000000-0000-4000-8000-000000000001");
    private static final UUID FIRST_TRANSACTION_ID = UUID.fromString("92000000-0000-4000-8000-000000000001");

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withDatabaseName("banking_warehouse")
            .withUsername("banking_warehouse")
            .withPassword("warehouse-test-password");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.read-only", () -> false);
        registry.add("spring.flyway.enabled", () -> true);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void seed() {
        jdbc.update("DELETE FROM fact_fraud_alerts");
        jdbc.update("DELETE FROM fact_transactions");
        jdbc.update("DELETE FROM agg_daily_transactions");
        jdbc.update("DELETE FROM batch_runs");
        jdbc.update("DELETE FROM dim_account");
        jdbc.update("DELETE FROM dim_date");

        insertDate(DATE);
        long customerKey = jdbc.queryForObject(
                "SELECT customer_key FROM dim_customer WHERE customer_id = 'UNKNOWN'", Long.class
        );
        long vndAccount = insertAccount("ACC_ANALYTICS_VND", customerKey, "VND");
        long usdAccount = insertAccount("ACC_ANALYTICS_USD", customerKey, "USD");
        insertTransaction(FIRST_TRANSACTION_ID, vndAccount, "VND", "10.00", "SUCCESS", "04:00:00", 1);
        insertTransaction(UUID.fromString("92000000-0000-4000-8000-000000000002"),
                vndAccount, "VND", "20.00", "FAILED", "04:01:00", 2);
        insertTransaction(UUID.fromString("92000000-0000-4000-8000-000000000003"),
                usdAccount, "USD", "5.00", "PENDING", "04:02:00", 3);
        insertFraud(vndAccount, FIRST_TRANSACTION_ID,
                "93000000-0000-4000-8000-000000000001", "HIGH_VALUE", "HIGH", "04:10:00");
        insertFraud(vndAccount, FIRST_TRANSACTION_ID,
                "93000000-0000-4000-8000-000000000002", "VELOCITY", "MEDIUM", "04:20:00");
        insertDaily();
    }

    @Test
    void returnsFinalizedDailyMetricsByCurrency() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/daily")
                        .param("date", DATE.toString())
                        .header("X-Correlation-ID", "phase9-daily"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Correlation-ID", "phase9-daily"))
                .andExpect(header().string("Cache-Control", "max-age=30, private"))
                .andExpect(jsonPath("$.date").value("2026-10-18"))
                .andExpect(jsonPath("$.businessTimezone").value("Asia/Ho_Chi_Minh"))
                .andExpect(jsonPath("$.batchRunId").value(BATCH_RUN_ID.toString()))
                .andExpect(jsonPath("$.transactionCount").value(3))
                .andExpect(jsonPath("$.totalAmount").value("35.00"))
                .andExpect(jsonPath("$.failedCount").value(1))
                .andExpect(jsonPath("$.currencies.length()").value(2))
                .andExpect(jsonPath("$.currencies[0].currency").value("USD"))
                .andExpect(jsonPath("$.currencies[1].currency").value("VND"));
    }

    @Test
    void doesNotExposeDailyRowsFromAnUnsuccessfulBatch() throws Exception {
        jdbc.update("UPDATE batch_runs SET status = 'FAILED' WHERE batch_run_id = ?", BATCH_RUN_ID);

        mockMvc.perform(get("/api/v1/analytics/daily").param("date", DATE.toString()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("ANALYTICS_NOT_FOUND"));
    }

    @Test
    void pagesFraudAlertsUsingHalfOpenTimeRange() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/fraud")
                        .param("from", "2026-10-18T04:00:00Z")
                        .param("to", "2026-10-18T04:20:00Z")
                        .param("page", "0")
                        .param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.totalPages").value(1))
                .andExpect(jsonPath("$.items[0].ruleCode").value("HIGH_VALUE"));

        mockMvc.perform(get("/api/v1/analytics/fraud")
                        .param("from", "2026-10-18T04:00:00Z")
                        .param("to", "2026-10-18T05:00:00Z")
                        .param("page", "0")
                        .param("size", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.items[0].ruleCode").value("VELOCITY"));
    }

    @Test
    void returnsAccountTransactionAndFraudSummary() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/accounts/{accountId}/summary", "ACC_ANALYTICS_VND"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.customerId").value("UNKNOWN"))
                .andExpect(jsonPath("$.currency").value("VND"))
                .andExpect(jsonPath("$.transactionCount").value(2))
                .andExpect(jsonPath("$.totalAmount").value("30.00"))
                .andExpect(jsonPath("$.successCount").value(1))
                .andExpect(jsonPath("$.failedCount").value(1))
                .andExpect(jsonPath("$.fraudAlertCount").value(2))
                .andExpect(jsonPath("$.latestFraudDetectedAt").value("2026-10-18T04:20:00Z"));
    }

    @Test
    void rejectsInvalidRangesAndPagination() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/fraud")
                        .param("from", "2026-10-19T00:00:00Z")
                        .param("to", "2026-10-18T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_ANALYTICS_RANGE"));

        mockMvc.perform(get("/api/v1/analytics/fraud")
                        .param("from", "2026-01-01T00:00:00Z")
                        .param("to", "2026-03-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Fraud analytics range cannot exceed 31 days"));

        mockMvc.perform(get("/api/v1/analytics/fraud")
                        .param("from", "2026-10-18T00:00:00Z")
                        .param("to", "2026-10-19T00:00:00Z")
                        .param("size", "201"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void reportsMissingAnalyticsWithSafeCorrelatedErrors() throws Exception {
        mockMvc.perform(get("/api/v1/analytics/daily")
                        .param("date", "2026-10-17")
                        .header("X-Correlation-ID", "phase9-missing"))
                .andExpect(status().isNotFound())
                .andExpect(header().string("X-Correlation-ID", "phase9-missing"))
                .andExpect(jsonPath("$.code").value("ANALYTICS_NOT_FOUND"))
                .andExpect(jsonPath("$.correlationId").value("phase9-missing"));

        mockMvc.perform(get("/api/v1/analytics/accounts/{accountId}/summary", "ACC_MISSING"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("ANALYTICS_NOT_FOUND"));
    }

    @Test
    void schemaContainsPhaseNineIndexesAndGetRequestsDoNotWrite() throws Exception {
        int factsBefore = count("fact_transactions");
        int auditsBefore = count("batch_runs");
        mockMvc.perform(get("/api/v1/analytics/accounts/{accountId}/summary", "ACC_ANALYTICS_VND"))
                .andExpect(status().isOk());
        assertEquals(factsBefore, count("fact_transactions"));
        assertEquals(auditsBefore, count("batch_runs"));

        String indexDefinition = jdbc.queryForObject("""
                SELECT indexdef FROM pg_indexes
                 WHERE schemaname = 'public' AND indexname = 'idx_fact_fraud_detected_alert'
                """, String.class);
        assertTrue(indexDefinition.contains("detected_at DESC, alert_id DESC"));
        assertEquals(1, jdbc.queryForObject("""
                SELECT COUNT(*) FROM pg_indexes
                 WHERE schemaname = 'public' AND indexname = 'idx_fact_fraud_account_detected'
                """, Integer.class));
    }

    private void insertDate(LocalDate date) {
        jdbc.update("""
                INSERT INTO dim_date (
                    date_key, calendar_date, calendar_year, calendar_quarter,
                    calendar_month, day_of_month, day_of_week, is_weekend
                ) VALUES (20261018, ?, 2026, 4, 10, 18, 7, TRUE)
                """, Date.valueOf(date));
    }

    private long insertAccount(String accountId, long customerKey, String currency) {
        return jdbc.queryForObject("""
                INSERT INTO dim_account (account_id, customer_key, currency, account_status)
                VALUES (?, ?, ?, 'ACTIVE') RETURNING account_key
                """, Long.class, accountId, customerKey, currency);
    }

    private void insertTransaction(
            UUID transactionId,
            long accountKey,
            String currency,
            String amount,
            String status,
            String time,
            long offset
    ) {
        Instant occurredAt = Instant.parse("2026-10-18T" + time + "Z");
        jdbc.update("""
                INSERT INTO fact_transactions (
                    transaction_id, event_id, account_key, date_key, transaction_type, amount, currency,
                    status, occurred_at, produced_at, correlation_id, schema_version,
                    source_topic, source_partition, source_offset, source_timestamp
                ) VALUES (?, ?, ?, 20261018, 'PAYMENT', ?, ?, ?, ?, ?, 'phase9-test', 1,
                          'banking.transactions.validated', 0, ?, ?)
                """, transactionId, UUID.randomUUID(), accountKey, new BigDecimal(amount), currency, status,
                Timestamp.from(occurredAt), Timestamp.from(occurredAt.plusSeconds(1)), offset,
                Timestamp.from(occurredAt));
    }

    private void insertFraud(
            long accountKey,
            UUID transactionId,
            String alertId,
            String ruleCode,
            String severity,
            String time
    ) {
        Instant detectedAt = Instant.parse("2026-10-18T" + time + "Z");
        jdbc.update("""
                INSERT INTO fact_fraud_alerts (
                    alert_id, event_id, transaction_id, account_key, date_key, rule_code, rule_version,
                    severity, amount, currency, observed_count, window_start, window_end, detected_at,
                    schema_version, source_topic, source_partition, source_offset
                ) VALUES (?, ?, ?, ?, 20261018, ?, 'v1', ?, 10.00, 'VND', 1, ?, ?, ?, 1,
                          'banking.transactions.fraud-alerts', 0, ?)
                """, UUID.fromString(alertId), UUID.randomUUID(), transactionId, accountKey, ruleCode, severity,
                Timestamp.from(detectedAt.minusSeconds(60)), Timestamp.from(detectedAt), Timestamp.from(detectedAt),
                detectedAt.getEpochSecond());
    }

    private void insertDaily() {
        jdbc.update("""
                INSERT INTO batch_runs (
                    batch_run_id, job_name, business_date, business_timezone, status,
                    source_row_count, output_row_count, source_fingerprint, gold_object_key,
                    gold_object_sha256, gold_object_size, gold_schema_version, finished_at
                ) VALUES (?, 'daily-transactions', ?, 'Asia/Ho_Chi_Minh', 'SUCCEEDED', 3, 2,
                          repeat('a', 64), 'gold/test.parquet', repeat('b', 64), 100,
                          'gold-daily-final-v1', NOW())
                """, BATCH_RUN_ID, Date.valueOf(DATE));
        jdbc.update("""
                INSERT INTO agg_daily_transactions (
                    date_key, currency, transaction_count, total_amount, failed_count, batch_run_id
                ) VALUES
                    (20261018, 'USD', 1, 5.00, 0, ?),
                    (20261018, 'VND', 2, 30.00, 1, ?)
                """, BATCH_RUN_ID, BATCH_RUN_ID);
    }

    private int count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
    }
}
