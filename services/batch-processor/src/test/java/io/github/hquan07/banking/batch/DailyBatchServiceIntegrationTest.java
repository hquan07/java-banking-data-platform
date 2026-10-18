package io.github.hquan07.banking.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.batch.model.PublishedObject;
import io.github.hquan07.banking.batch.persistence.BatchRepository;
import io.github.hquan07.banking.batch.quality.DailyDataQuality;
import io.github.hquan07.banking.batch.storage.ContentHashes;
import io.github.hquan07.banking.batch.storage.GoldObjectStore;
import io.github.hquan07.banking.batch.storage.GoldParquetCodec;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class DailyBatchServiceIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withDatabaseName("banking_warehouse")
            .withUsername("banking_warehouse")
            .withPassword("warehouse-test-password");

    private static final LocalDate BUSINESS_DATE = LocalDate.parse("2026-10-18");
    private static JdbcTemplate jdbc;
    private static DailyBatchService service;
    private static FakeGoldObjectStore objectStore;

    @BeforeAll
    static void setUp() {
        Flyway flyway = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load();
        assertEquals(3, flyway.migrate().migrationsExecuted);

        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()
        );
        jdbc = new JdbcTemplate(dataSource);
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        BatchProcessorProperties properties = new BatchProcessorProperties(
                "daily-transactions", "Asia/Ho_Chi_Minh", "http://minio:9000",
                "batch-test", "batch-test-secret", "banking-lake"
        );
        BatchRepository repository = new BatchRepository(
                jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
                mapper,
                properties
        );
        objectStore = new FakeGoldObjectStore();
        service = new DailyBatchService(
                repository, new DailyDataQuality(), new GoldParquetCodec(), objectStore, properties, mapper
        );
        seedTransactions();
    }

    @Test
    void rerunKeepsDailyResultsAndParquetStableThenPublishesManifest() {
        var first = service.runDaily(BUSINESS_DATE);
        byte[] firstContent = objectStore.dailyContent.clone();
        var second = service.runDaily(BUSINESS_DATE);

        assertNotEquals(first.batchRunId(), second.batchRunId());
        assertEquals(first.sourceFingerprint(), second.sourceFingerprint());
        assertEquals(first.goldObjectSha256(), second.goldObjectSha256());
        assertArrayEquals(firstContent, objectStore.dailyContent);
        assertTrue(firstContent.length > 4);
        assertArrayEquals(new byte[]{'P', 'A', 'R', '1'}, Arrays.copyOf(firstContent, 4));
        assertEquals(2, jdbc.queryForObject(
                "SELECT COUNT(*) FROM agg_daily_transactions WHERE date_key = 20261018", Integer.class
        ));
        assertEquals(new BigDecimal("35.00"), jdbc.queryForObject(
                "SELECT SUM(total_amount) FROM agg_daily_transactions WHERE date_key = 20261018", BigDecimal.class
        ));
        assertEquals(2, jdbc.queryForObject(
                "SELECT COUNT(*) FROM batch_runs WHERE business_date = '2026-10-18' AND status = 'SUCCEEDED'",
                Integer.class
        ));

        service.validateDaily(BUSINESS_DATE);
        var published = service.publishReportMetadata(BUSINESS_DATE);
        assertEquals(second.batchRunId(), published.get("batchRunId"));
        assertTrue(objectStore.manifestContent.length > 0);
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM batch_runs WHERE batch_run_id = ? AND published_at IS NOT NULL",
                Integer.class, second.batchRunId()
        ));
    }

    @Test
    void emptyPartitionFailsQualityAndPersistsFailedAudit() {
        LocalDate emptyDate = LocalDate.parse("2026-10-17");
        assertThrows(IllegalStateException.class, () -> service.runDaily(emptyDate));
        assertEquals("DQ_FAILED", jdbc.queryForObject(
                "SELECT error_code FROM batch_runs WHERE business_date = ? ORDER BY started_at DESC LIMIT 1",
                String.class, Date.valueOf(emptyDate)
        ));
    }

    private static void seedTransactions() {
        long unknownCustomer = jdbc.queryForObject(
                "SELECT customer_key FROM dim_customer WHERE customer_id = 'UNKNOWN'", Long.class
        );
        insertDate(LocalDate.parse("2026-10-18"));
        long vndAccount = insertAccount("ACC_BATCH_VND", unknownCustomer, "VND");
        long usdAccount = insertAccount("ACC_BATCH_USD", unknownCustomer, "USD");
        insertFact(vndAccount, "VND", "10.00", "SUCCESS", "2026-10-18T02:00:00Z", 1);
        insertFact(vndAccount, "VND", "20.00", "FAILED", "2026-10-18T03:00:00Z", 2);
        insertFact(usdAccount, "USD", "5.00", "PENDING", "2026-10-18T04:00:00Z", 3);
    }

    private static void insertDate(LocalDate date) {
        int dateKey = date.getYear() * 10_000 + date.getMonthValue() * 100 + date.getDayOfMonth();
        jdbc.update("""
                INSERT INTO dim_date (
                    date_key, calendar_date, calendar_year, calendar_quarter,
                    calendar_month, day_of_month, day_of_week, is_weekend
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, dateKey, Date.valueOf(date), date.getYear(), (date.getMonthValue() - 1) / 3 + 1,
                date.getMonthValue(), date.getDayOfMonth(), date.getDayOfWeek().getValue(),
                date.getDayOfWeek().getValue() >= 6);
    }

    private static long insertAccount(String accountId, long customerKey, String currency) {
        return jdbc.queryForObject("""
                INSERT INTO dim_account (account_id, customer_key, currency)
                VALUES (?, ?, ?) RETURNING account_key
                """, Long.class, accountId, customerKey, currency);
    }

    private static void insertFact(
            long accountKey,
            String currency,
            String amount,
            String status,
            String occurredAt,
            long offset
    ) {
        Instant occurred = Instant.parse(occurredAt);
        jdbc.update("""
                INSERT INTO fact_transactions (
                    transaction_id, event_id, account_key, date_key, transaction_type, amount, currency,
                    status, occurred_at, produced_at, correlation_id, schema_version,
                    source_topic, source_partition, source_offset, source_timestamp
                ) VALUES (?, ?, ?, 20261018, 'PAYMENT', ?, ?, ?, ?, ?, 'phase8-test', 1,
                          'banking.transactions.validated', 0, ?, ?)
                """, UUID.randomUUID(), UUID.randomUUID(), accountKey, new BigDecimal(amount), currency, status,
                Timestamp.from(occurred), Timestamp.from(occurred.plusSeconds(1)), offset, Timestamp.from(occurred));
    }

    private static final class FakeGoldObjectStore implements GoldObjectStore {
        private byte[] dailyContent;
        private byte[] manifestContent;
        private final Map<String, PublishedObject> objects = new LinkedHashMap<>();

        @Override
        public void verifyBucket() {
        }

        @Override
        public PublishedObject publishDaily(
                LocalDate businessDate,
                byte[] content,
                long rowCount,
                String schemaVersion,
                String sourceFingerprint
        ) {
            dailyContent = content.clone();
            String key = "gold/daily_transactions/report_date=" + businessDate + "/daily-transactions-v1.parquet";
            PublishedObject object = new PublishedObject(
                    key, ContentHashes.sha256(content), content.length, rowCount, schemaVersion
            );
            objects.put(key, object);
            return object;
        }

        @Override
        public void verify(PublishedObject object) {
            assertEquals(objects.get(object.key()), object);
            assertEquals(object.sha256(), ContentHashes.sha256(dailyContent));
        }

        @Override
        public String publishManifest(LocalDate businessDate, byte[] content) {
            manifestContent = content.clone();
            return "gold/daily_transactions/report_date=" + businessDate + "/_SUCCESS.json";
        }
    }
}
