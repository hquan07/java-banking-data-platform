package io.github.hquan07.banking.warehouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.common.event.FraudAlertEvent;
import io.github.hquan07.banking.common.event.FraudRule;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;
import io.github.hquan07.banking.warehouse.model.RecordKind;
import io.github.hquan07.banking.warehouse.model.WarehouseRecord;
import io.github.hquan07.banking.warehouse.persistence.WarehouseBatchRepository;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class WarehouseRepositoryIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withDatabaseName("banking_warehouse")
            .withUsername("banking_warehouse")
            .withPassword("warehouse-test-password");

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static JdbcTemplate jdbc;
    private static WarehouseBatchRepository repository;

    @BeforeAll
    static void migrate() {
        Flyway flyway = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load();
        assertEquals(1, flyway.migrate().migrationsExecuted);

        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()
        );
        jdbc = new JdbcTemplate(dataSource);
        repository = new WarehouseBatchRepository(
                jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
                MAPPER
        );
    }

    @Test
    void replayingOneThousandBusinessEventsDoesNotIncreaseCountOrSum() throws Exception {
        int processedBefore = count("processed_kafka_records");
        int reconciliationRunsBefore = count("warehouse_reconciliation_runs");
        List<WarehouseRecord> firstDelivery = records(0);
        var first = repository.process(RecordKind.TRANSACTION, "validated-test", firstDelivery);

        assertEquals(1000, first.inserted());
        assertEquals(0, first.updated());
        assertWarehouseTotals(1000, new BigDecimal("10000.00"));

        List<WarehouseRecord> replayAtNewKafkaOffsets = records(1000);
        var replay = repository.process(RecordKind.TRANSACTION, "validated-test", replayAtNewKafkaOffsets);

        assertEquals(0, replay.inserted());
        assertEquals(1000, replay.updated());
        assertWarehouseTotals(1000, new BigDecimal("10000.00"));
        assertEquals(processedBefore + 2000, count("processed_kafka_records"));
        assertEquals(reconciliationRunsBefore + 2, count("warehouse_reconciliation_runs"));
    }

    @Test
    void quarantinesMalformedPayloadAndStagesDeterministicDlqRecord() {
        WarehouseRecord malformed = new WarehouseRecord(
                "banking.transactions.validated", 1, 50_000, Instant.now().toEpochMilli(),
                "{\"event_id\":\"not-a-uuid\"}".getBytes(StandardCharsets.UTF_8)
        );

        var result = repository.process(RecordKind.TRANSACTION, "validated-test", List.of(malformed));

        assertEquals(1, result.invalid());
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM warehouse_rejections WHERE source_offset = 50000", Integer.class
        ));
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM warehouse_dlq_outbox WHERE source_offset = 50000 AND published_at IS NULL",
                Integer.class
        ));
        assertEquals("REJECTED", jdbc.queryForObject(
                "SELECT outcome FROM processed_kafka_records WHERE source_offset = 50000", String.class
        ));
    }

    @Test
    void migrationCreatesStarSchemaAndPhaseEightDailyTarget() {
        List<String> tables = jdbc.queryForList("""
                SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
                """, String.class);
        assertTrue(tables.containsAll(List.of(
                "dim_customer", "dim_account", "dim_date", "fact_transactions", "fact_fraud_alerts",
                "agg_transaction_windows", "agg_daily_transactions", "processed_kafka_records",
                "warehouse_rejections", "warehouse_dlq_outbox", "warehouse_reconciliation_runs"
        )));
    }

    @Test
    void linksAnEarlierFraudAlertWhenItsTransactionArrivesLater() throws Exception {
        UUID eventId = UUID.fromString("75000000-0000-4000-8000-000000000001");
        UUID transactionId = UUID.fromString("76000000-0000-4000-8000-000000000001");
        UUID alertId = UUID.fromString("77000000-0000-4000-8000-000000000001");
        Instant occurredAt = Instant.parse("2026-10-18T04:00:00Z");
        FraudAlertEvent alert = new FraudAlertEvent(
                alertId, FraudRule.HIGH_VALUE, FraudAlertEvent.RULE_VERSION, eventId,
                "ACC_LATE_FACT", new BigDecimal("500.00"), "VND", 1,
                occurredAt.minusSeconds(60), occurredAt, occurredAt, FraudAlertEvent.SCHEMA_VERSION
        );
        repository.process(RecordKind.FRAUD_ALERT, "fraud-test", List.of(new WarehouseRecord(
                "banking.transactions.fraud-alerts", 2, 60_000, occurredAt.toEpochMilli(),
                MAPPER.writeValueAsBytes(alert)
        )));
        assertEquals(0, jdbc.queryForObject(
                "SELECT COUNT(transaction_id) FROM fact_fraud_alerts WHERE alert_id = ?", Integer.class, alertId
        ));

        TransactionCreatedEvent transaction = new TransactionCreatedEvent(
                eventId, TransactionCreatedEvent.EVENT_TYPE, TransactionCreatedEvent.SCHEMA_VERSION,
                transactionId, "ACC_LATE_FACT", TransactionType.PAYMENT, new BigDecimal("500.00"),
                "VND", TransactionStatus.SUCCESS, occurredAt, occurredAt.plusSeconds(1), "phase7-late-fact"
        );
        repository.process(RecordKind.TRANSACTION, "validated-test", List.of(new WarehouseRecord(
                "banking.transactions.validated", 2, 60_001, occurredAt.toEpochMilli(),
                MAPPER.writeValueAsBytes(transaction)
        )));

        assertEquals(transactionId, jdbc.queryForObject(
                "SELECT transaction_id FROM fact_fraud_alerts WHERE alert_id = ?", UUID.class, alertId
        ));

        TransactionCreatedEvent conflictingCurrency = new TransactionCreatedEvent(
                UUID.fromString("75000000-0000-4000-8000-000000000002"),
                TransactionCreatedEvent.EVENT_TYPE, TransactionCreatedEvent.SCHEMA_VERSION,
                UUID.fromString("76000000-0000-4000-8000-000000000002"),
                "ACC_LATE_FACT", TransactionType.PAYMENT, new BigDecimal("1.00"),
                "USD", TransactionStatus.SUCCESS, occurredAt, occurredAt.plusSeconds(2), "phase7-currency-conflict"
        );
        var rejected = repository.process(RecordKind.TRANSACTION, "validated-test", List.of(new WarehouseRecord(
                "banking.transactions.validated", 2, 60_002, occurredAt.toEpochMilli(),
                MAPPER.writeValueAsBytes(conflictingCurrency)
        )));

        assertEquals(1, rejected.invalid());
        assertEquals("VND", jdbc.queryForObject(
                "SELECT currency FROM dim_account WHERE account_id = 'ACC_LATE_FACT'", String.class
        ));
        assertEquals(0, jdbc.queryForObject(
                "SELECT COUNT(*) FROM fact_transactions WHERE correlation_id = 'phase7-currency-conflict'",
                Integer.class
        ));
    }

    private static List<WarehouseRecord> records(long firstOffset) throws Exception {
        List<WarehouseRecord> records = new ArrayList<>(1000);
        Instant occurredAt = Instant.parse("2026-10-18T03:00:00Z");
        for (int index = 0; index < 1000; index++) {
            UUID transactionId = UUID.nameUUIDFromBytes(("transaction-" + index).getBytes(StandardCharsets.UTF_8));
            UUID eventId = UUID.nameUUIDFromBytes(("event-" + index).getBytes(StandardCharsets.UTF_8));
            TransactionCreatedEvent event = new TransactionCreatedEvent(
                    eventId, TransactionCreatedEvent.EVENT_TYPE, TransactionCreatedEvent.SCHEMA_VERSION,
                    transactionId, "ACC" + (10_000 + index % 10), TransactionType.PAYMENT,
                    new BigDecimal("10.00"), "VND", TransactionStatus.SUCCESS,
                    occurredAt.plusSeconds(index), occurredAt.plusSeconds(index + 1), "phase7-test"
            );
            records.add(new WarehouseRecord(
                    "banking.transactions.validated", 0, firstOffset + index,
                    occurredAt.plusSeconds(index).toEpochMilli(), MAPPER.writeValueAsBytes(event)
            ));
        }
        return records;
    }

    private static void assertWarehouseTotals(int expectedCount, BigDecimal expectedSum) {
        assertEquals(expectedCount, jdbc.queryForObject(
                "SELECT COUNT(*) FROM fact_transactions WHERE correlation_id = 'phase7-test'", Integer.class
        ));
        assertEquals(expectedSum, jdbc.queryForObject(
                "SELECT SUM(amount) FROM fact_transactions WHERE correlation_id = 'phase7-test'", BigDecimal.class
        ));
    }

    private static int count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
    }
}
