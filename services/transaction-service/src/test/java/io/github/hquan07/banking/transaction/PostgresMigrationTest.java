package io.github.hquan07.banking.transaction;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class PostgresMigrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withDatabaseName("banking")
            .withUsername("banking")
            .withPassword("banking-test-password");

    @BeforeAll
    static void migrateEmptyDatabase() {
        Flyway flyway = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load();

        assertEquals(2, flyway.migrate().migrationsExecuted);
    }

    @Test
    void createsAllPhaseOneTables() throws SQLException {
        Set<String> expected = Set.of(
                "customers",
                "accounts",
                "transactions",
                "outbox_events",
                "fraud_alerts",
                "processing_runs"
        );

        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
             );
             ResultSet resultSet = statement.executeQuery()) {
            java.util.HashSet<String> actual = new java.util.HashSet<>();
            while (resultSet.next()) {
                actual.add(resultSet.getString(1));
            }
            assertTrue(actual.containsAll(expected), () -> "created tables: " + actual);
        }
    }

    @Test
    void enforcesIdempotencyMoneyOutboxAndFraudConstraints() throws SQLException {
        seedCustomerAccountAndTransaction();

        SQLException duplicateIdempotency = assertThrows(SQLException.class, () -> execute("""
                INSERT INTO transactions (
                    transaction_id, account_id, transaction_type, amount, currency, status,
                    occurred_at, idempotency_key, request_fingerprint
                ) VALUES (
                    '00000000-0000-0000-0000-000000000102', 'ACC_PHASE1', 'PAYMENT', 5.00,
                    'VND', 'SUCCESS', NOW(), 'idem-phase1',
                    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
                )
                """));
        assertEquals("23505", duplicateIdempotency.getSQLState());

        SQLException invalidMoney = assertThrows(SQLException.class, () -> execute("""
                INSERT INTO transactions (
                    transaction_id, account_id, transaction_type, amount, currency, status,
                    occurred_at, idempotency_key, request_fingerprint
                ) VALUES (
                    '00000000-0000-0000-0000-000000000103', 'ACC_PHASE1', 'PAYMENT', -1.00,
                    'VND', 'FAILED', NOW(), 'idem-negative',
                    'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
                )
                """));
        assertEquals("23514", invalidMoney.getSQLState());

        SQLException incompleteClaim = assertThrows(SQLException.class, () -> execute("""
                INSERT INTO outbox_events (
                    event_id, aggregate_id, event_type, schema_version, payload, claim_token
                ) VALUES (
                    '00000000-0000-0000-0000-000000000201',
                    '00000000-0000-0000-0000-000000000101',
                    'transaction.created', 1, '{}'::jsonb,
                    '00000000-0000-0000-0000-000000000301'
                )
                """));
        assertEquals("23514", incompleteClaim.getSQLState());

        execute("""
                INSERT INTO fraud_alerts (
                    alert_id, transaction_id, rule_code, window_id, severity, detected_at
                ) VALUES (
                    '00000000-0000-0000-0000-000000000401',
                    '00000000-0000-0000-0000-000000000101',
                    'VELOCITY_60S', '2026-10-16T03:30:00Z/PT60S', 'HIGH', NOW()
                )
                """);

        SQLException duplicateAlert = assertThrows(SQLException.class, () -> execute("""
                INSERT INTO fraud_alerts (
                    alert_id, transaction_id, rule_code, window_id, severity, detected_at
                ) VALUES (
                    '00000000-0000-0000-0000-000000000402',
                    '00000000-0000-0000-0000-000000000101',
                    'VELOCITY_60S', '2026-10-16T03:30:00Z/PT60S', 'HIGH', NOW()
                )
                """));
        assertEquals("23505", duplicateAlert.getSQLState());
    }

    private void seedCustomerAccountAndTransaction() throws SQLException {
        execute("""
                INSERT INTO customers (customer_id, status)
                VALUES ('CUS_PHASE1', 'ACTIVE')
                """);
        execute("""
                INSERT INTO accounts (account_id, customer_id, currency, status)
                VALUES ('ACC_PHASE1', 'CUS_PHASE1', 'VND', 'ACTIVE')
                """);
        execute("""
                INSERT INTO transactions (
                    transaction_id, account_id, transaction_type, amount, currency, status,
                    occurred_at, idempotency_key, request_fingerprint
                ) VALUES (
                    '00000000-0000-0000-0000-000000000101', 'ACC_PHASE1', 'TRANSFER', 1500000.00,
                    'VND', 'SUCCESS', NOW(), 'idem-phase1',
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                )
                """);
    }

    private void execute(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()
        );
    }
}
