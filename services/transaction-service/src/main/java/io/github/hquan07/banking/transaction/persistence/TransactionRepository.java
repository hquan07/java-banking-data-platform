package io.github.hquan07.banking.transaction.persistence;

import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

@Repository
public class TransactionRepository {

    private static final String SELECT_COLUMNS = """
            SELECT transaction_id, account_id, transaction_type, amount, currency, status,
                   occurred_at, created_at, idempotency_key, request_fingerprint, correlation_id
            FROM transactions
            """;

    private final JdbcTemplate jdbcTemplate;

    public TransactionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void lockIdempotencyKey(String idempotencyKey) {
        jdbcTemplate.queryForList(
                "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
                idempotencyKey
        );
    }

    public Optional<StoredTransaction> findByIdempotencyKey(String idempotencyKey) {
        return queryOne(SELECT_COLUMNS + " WHERE idempotency_key = ?", idempotencyKey);
    }

    public Optional<StoredTransaction> findById(UUID transactionId) {
        return queryOne(SELECT_COLUMNS + " WHERE transaction_id = ?", transactionId);
    }

    public void insert(StoredTransaction transaction) {
        jdbcTemplate.update("""
                        INSERT INTO transactions (
                            transaction_id, account_id, transaction_type, amount, currency, status,
                            occurred_at, created_at, idempotency_key, request_fingerprint, correlation_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                transaction.transactionId(),
                transaction.accountId(),
                transaction.transactionType().name(),
                transaction.amount(),
                transaction.currency(),
                transaction.status().name(),
                Timestamp.from(transaction.occurredAt()),
                Timestamp.from(transaction.createdAt()),
                transaction.idempotencyKey(),
                transaction.requestFingerprint(),
                transaction.correlationId()
        );
    }

    private Optional<StoredTransaction> queryOne(String sql, Object argument) {
        return jdbcTemplate.query(
                sql,
                (resultSet, rowNumber) -> new StoredTransaction(
                        resultSet.getObject("transaction_id", UUID.class),
                        resultSet.getString("account_id"),
                        TransactionType.valueOf(resultSet.getString("transaction_type")),
                        resultSet.getBigDecimal("amount"),
                        resultSet.getString("currency").trim(),
                        TransactionStatus.valueOf(resultSet.getString("status")),
                        resultSet.getTimestamp("occurred_at").toInstant(),
                        resultSet.getTimestamp("created_at").toInstant(),
                        resultSet.getString("idempotency_key"),
                        resultSet.getString("request_fingerprint").trim(),
                        resultSet.getString("correlation_id")
                ),
                argument
        ).stream().findFirst();
    }
}
