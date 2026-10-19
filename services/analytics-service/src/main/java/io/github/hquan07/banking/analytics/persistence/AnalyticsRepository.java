package io.github.hquan07.banking.analytics.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class AnalyticsRepository {

    private final JdbcTemplate jdbc;

    public AnalyticsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<DailyRow> daily(LocalDate date) {
        return jdbc.query("""
                SELECT daily.currency, daily.transaction_count, daily.total_amount, daily.failed_count,
                       daily.batch_run_id, daily.updated_at, runs.business_timezone
                  FROM agg_daily_transactions daily
                  JOIN batch_runs runs ON runs.batch_run_id = daily.batch_run_id
                 WHERE daily.date_key = ? AND runs.status = 'SUCCEEDED'
                 ORDER BY daily.currency
                """, (resultSet, rowNumber) -> new DailyRow(
                resultSet.getString("currency"),
                resultSet.getLong("transaction_count"),
                resultSet.getBigDecimal("total_amount"),
                resultSet.getLong("failed_count"),
                resultSet.getObject("batch_run_id", UUID.class),
                resultSet.getTimestamp("updated_at").toInstant(),
                resultSet.getString("business_timezone")
        ), dateKey(date));
    }

    public long fraudCount(Instant from, Instant to) {
        Long count = jdbc.queryForObject("""
                SELECT COUNT(*)
                  FROM fact_fraud_alerts
                 WHERE detected_at >= ? AND detected_at < ?
                """, Long.class, Timestamp.from(from), Timestamp.from(to));
        return count == null ? 0 : count;
    }

    public List<FraudRow> fraud(Instant from, Instant to, int size, long offset) {
        return jdbc.query("""
                SELECT fraud.alert_id, fraud.event_id, fraud.transaction_id, account.account_id,
                       fraud.rule_code, fraud.rule_version, fraud.severity, fraud.amount, fraud.currency,
                       fraud.observed_count, fraud.window_start, fraud.window_end, fraud.detected_at
                  FROM fact_fraud_alerts fraud
                  JOIN dim_account account ON account.account_key = fraud.account_key
                 WHERE fraud.detected_at >= ? AND fraud.detected_at < ?
                 ORDER BY fraud.detected_at DESC, fraud.alert_id DESC
                 LIMIT ? OFFSET ?
                """, (resultSet, rowNumber) -> new FraudRow(
                resultSet.getObject("alert_id", UUID.class),
                resultSet.getObject("event_id", UUID.class),
                resultSet.getObject("transaction_id", UUID.class),
                resultSet.getString("account_id"),
                resultSet.getString("rule_code"),
                resultSet.getString("rule_version"),
                resultSet.getString("severity"),
                resultSet.getBigDecimal("amount"),
                resultSet.getString("currency"),
                resultSet.getLong("observed_count"),
                resultSet.getTimestamp("window_start").toInstant(),
                resultSet.getTimestamp("window_end").toInstant(),
                resultSet.getTimestamp("detected_at").toInstant()
        ), Timestamp.from(from), Timestamp.from(to), size, offset);
    }

    public Optional<AccountSummaryRow> accountSummary(String accountId) {
        List<AccountSummaryRow> results = jdbc.query("""
                SELECT account.account_id, customer.customer_id, account.currency,
                       account.account_status, account.is_inferred, account.first_seen_at,
                       COUNT(transaction.transaction_id) AS transaction_count,
                       COALESCE(SUM(transaction.amount), 0) AS total_amount,
                       COUNT(*) FILTER (WHERE transaction.status = 'SUCCESS') AS success_count,
                       COUNT(*) FILTER (WHERE transaction.status = 'FAILED') AS failed_count,
                       COUNT(*) FILTER (WHERE transaction.status = 'PENDING') AS pending_count,
                       COUNT(*) FILTER (WHERE transaction.status = 'REVERSED') AS reversed_count,
                       MIN(transaction.occurred_at) AS first_transaction_at,
                       MAX(transaction.occurred_at) AS last_transaction_at,
                       (SELECT COUNT(*) FROM fact_fraud_alerts fraud
                         WHERE fraud.account_key = account.account_key) AS fraud_alert_count,
                       (SELECT MAX(fraud.detected_at) FROM fact_fraud_alerts fraud
                         WHERE fraud.account_key = account.account_key) AS latest_fraud_detected_at
                  FROM dim_account account
                  JOIN dim_customer customer ON customer.customer_key = account.customer_key
                  LEFT JOIN fact_transactions transaction ON transaction.account_key = account.account_key
                 WHERE account.account_id = ?
                 GROUP BY account.account_key, account.account_id, customer.customer_id,
                          account.currency, account.account_status, account.is_inferred, account.first_seen_at
                """, (resultSet, rowNumber) -> new AccountSummaryRow(
                resultSet.getString("account_id"),
                resultSet.getString("customer_id"),
                resultSet.getString("currency"),
                resultSet.getString("account_status"),
                resultSet.getBoolean("is_inferred"),
                resultSet.getTimestamp("first_seen_at").toInstant(),
                resultSet.getLong("transaction_count"),
                resultSet.getBigDecimal("total_amount"),
                resultSet.getLong("success_count"),
                resultSet.getLong("failed_count"),
                resultSet.getLong("pending_count"),
                resultSet.getLong("reversed_count"),
                instant(resultSet.getTimestamp("first_transaction_at")),
                instant(resultSet.getTimestamp("last_transaction_at")),
                resultSet.getLong("fraud_alert_count"),
                instant(resultSet.getTimestamp("latest_fraud_detected_at"))
        ), accountId);
        return results.stream().findFirst();
    }

    private int dateKey(LocalDate date) {
        return date.getYear() * 10_000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    public record DailyRow(
            String currency,
            long transactionCount,
            BigDecimal totalAmount,
            long failedCount,
            UUID batchRunId,
            Instant updatedAt,
            String businessTimezone
    ) {
    }

    public record FraudRow(
            UUID alertId,
            UUID eventId,
            UUID transactionId,
            String accountId,
            String ruleCode,
            String ruleVersion,
            String severity,
            BigDecimal amount,
            String currency,
            long observedCount,
            Instant windowStart,
            Instant windowEnd,
            Instant detectedAt
    ) {
    }

    public record AccountSummaryRow(
            String accountId,
            String customerId,
            String currency,
            String accountStatus,
            boolean inferred,
            Instant firstSeenAt,
            long transactionCount,
            BigDecimal totalAmount,
            long successCount,
            long failedCount,
            long pendingCount,
            long reversedCount,
            Instant firstTransactionAt,
            Instant lastTransactionAt,
            long fraudAlertCount,
            Instant latestFraudDetectedAt
    ) {
    }
}
