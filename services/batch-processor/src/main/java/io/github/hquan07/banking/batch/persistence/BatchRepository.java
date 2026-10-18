package io.github.hquan07.banking.batch.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.batch.BatchProcessorProperties;
import io.github.hquan07.banking.batch.model.BatchRun;
import io.github.hquan07.banking.batch.model.DailyAggregate;
import io.github.hquan07.banking.batch.model.DailyTarget;
import io.github.hquan07.banking.batch.model.PublishedObject;
import io.github.hquan07.banking.batch.model.SourceSnapshot;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

@Repository
public class BatchRepository {

    private static final int DAILY_LOCK_NAMESPACE = 16_072_026;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final BatchProcessorProperties properties;

    public BatchRepository(
            JdbcTemplate jdbc,
            TransactionTemplate transactionTemplate,
            ObjectMapper objectMapper,
            BatchProcessorProperties properties
    ) {
        this.jdbc = jdbc;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public UUID createRun(LocalDate businessDate) {
        UUID runId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO batch_runs (
                    batch_run_id, job_name, business_date, business_timezone, status
                ) VALUES (?, ?, ?, ?, 'RUNNING')
                """, runId, properties.jobName(), Date.valueOf(businessDate), properties.businessTimezone());
        return runId;
    }

    public <T> T inTransaction(Supplier<T> work) {
        return transactionTemplate.execute(status -> work.get());
    }

    public void acquireDailyLock(LocalDate businessDate) {
        Boolean acquired = jdbc.queryForObject(
                "SELECT pg_try_advisory_xact_lock(?, ?)",
                Boolean.class,
                DAILY_LOCK_NAMESPACE,
                Math.toIntExact(businessDate.toEpochDay())
        );
        if (!Boolean.TRUE.equals(acquired)) {
            throw new IllegalStateException("Another daily batch is already running for " + businessDate);
        }
    }

    public SourceSnapshot sourceSnapshot(LocalDate businessDate) {
        ZoneId zone = properties.zoneId();
        Instant start = businessDate.atStartOfDay(zone).toInstant();
        Instant end = businessDate.plusDays(1).atStartOfDay(zone).toInstant();
        List<DailyAggregate> rows = jdbc.query("""
                SELECT currency,
                       COUNT(*) AS transaction_count,
                       COALESCE(SUM(amount), 0) AS total_amount,
                       COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_count,
                       COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_count,
                       COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_count,
                       COUNT(*) FILTER (WHERE status = 'REVERSED') AS reversed_count
                  FROM fact_transactions
                 WHERE occurred_at >= ? AND occurred_at < ?
                 GROUP BY currency
                 ORDER BY currency
                """, (resultSet, rowNumber) -> new DailyAggregate(
                businessDate,
                resultSet.getString("currency"),
                resultSet.getLong("transaction_count"),
                resultSet.getBigDecimal("total_amount").setScale(2),
                resultSet.getLong("success_count"),
                resultSet.getLong("failed_count"),
                resultSet.getLong("pending_count"),
                resultSet.getLong("reversed_count")
        ), Timestamp.from(start), Timestamp.from(end));

        long count = rows.stream().mapToLong(DailyAggregate::transactionCount).sum();
        BigDecimal total = rows.stream().map(DailyAggregate::totalAmount)
                .reduce(BigDecimal.ZERO.setScale(2), BigDecimal::add);
        Map<String, BigDecimal> currencyTotals = new LinkedHashMap<>();
        rows.forEach(row -> currencyTotals.put(row.currency(), row.totalAmount()));
        return new SourceSnapshot(
                businessDate, start, end, count, total, Map.copyOf(currencyTotals), fingerprint(rows), List.copyOf(rows)
        );
    }

    public void replaceDailyAggregates(UUID runId, SourceSnapshot source) {
        int dateKey = dateKey(source.businessDate());
        ensureDate(source.businessDate(), dateKey);
        jdbc.update("DELETE FROM agg_daily_transactions WHERE date_key = ?", dateKey);
        if (!source.rows().isEmpty()) {
            List<Object[]> arguments = source.rows().stream().map(row -> new Object[]{
                    dateKey, row.currency(), row.transactionCount(), row.totalAmount(), row.failedCount(), runId
            }).toList();
            jdbc.batchUpdate("""
                    INSERT INTO agg_daily_transactions (
                        date_key, currency, transaction_count, total_amount, failed_count, batch_run_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, NOW())
                    """, arguments);
        }
    }

    public List<DailyTarget> dailyTargets(LocalDate businessDate) {
        return jdbc.query("""
                SELECT currency, transaction_count, total_amount, failed_count
                  FROM agg_daily_transactions
                 WHERE date_key = ?
                 ORDER BY currency
                """, (resultSet, rowNumber) -> new DailyTarget(
                resultSet.getString("currency"),
                resultSet.getLong("transaction_count"),
                resultSet.getBigDecimal("total_amount").setScale(2),
                resultSet.getLong("failed_count")
        ), dateKey(businessDate));
    }

    public void markSucceeded(
            UUID runId,
            SourceSnapshot source,
            PublishedObject object,
            Map<String, Object> checks
    ) {
        int updated = jdbc.update("""
                UPDATE batch_runs
                   SET status = 'SUCCEEDED',
                       source_row_count = ?,
                       output_row_count = ?,
                       source_currency_totals = ?::jsonb,
                       output_currency_totals = ?::jsonb,
                       source_fingerprint = ?,
                       gold_object_key = ?,
                       gold_object_sha256 = ?,
                       gold_object_size = ?,
                       gold_schema_version = ?,
                       dq_checks = ?::jsonb,
                       finished_at = NOW(),
                       error_code = NULL,
                       error_message = NULL
                 WHERE batch_run_id = ? AND status = 'RUNNING'
                """,
                source.sourceRowCount(), source.rows().size(), json(source.currencyTotals()),
                json(source.currencyTotals()), source.fingerprint(), object.key(), object.sha256(), object.size(),
                object.schemaVersion(), json(checks), runId
        );
        if (updated != 1) {
            throw new IllegalStateException("Batch audit row is not RUNNING: " + runId);
        }
    }

    public void markFailed(UUID runId, String code, String message) {
        jdbc.update("""
                UPDATE batch_runs
                   SET status = 'FAILED', finished_at = NOW(), error_code = ?, error_message = ?
                 WHERE batch_run_id = ? AND status = 'RUNNING'
                """, code, truncate(message, 1024), runId);
    }

    public BatchRun latestSuccessful(LocalDate businessDate) {
        List<BatchRun> runs = jdbc.query("""
                SELECT batch_run_id, job_name, business_date, business_timezone, status,
                       source_row_count, output_row_count, source_fingerprint,
                       gold_object_key, gold_object_sha256, gold_object_size, gold_schema_version,
                       started_at, finished_at, published_at
                  FROM batch_runs
                 WHERE job_name = ? AND business_date = ? AND status = 'SUCCEEDED'
                 ORDER BY finished_at DESC, started_at DESC
                 LIMIT 1
                """, (resultSet, rowNumber) -> new BatchRun(
                resultSet.getObject("batch_run_id", UUID.class),
                resultSet.getString("job_name"),
                resultSet.getObject("business_date", LocalDate.class),
                resultSet.getString("business_timezone"),
                resultSet.getString("status"),
                resultSet.getLong("source_row_count"),
                resultSet.getInt("output_row_count"),
                resultSet.getString("source_fingerprint"),
                new PublishedObject(
                        resultSet.getString("gold_object_key"),
                        resultSet.getString("gold_object_sha256"),
                        resultSet.getLong("gold_object_size"),
                        resultSet.getInt("output_row_count"),
                        resultSet.getString("gold_schema_version")
                ),
                resultSet.getTimestamp("started_at").toInstant(),
                resultSet.getTimestamp("finished_at").toInstant(),
                resultSet.getTimestamp("published_at") == null
                        ? null : resultSet.getTimestamp("published_at").toInstant()
        ), properties.jobName(), Date.valueOf(businessDate));
        if (runs.isEmpty()) {
            throw new IllegalStateException("No successful daily batch exists for " + businessDate);
        }
        return runs.getFirst();
    }

    public void markPublished(UUID runId) {
        int updated = jdbc.update("""
                UPDATE batch_runs SET published_at = COALESCE(published_at, NOW())
                 WHERE batch_run_id = ? AND status = 'SUCCEEDED'
                """, runId);
        if (updated != 1) {
            throw new IllegalStateException("Unable to publish batch metadata for " + runId);
        }
    }

    private void ensureDate(LocalDate date, int dateKey) {
        jdbc.update("""
                INSERT INTO dim_date (
                    date_key, calendar_date, calendar_year, calendar_quarter,
                    calendar_month, day_of_month, day_of_week, is_weekend
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (date_key) DO NOTHING
                """, dateKey, Date.valueOf(date), date.getYear(), (date.getMonthValue() - 1) / 3 + 1,
                date.getMonthValue(), date.getDayOfMonth(), date.getDayOfWeek().getValue(),
                date.getDayOfWeek().getValue() >= 6);
    }

    private int dateKey(LocalDate date) {
        return date.getYear() * 10_000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    }

    private String fingerprint(List<DailyAggregate> rows) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (DailyAggregate row : rows) {
                String canonical = row.businessDate() + "|" + row.currency() + "|" + row.transactionCount()
                        + "|" + row.totalAmount().setScale(2).toPlainString() + "|" + row.successCount()
                        + "|" + row.failedCount() + "|" + row.pendingCount() + "|" + row.reversedCount() + "\n";
                digest.update(canonical.getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to fingerprint daily source", exception);
        }
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize batch audit JSON", exception);
        }
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.isBlank()) {
            return "Unknown batch failure";
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
