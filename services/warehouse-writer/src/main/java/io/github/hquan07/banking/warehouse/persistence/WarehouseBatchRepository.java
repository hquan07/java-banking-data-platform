package io.github.hquan07.banking.warehouse.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.common.event.FailureClass;
import io.github.hquan07.banking.common.event.FraudAlertEvent;
import io.github.hquan07.banking.common.event.FraudRule;
import io.github.hquan07.banking.common.event.ReplayStatus;
import io.github.hquan07.banking.common.event.TransactionAggregateEvent;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.warehouse.model.PendingDlqRecord;
import io.github.hquan07.banking.warehouse.model.RecordKind;
import io.github.hquan07.banking.warehouse.model.WarehouseRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class WarehouseBatchRepository {

    private static final UUID REJECTION_NAMESPACE = UUID.fromString("89d6357a-eb3f-4b60-8e93-16d73fe51c07");

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final ObjectMapper mapper;

    public WarehouseBatchRepository(JdbcTemplate jdbc, TransactionTemplate transactions, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.transactions = transactions;
        this.mapper = mapper;
    }

    public BatchResult process(RecordKind kind, String consumerName, List<WarehouseRecord> records) {
        if (records.isEmpty()) {
            return new BatchResult(0, 0, 0, 0, 0);
        }
        return transactions.execute(status -> processInTransaction(kind, consumerName, records));
    }

    public List<PendingDlqRecord> pendingDlq(int limit) {
        return jdbc.query("""
                SELECT failure_id, message_key, payload::text
                FROM warehouse_dlq_outbox
                WHERE published_at IS NULL
                ORDER BY created_at
                LIMIT ?
                """, (rs, row) -> new PendingDlqRecord(
                rs.getObject("failure_id", UUID.class),
                rs.getString("message_key"),
                rs.getString("payload").getBytes(StandardCharsets.UTF_8)
        ), limit);
    }

    public void markDlqPublished(UUID failureId) {
        jdbc.update("UPDATE warehouse_dlq_outbox SET published_at = NOW(), attempts = attempts + 1, last_error = NULL WHERE failure_id = ?", failureId);
    }

    public void markDlqFailed(UUID failureId, Exception exception) {
        jdbc.update("UPDATE warehouse_dlq_outbox SET attempts = attempts + 1, last_error = ? WHERE failure_id = ?",
                abbreviate(exception.getClass().getSimpleName() + ": " + exception.getMessage(), 1024), failureId);
    }

    private BatchResult processInTransaction(RecordKind kind, String consumerName, List<WarehouseRecord> records) {
        Instant startedAt = Instant.now();
        MutableCounts counts = new MutableCounts();
        Map<String, UpsertResult> uniqueEvents = new LinkedHashMap<>();
        Map<String, BigDecimal> currencyTotals = new LinkedHashMap<>();

        for (WarehouseRecord record : records) {
            String hash = sha256(record.payload());
            if (!claim(record, kind, hash)) {
                counts.duplicates++;
                continue;
            }

            Object event;
            try {
                event = parse(kind, record.payload());
            } catch (Exception exception) {
                reject(record, hash, exception);
                finishMarker(record, null, "REJECTED");
                counts.invalid++;
                counts.rejected++;
                continue;
            }

            UpsertResult upsert;
            try {
                upsert = switch (kind) {
                    case TRANSACTION -> upsertTransaction(record, (TransactionCreatedEvent) event);
                    case FRAUD_ALERT -> upsertFraudAlert(record, (FraudAlertEvent) event);
                    case AGGREGATE -> upsertAggregate(record, (TransactionAggregateEvent) event);
                };
            } catch (RejectedRecordException exception) {
                reject(record, hash, exception);
                finishMarker(record, null, "REJECTED");
                counts.invalid++;
                counts.rejected++;
                continue;
            }
            finishMarker(record, upsert.identifier(), "ACCEPTED");
            counts.valid++;
            if (upsert.inserted()) {
                counts.inserted++;
            } else {
                counts.updated++;
            }
            uniqueEvents.put(upsert.identifier(), upsert);
            currencyTotals.merge(upsert.currency(), upsert.amount(), BigDecimal::add);
        }

        saveReconciliation(
                consumerName, records, counts, uniqueEvents, currencyTotals, startedAt, Instant.now()
        );
        return new BatchResult(counts.valid, counts.invalid, counts.inserted, counts.updated, counts.duplicates);
    }

    private Object parse(RecordKind kind, byte[] payload) throws Exception {
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("payload is empty");
        }
        return switch (kind) {
            case TRANSACTION -> mapper.readValue(payload, TransactionCreatedEvent.class);
            case FRAUD_ALERT -> mapper.readValue(payload, FraudAlertEvent.class);
            case AGGREGATE -> mapper.readValue(payload, TransactionAggregateEvent.class);
        };
    }

    private boolean claim(WarehouseRecord record, RecordKind kind, String payloadHash) {
        List<Integer> inserted = jdbc.query("""
                INSERT INTO processed_kafka_records (
                    source_topic, source_partition, source_offset, record_type, payload_hash
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                RETURNING 1
                """, (rs, row) -> rs.getInt(1), record.topic(), record.partition(), record.offset(), kind.name(), payloadHash);
        return !inserted.isEmpty();
    }

    private void finishMarker(WarehouseRecord record, String identifier, String outcome) {
        jdbc.update("""
                UPDATE processed_kafka_records
                SET event_identifier = ?, outcome = ?, processed_at = NOW()
                WHERE source_topic = ? AND source_partition = ? AND source_offset = ?
                """, identifier, outcome, record.topic(), record.partition(), record.offset());
    }

    private UpsertResult upsertTransaction(WarehouseRecord record, TransactionCreatedEvent event) {
        long accountKey = resolveAccount(event.accountId(), event.currency());
        int dateKey = resolveDate(event.occurredAt());
        Boolean inserted = jdbc.queryForObject("""
                INSERT INTO fact_transactions (
                    transaction_id, event_id, account_key, date_key, transaction_type, amount,
                    currency, status, occurred_at, produced_at, correlation_id, schema_version,
                    source_topic, source_partition, source_offset, source_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    account_key = EXCLUDED.account_key,
                    date_key = EXCLUDED.date_key,
                    transaction_type = EXCLUDED.transaction_type,
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    status = EXCLUDED.status,
                    occurred_at = EXCLUDED.occurred_at,
                    produced_at = EXCLUDED.produced_at,
                    correlation_id = EXCLUDED.correlation_id,
                    schema_version = EXCLUDED.schema_version,
                    source_topic = EXCLUDED.source_topic,
                    source_partition = EXCLUDED.source_partition,
                    source_offset = EXCLUDED.source_offset,
                    source_timestamp = EXCLUDED.source_timestamp,
                    updated_at = NOW()
                RETURNING (xmax = 0)
                """, Boolean.class,
                event.transactionId(), event.eventId(), accountKey, dateKey, event.transactionType().name(),
                event.amount(), event.currency(), event.status().name(), Timestamp.from(event.occurredAt()),
                Timestamp.from(event.producedAt()), event.correlationId(), event.schemaVersion(), record.topic(),
                record.partition(), record.offset(), timestamp(record.timestamp()));
        jdbc.update("""
                UPDATE fact_fraud_alerts
                SET transaction_id = ?, updated_at = NOW()
                WHERE event_id = ? AND transaction_id IS NULL
                """, event.transactionId(), event.eventId());
        return new UpsertResult(event.transactionId().toString(), event.currency(), event.amount(), Boolean.TRUE.equals(inserted));
    }

    private UpsertResult upsertFraudAlert(WarehouseRecord record, FraudAlertEvent event) {
        long accountKey = resolveAccount(event.accountId(), event.currency());
        int dateKey = resolveDate(event.detectedAt());
        UUID transactionId = event.eventId() == null ? null : jdbc.query("""
                SELECT transaction_id FROM fact_transactions WHERE event_id = ?
                """, rs -> rs.next() ? rs.getObject(1, UUID.class) : null, event.eventId());
        Boolean inserted = jdbc.queryForObject("""
                INSERT INTO fact_fraud_alerts (
                    alert_id, event_id, transaction_id, account_key, date_key, rule_code, rule_version,
                    severity, amount, currency, observed_count, window_start, window_end, detected_at,
                    schema_version, source_topic, source_partition, source_offset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (alert_id) DO UPDATE SET
                    transaction_id = COALESCE(EXCLUDED.transaction_id, fact_fraud_alerts.transaction_id),
                    account_key = EXCLUDED.account_key,
                    date_key = EXCLUDED.date_key,
                    rule_code = EXCLUDED.rule_code,
                    rule_version = EXCLUDED.rule_version,
                    severity = EXCLUDED.severity,
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    observed_count = EXCLUDED.observed_count,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    detected_at = EXCLUDED.detected_at,
                    schema_version = EXCLUDED.schema_version,
                    source_topic = EXCLUDED.source_topic,
                    source_partition = EXCLUDED.source_partition,
                    source_offset = EXCLUDED.source_offset,
                    updated_at = NOW()
                RETURNING (xmax = 0)
                """, Boolean.class,
                event.alertId(), event.eventId(), transactionId, accountKey, dateKey, event.rule().name(),
                event.ruleVersion(), severity(event.rule()), event.amount(), event.currency(), event.observedCount(),
                Timestamp.from(event.windowStart()), Timestamp.from(event.windowEnd()), Timestamp.from(event.detectedAt()),
                event.schemaVersion(), record.topic(), record.partition(), record.offset());
        BigDecimal amount = event.amount() == null ? BigDecimal.ZERO : event.amount();
        return new UpsertResult(event.alertId().toString(), event.currency(), amount, Boolean.TRUE.equals(inserted));
    }

    private UpsertResult upsertAggregate(WarehouseRecord record, TransactionAggregateEvent event) {
        int dateKey = resolveDate(event.windowStart());
        Boolean inserted = jdbc.queryForObject("""
                INSERT INTO agg_transaction_windows (
                    aggregate_id, window_key, date_key, window_start, window_end, currency,
                    transaction_count, total_amount, failed_count, schema_version,
                    source_topic, source_partition, source_offset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (aggregate_id) DO UPDATE SET
                    window_key = EXCLUDED.window_key,
                    date_key = EXCLUDED.date_key,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    currency = EXCLUDED.currency,
                    transaction_count = EXCLUDED.transaction_count,
                    total_amount = EXCLUDED.total_amount,
                    failed_count = EXCLUDED.failed_count,
                    schema_version = EXCLUDED.schema_version,
                    source_topic = EXCLUDED.source_topic,
                    source_partition = EXCLUDED.source_partition,
                    source_offset = EXCLUDED.source_offset,
                    updated_at = NOW()
                RETURNING (xmax = 0)
                """, Boolean.class,
                event.aggregateId(), event.windowKey(), dateKey, Timestamp.from(event.windowStart()),
                Timestamp.from(event.windowEnd()), event.currency(), event.transactionCount(), event.totalAmount(),
                event.failedCount(), event.schemaVersion(), record.topic(), record.partition(), record.offset());
        return new UpsertResult(event.aggregateId().toString(), event.currency(), event.totalAmount(), Boolean.TRUE.equals(inserted));
    }

    private long resolveAccount(String accountId, String currency) {
        List<Long> keys = jdbc.query("""
                INSERT INTO dim_account (account_id, customer_key, currency, is_inferred)
                VALUES (?, (SELECT customer_key FROM dim_customer WHERE customer_id = 'UNKNOWN'), ?, TRUE)
                ON CONFLICT (account_id) DO UPDATE SET
                    updated_at = NOW()
                WHERE dim_account.currency = EXCLUDED.currency
                RETURNING account_key
                """, (rs, row) -> rs.getLong(1), accountId, currency);
        if (keys.isEmpty()) {
            throw new RejectedRecordException(
                    "DIMENSION_CURRENCY_CONFLICT",
                    "account " + accountId + " already exists with a different currency"
            );
        }
        return keys.getFirst();
    }

    private int resolveDate(Instant instant) {
        LocalDate date = instant.atZone(ZoneOffset.UTC).toLocalDate();
        int key = date.getYear() * 10_000 + date.getMonthValue() * 100 + date.getDayOfMonth();
        DayOfWeek day = date.getDayOfWeek();
        return jdbc.queryForObject("""
                INSERT INTO dim_date (
                    date_key, calendar_date, calendar_year, calendar_quarter,
                    calendar_month, day_of_month, day_of_week, is_weekend
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (date_key) DO UPDATE SET calendar_date = EXCLUDED.calendar_date
                RETURNING date_key
                """, Integer.class, key, Date.valueOf(date), date.getYear(), ((date.getMonthValue() - 1) / 3) + 1,
                date.getMonthValue(), date.getDayOfMonth(), day.getValue(),
                day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY);
    }

    private void reject(WarehouseRecord record, String payloadHash, Exception exception) {
        UUID failureId = deterministicUuid(record.topic() + ":" + record.partition() + ":" + record.offset());
        String message = abbreviate(rootMessage(exception), 1024);
        FailureClass failureClass = exception instanceof RejectedRecordException
                ? FailureClass.BUSINESS : FailureClass.SCHEMA;
        String failureCode = exception instanceof RejectedRecordException rejected
                ? rejected.code() : "WAREHOUSE_INVALID_EVENT";
        jdbc.update("""
                INSERT INTO warehouse_rejections (
                    rejection_id, source_topic, source_partition, source_offset, payload_hash,
                    failure_code, failure_message, raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
                """, failureId, record.topic(), record.partition(), record.offset(), payloadHash,
                failureCode, message, record.payload());
        DeadLetterEvent deadLetter = new DeadLetterEvent(
                null, record.topic(), record.partition(), record.offset(), failureClass,
                failureCode, message, Instant.now(), 0, DeadLetterEvent.SCHEMA_VERSION,
                payloadHash, ReplayStatus.PENDING
        );
        try {
            jdbc.update("""
                    INSERT INTO warehouse_dlq_outbox (
                        failure_id, source_topic, source_partition, source_offset, message_key, payload
                    ) VALUES (?, ?, ?, ?, ?, ?::jsonb)
                    ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
                    """, failureId, record.topic(), record.partition(), record.offset(), failureId.toString(),
                    mapper.writeValueAsString(deadLetter));
        } catch (JsonProcessingException serializationFailure) {
            throw new IllegalStateException("Unable to serialize warehouse DLQ record", serializationFailure);
        }
    }

    private void saveReconciliation(
            String consumerName,
            List<WarehouseRecord> records,
            MutableCounts counts,
            Map<String, UpsertResult> uniqueEvents,
            Map<String, BigDecimal> currencyTotals,
            Instant startedAt,
            Instant finishedAt
    ) {
        try {
            Map<String, BigDecimal> warehouseTotals = new LinkedHashMap<>();
            uniqueEvents.values().forEach(event ->
                    warehouseTotals.merge(event.currency(), event.amount(), BigDecimal::add));
            Map<String, BigDecimal> deviation = new LinkedHashMap<>();
            currencyTotals.forEach((currency, sourceTotal) -> deviation.put(
                    currency, sourceTotal.subtract(warehouseTotals.getOrDefault(currency, BigDecimal.ZERO))
            ));
            jdbc.update("""
                    INSERT INTO warehouse_reconciliation_runs (
                        run_id, consumer_name, source_topic, source_offsets, raw_count, valid_count,
                        invalid_count, unique_count, inserted_count, updated_count, rejected_count,
                        duplicate_delivery_count, currency_totals, warehouse_currency_totals,
                        amount_deviation, started_at, finished_at
                    ) VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?)
                    """, UUID.randomUUID(), consumerName, records.getFirst().topic(),
                    mapper.writeValueAsString(offsetRanges(records)), records.size(), counts.valid, counts.invalid,
                    uniqueEvents.size(), counts.inserted, counts.updated, counts.rejected, counts.duplicates,
                    mapper.writeValueAsString(currencyTotals), mapper.writeValueAsString(warehouseTotals),
                    mapper.writeValueAsString(deviation), Timestamp.from(startedAt), Timestamp.from(finishedAt));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize reconciliation details", exception);
        }
    }

    private static Map<String, Map<String, Long>> offsetRanges(List<WarehouseRecord> records) {
        Map<String, long[]> ranges = new LinkedHashMap<>();
        for (WarehouseRecord record : records) {
            long[] range = ranges.computeIfAbsent(Integer.toString(record.partition()), ignored -> new long[]{Long.MAX_VALUE, Long.MIN_VALUE});
            range[0] = Math.min(range[0], record.offset());
            range[1] = Math.max(range[1], record.offset());
        }
        Map<String, Map<String, Long>> result = new LinkedHashMap<>();
        ranges.forEach((partition, range) -> result.put(partition, Map.of("first", range[0], "last", range[1])));
        return result;
    }

    private static Timestamp timestamp(long epochMillis) {
        return epochMillis < 0 ? null : Timestamp.from(Instant.ofEpochMilli(epochMillis));
    }

    private static String severity(FraudRule rule) {
        return rule == FraudRule.HIGH_VALUE ? "HIGH" : "MEDIUM";
    }

    private static UUID deterministicUuid(String value) {
        return UUID.nameUUIDFromBytes((REJECTION_NAMESPACE + ":" + value).getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] payload) {
        try {
            byte[] value = payload == null ? new byte[0] : payload;
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static String rootMessage(Exception exception) {
        Throwable current = exception;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    private static String abbreviate(String value, int limit) {
        if (value == null) {
            return "unknown failure";
        }
        return value.length() <= limit ? value : value.substring(0, limit);
    }

    public record BatchResult(int valid, int invalid, int inserted, int updated, int duplicates) {
    }

    private record UpsertResult(String identifier, String currency, BigDecimal amount, boolean inserted) {
    }

    private static final class MutableCounts {
        private int valid;
        private int invalid;
        private int inserted;
        private int updated;
        private int rejected;
        private int duplicates;
    }

    private static final class RejectedRecordException extends RuntimeException {
        private final String code;

        private RejectedRecordException(String code, String message) {
            super(message);
            this.code = code;
        }

        private String code() {
            return code;
        }
    }
}
