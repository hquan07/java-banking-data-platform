package io.github.hquan07.banking.publisher.persistence;

import io.github.hquan07.banking.publisher.model.OutboxRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public List<OutboxRecord> claimBatch(
            UUID claimToken,
            Instant now,
            Duration leaseDuration,
            int batchSize
    ) {
        Instant lockedUntil = now.plus(leaseDuration);
        return jdbcTemplate.query("""
                        WITH candidates AS (
                            SELECT event_id
                            FROM outbox_events
                            WHERE published_at IS NULL
                              AND next_attempt_at <= ?
                              AND (claim_token IS NULL OR locked_until < ?)
                            ORDER BY created_at, event_id
                            FOR UPDATE SKIP LOCKED
                            LIMIT ?
                        )
                        UPDATE outbox_events AS outbox
                        SET claim_token = ?,
                            locked_until = ?,
                            attempts = outbox.attempts + 1
                        FROM candidates
                        WHERE outbox.event_id = candidates.event_id
                        RETURNING outbox.event_id, outbox.aggregate_id, outbox.event_type,
                                  outbox.schema_version, outbox.payload::text AS payload,
                                  outbox.created_at, outbox.attempts, outbox.claim_token,
                                  outbox.locked_until
                        """,
                (resultSet, rowNumber) -> new OutboxRecord(
                        resultSet.getObject("event_id", UUID.class),
                        resultSet.getObject("aggregate_id", UUID.class),
                        resultSet.getString("event_type"),
                        resultSet.getInt("schema_version"),
                        resultSet.getString("payload"),
                        resultSet.getTimestamp("created_at").toInstant(),
                        resultSet.getInt("attempts"),
                        resultSet.getObject("claim_token", UUID.class),
                        resultSet.getTimestamp("locked_until").toInstant()
                ),
                Timestamp.from(now),
                Timestamp.from(now),
                batchSize,
                claimToken,
                Timestamp.from(lockedUntil)
        );
    }

    public boolean markPublished(UUID eventId, UUID claimToken, Instant publishedAt) {
        return jdbcTemplate.update("""
                        UPDATE outbox_events
                        SET published_at = ?,
                            claim_token = NULL,
                            locked_until = NULL,
                            last_error = NULL
                        WHERE event_id = ?
                          AND claim_token = ?
                          AND published_at IS NULL
                        """,
                Timestamp.from(publishedAt),
                eventId,
                claimToken
        ) == 1;
    }

    public boolean markFailed(
            UUID eventId,
            UUID claimToken,
            Instant nextAttemptAt,
            String errorMessage
    ) {
        return jdbcTemplate.update("""
                        UPDATE outbox_events
                        SET next_attempt_at = ?,
                            last_error = ?,
                            claim_token = NULL,
                            locked_until = NULL
                        WHERE event_id = ?
                          AND claim_token = ?
                          AND published_at IS NULL
                        """,
                Timestamp.from(nextAttemptAt),
                errorMessage,
                eventId,
                claimToken
        ) == 1;
    }

    public long countUnpublished() {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL",
                Long.class
        );
        return count == null ? 0 : count;
    }
}
