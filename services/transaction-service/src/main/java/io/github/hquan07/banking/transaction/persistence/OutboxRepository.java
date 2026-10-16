package io.github.hquan07.banking.transaction.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public OutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(
            UUID eventId,
            UUID transactionId,
            String eventType,
            int schemaVersion,
            String payload,
            Instant createdAt
    ) {
        jdbcTemplate.update("""
                        INSERT INTO outbox_events (
                            event_id, aggregate_id, event_type, schema_version, payload, created_at
                        ) VALUES (?, ?, ?, ?, ?::jsonb, ?)
                        """,
                eventId,
                transactionId,
                eventType,
                schemaVersion,
                payload,
                Timestamp.from(createdAt)
        );
    }
}
