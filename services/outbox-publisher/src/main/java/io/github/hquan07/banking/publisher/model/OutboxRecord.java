package io.github.hquan07.banking.publisher.model;

import java.time.Instant;
import java.util.UUID;

public record OutboxRecord(
        UUID eventId,
        UUID aggregateId,
        String eventType,
        int schemaVersion,
        String payload,
        Instant createdAt,
        int attempts,
        UUID claimToken,
        Instant lockedUntil
) {
}
