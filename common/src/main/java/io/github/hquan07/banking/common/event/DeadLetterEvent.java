package io.github.hquan07.banking.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record DeadLetterEvent(
        UUID eventId,
        String originalTopic,
        int originalPartition,
        long originalOffset,
        FailureClass failureClass,
        String failureCode,
        String failureMessage,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant firstFailedAt,
        int retryCount,
        int schemaVersion,
        String payloadHash,
        ReplayStatus replayStatus
) {

    public static final int SCHEMA_VERSION = 1;

    public DeadLetterEvent {
        originalTopic = ContractChecks.requiredText(originalTopic, "originalTopic", 249);
        if (originalPartition < 0) {
            throw new IllegalArgumentException("originalPartition must be non-negative");
        }
        if (originalOffset < 0) {
            throw new IllegalArgumentException("originalOffset must be non-negative");
        }
        Objects.requireNonNull(failureClass, "failureClass is required");
        failureCode = ContractChecks.requiredText(failureCode, "failureCode", 100);
        failureMessage = ContractChecks.requiredText(failureMessage, "failureMessage", 1024);
        Objects.requireNonNull(firstFailedAt, "firstFailedAt is required");
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must be non-negative");
        }
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("schemaVersion must be " + SCHEMA_VERSION);
        }
        payloadHash = ContractChecks.sha256(payloadHash);
        Objects.requireNonNull(replayStatus, "replayStatus is required");
    }
}
