package io.github.hquan07.banking.streaming.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.common.event.FailureClass;
import io.github.hquan07.banking.common.event.ReplayStatus;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.streaming.model.RawKafkaRecord;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import io.github.hquan07.banking.streaming.util.PayloadHashes;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public final class TransactionValidator implements Serializable {

    private transient ObjectMapper objectMapper;

    public ValidationOutcome validate(RawKafkaRecord record) {
        String payloadHash = PayloadHashes.sha256(record.payload());
        JsonNode root;
        try {
            root = mapper().readTree(record.payload());
        } catch (IOException exception) {
            return ValidationOutcome.invalid(failure(
                    record, null, FailureClass.SCHEMA, "MALFORMED_JSON",
                    "payload is not valid JSON", payloadHash
            ));
        } catch (RuntimeException exception) {
            return ValidationOutcome.dlq(failure(
                    record, null, FailureClass.INFRASTRUCTURE, "VALIDATOR_FAILURE",
                    safeMessage(exception), payloadHash
            ));
        }

        UUID eventId = extractEventId(root);
        if (root == null || !root.isObject()) {
            return ValidationOutcome.invalid(failure(
                    record, eventId, FailureClass.SCHEMA, "INVALID_JSON_ROOT",
                    "payload root must be a JSON object", payloadHash
            ));
        }
        if (root.path("schema_version").asInt(Integer.MIN_VALUE) != TransactionCreatedEvent.SCHEMA_VERSION) {
            return ValidationOutcome.invalid(failure(
                    record, eventId, FailureClass.SCHEMA, "UNSUPPORTED_SCHEMA_VERSION",
                    "schema_version must be 1", payloadHash
            ));
        }

        try {
            TransactionCreatedEvent event = mapper().treeToValue(root, TransactionCreatedEvent.class);
            if (record.key() == null || !record.key().equals(event.accountId())) {
                return ValidationOutcome.invalid(failure(
                        record, event.eventId(), FailureClass.BUSINESS, "KAFKA_KEY_MISMATCH",
                        "Kafka key must match account_id", payloadHash
                ));
            }
            return ValidationOutcome.validated(new ValidatedTransaction(
                    event,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    payloadHash
            ));
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            return ValidationOutcome.invalid(failure(
                    record, eventId, FailureClass.SCHEMA, "CONTRACT_VIOLATION",
                    safeMessage(exception), payloadHash
            ));
        } catch (RuntimeException exception) {
            return ValidationOutcome.dlq(failure(
                    record, eventId, FailureClass.INFRASTRUCTURE, "VALIDATOR_FAILURE",
                    safeMessage(exception), payloadHash
            ));
        }
    }

    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }

    private UUID extractEventId(JsonNode root) {
        if (root == null || !root.isObject()) {
            return null;
        }
        try {
            return UUID.fromString(root.path("event_id").asText());
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private DeadLetterEvent failure(
            RawKafkaRecord record,
            UUID eventId,
            FailureClass failureClass,
            String code,
            String message,
            String payloadHash
    ) {
        Instant failedAt = record.timestamp() >= 0
                ? Instant.ofEpochMilli(record.timestamp())
                : Instant.EPOCH;
        return new DeadLetterEvent(
                eventId,
                record.topic(),
                record.partition(),
                record.offset(),
                failureClass,
                code,
                message,
                failedAt,
                0,
                DeadLetterEvent.SCHEMA_VERSION,
                payloadHash,
                ReplayStatus.PENDING
        );
    }

    private String safeMessage(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        String sanitized = root.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message)
                .replace('\n', ' ')
                .replace('\r', ' ');
        return sanitized.substring(0, Math.min(sanitized.length(), 1024));
    }
}
