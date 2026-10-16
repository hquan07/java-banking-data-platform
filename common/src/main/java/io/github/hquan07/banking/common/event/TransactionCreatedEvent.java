package io.github.hquan07.banking.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record TransactionCreatedEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        UUID transactionId,
        String accountId,
        TransactionType transactionType,
        @JsonSerialize(using = ToStringSerializer.class) BigDecimal amount,
        String currency,
        TransactionStatus status,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant occurredAt,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant producedAt,
        String correlationId
) {

    public static final String EVENT_TYPE = "transaction.created";
    public static final int SCHEMA_VERSION = 1;

    public TransactionCreatedEvent {
        Objects.requireNonNull(eventId, "eventId is required");
        if (!EVENT_TYPE.equals(eventType)) {
            throw new IllegalArgumentException("eventType must be " + EVENT_TYPE);
        }
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("schemaVersion must be " + SCHEMA_VERSION);
        }
        Objects.requireNonNull(transactionId, "transactionId is required");
        accountId = ContractChecks.identifier(accountId, "accountId");
        Objects.requireNonNull(transactionType, "transactionType is required");
        amount = ContractChecks.monetaryAmount(amount);
        currency = ContractChecks.currency(currency);
        Objects.requireNonNull(status, "status is required");
        Objects.requireNonNull(occurredAt, "occurredAt is required");
        Objects.requireNonNull(producedAt, "producedAt is required");
        if (correlationId != null) {
            correlationId = ContractChecks.requiredText(correlationId, "correlationId", 128);
        }
    }
}
