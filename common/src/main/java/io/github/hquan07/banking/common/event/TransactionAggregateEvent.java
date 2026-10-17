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
public record TransactionAggregateEvent(
        UUID aggregateId,
        String windowKey,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowStart,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowEnd,
        String currency,
        long transactionCount,
        @JsonSerialize(using = ToStringSerializer.class) BigDecimal totalAmount,
        long failedCount,
        int schemaVersion
) {

    public static final int SCHEMA_VERSION = 1;

    public TransactionAggregateEvent {
        Objects.requireNonNull(aggregateId, "aggregateId is required");
        windowKey = ContractChecks.requiredText(windowKey, "windowKey", 128);
        Objects.requireNonNull(windowStart, "windowStart is required");
        Objects.requireNonNull(windowEnd, "windowEnd is required");
        if (!windowEnd.isAfter(windowStart)) {
            throw new IllegalArgumentException("windowEnd must be after windowStart");
        }
        currency = ContractChecks.currency(currency);
        if (transactionCount <= 0) {
            throw new IllegalArgumentException("transactionCount must be positive");
        }
        totalAmount = ContractChecks.monetaryAmount(totalAmount);
        if (failedCount < 0 || failedCount > transactionCount) {
            throw new IllegalArgumentException("failedCount must be between zero and transactionCount");
        }
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("schemaVersion must be " + SCHEMA_VERSION);
        }
    }
}
