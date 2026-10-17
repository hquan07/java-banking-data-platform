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
public record FraudAlertEvent(
        UUID alertId,
        FraudRule rule,
        String ruleVersion,
        UUID eventId,
        String accountId,
        @JsonSerialize(using = ToStringSerializer.class) BigDecimal amount,
        String currency,
        long observedCount,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowStart,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant windowEnd,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant detectedAt,
        int schemaVersion
) {

    public static final int SCHEMA_VERSION = 1;
    public static final String RULE_VERSION = "demo-v1";

    public FraudAlertEvent {
        Objects.requireNonNull(alertId, "alertId is required");
        Objects.requireNonNull(rule, "rule is required");
        ruleVersion = ContractChecks.requiredText(ruleVersion, "ruleVersion", 32);
        accountId = ContractChecks.identifier(accountId, "accountId");
        if (amount != null) {
            amount = ContractChecks.monetaryAmount(amount);
        }
        currency = ContractChecks.currency(currency);
        if (observedCount <= 0) {
            throw new IllegalArgumentException("observedCount must be positive");
        }
        Objects.requireNonNull(windowStart, "windowStart is required");
        Objects.requireNonNull(windowEnd, "windowEnd is required");
        if (!windowEnd.isAfter(windowStart)) {
            throw new IllegalArgumentException("windowEnd must be after windowStart");
        }
        Objects.requireNonNull(detectedAt, "detectedAt is required");
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("schemaVersion must be " + SCHEMA_VERSION);
        }
    }
}
