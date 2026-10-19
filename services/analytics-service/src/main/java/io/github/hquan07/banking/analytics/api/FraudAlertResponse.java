package io.github.hquan07.banking.analytics.api;

import java.time.Instant;
import java.util.UUID;

public record FraudAlertResponse(
        UUID alertId,
        UUID eventId,
        UUID transactionId,
        String accountId,
        String ruleCode,
        String ruleVersion,
        String severity,
        String amount,
        String currency,
        long observedCount,
        Instant windowStart,
        Instant windowEnd,
        Instant detectedAt
) {
}
