package io.github.hquan07.banking.analytics.api;

import java.time.Instant;

public record AccountSummaryResponse(
        String accountId,
        String customerId,
        String currency,
        String accountStatus,
        boolean inferred,
        Instant firstSeenAt,
        long transactionCount,
        String totalAmount,
        long successCount,
        long failedCount,
        long pendingCount,
        long reversedCount,
        Instant firstTransactionAt,
        Instant lastTransactionAt,
        long fraudAlertCount,
        Instant latestFraudDetectedAt
) {
}
