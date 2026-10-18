package io.github.hquan07.banking.batch.model;

import java.math.BigDecimal;

public record DailyTarget(
        String currency,
        long transactionCount,
        BigDecimal totalAmount,
        long failedCount
) {
}
