package io.github.hquan07.banking.batch.model;

import java.math.BigDecimal;
import java.time.LocalDate;

public record DailyAggregate(
        LocalDate businessDate,
        String currency,
        long transactionCount,
        BigDecimal totalAmount,
        long successCount,
        long failedCount,
        long pendingCount,
        long reversedCount
) {
}
