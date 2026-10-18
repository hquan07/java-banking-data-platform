package io.github.hquan07.banking.lake.model;

import java.math.BigDecimal;
import java.time.LocalDate;

public record DailyAggregatePartial(
        LocalDate reportDate,
        String accountId,
        String currency,
        long transactionCount,
        BigDecimal totalAmount,
        long successCount,
        long failedCount,
        long pendingCount,
        long reversedCount,
        String batchId
) {
}
