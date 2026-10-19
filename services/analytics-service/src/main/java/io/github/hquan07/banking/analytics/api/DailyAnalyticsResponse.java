package io.github.hquan07.banking.analytics.api;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public record DailyAnalyticsResponse(
        LocalDate date,
        String businessTimezone,
        UUID batchRunId,
        long transactionCount,
        String totalAmount,
        long failedCount,
        Instant updatedAt,
        List<CurrencyMetric> currencies
) {
    public DailyAnalyticsResponse {
        currencies = List.copyOf(currencies);
    }

    public record CurrencyMetric(
            String currency,
            long transactionCount,
            String totalAmount,
            long failedCount
    ) {
    }
}
