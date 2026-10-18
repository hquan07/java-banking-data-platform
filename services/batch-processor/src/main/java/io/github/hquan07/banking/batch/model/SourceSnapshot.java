package io.github.hquan07.banking.batch.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public record SourceSnapshot(
        LocalDate businessDate,
        Instant intervalStart,
        Instant intervalEnd,
        long sourceRowCount,
        BigDecimal sourceTotalAmount,
        Map<String, BigDecimal> currencyTotals,
        String fingerprint,
        List<DailyAggregate> rows
) {
}
