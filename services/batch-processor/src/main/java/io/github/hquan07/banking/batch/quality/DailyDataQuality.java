package io.github.hquan07.banking.batch.quality;

import io.github.hquan07.banking.batch.model.DailyAggregate;
import io.github.hquan07.banking.batch.model.DailyTarget;
import io.github.hquan07.banking.batch.model.SourceSnapshot;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class DailyDataQuality {

    public Map<String, Object> validateSource(SourceSnapshot source) {
        Map<String, Object> checks = new LinkedHashMap<>();
        checks.put("source_not_empty", source.sourceRowCount() > 0);
        checks.put("currency_unique", source.rows().stream().map(DailyAggregate::currency).distinct().count()
                == source.rows().size());
        checks.put("currency_not_null", source.rows().stream().allMatch(row -> row.currency() != null));
        checks.put("amount_nonnegative", source.rows().stream()
                .allMatch(row -> row.totalAmount() != null && row.totalAmount().signum() >= 0));
        checks.put("counts_nonnegative", source.rows().stream().allMatch(this::countsNonnegative));
        checks.put("row_count_reconciled", source.rows().stream()
                .mapToLong(DailyAggregate::transactionCount).sum() == source.sourceRowCount());
        checks.put("amount_reconciled", normalized(source.rows().stream()
                .map(DailyAggregate::totalAmount).reduce(BigDecimal.ZERO, BigDecimal::add))
                .compareTo(normalized(source.sourceTotalAmount())) == 0);
        checks.put("status_counts_reconciled", source.rows().stream().allMatch(row ->
                row.successCount() + row.failedCount() + row.pendingCount() + row.reversedCount()
                        == row.transactionCount()));
        requireAll(checks);
        return Map.copyOf(checks);
    }

    public Map<String, Object> validateTarget(SourceSnapshot source, List<DailyTarget> target) {
        Map<String, Object> checks = new LinkedHashMap<>(validateSource(source));
        Map<String, DailyAggregate> sourceByCurrency = source.rows().stream()
                .collect(Collectors.toMap(DailyAggregate::currency, row -> row));
        Map<String, DailyTarget> targetByCurrency = target.stream()
                .collect(Collectors.toMap(DailyTarget::currency, row -> row));
        checks.put("target_currency_grain_matches", sourceByCurrency.keySet().equals(targetByCurrency.keySet()));
        checks.put("target_values_match", sourceByCurrency.entrySet().stream().allMatch(entry -> {
            DailyTarget row = targetByCurrency.get(entry.getKey());
            DailyAggregate expected = entry.getValue();
            return row != null
                    && row.transactionCount() == expected.transactionCount()
                    && row.failedCount() == expected.failedCount()
                    && normalized(row.totalAmount()).compareTo(normalized(expected.totalAmount())) == 0;
        }));
        requireAll(checks);
        return Map.copyOf(checks);
    }

    private boolean countsNonnegative(DailyAggregate row) {
        return row.transactionCount() >= 0 && row.successCount() >= 0 && row.failedCount() >= 0
                && row.pendingCount() >= 0 && row.reversedCount() >= 0;
    }

    private BigDecimal normalized(BigDecimal value) {
        return value == null ? BigDecimal.ZERO.setScale(2) : value.setScale(2);
    }

    private void requireAll(Map<String, Object> checks) {
        List<String> failures = checks.entrySet().stream()
                .filter(entry -> !Boolean.TRUE.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .toList();
        if (!failures.isEmpty()) {
            throw new IllegalStateException("Daily batch data-quality checks failed: " + String.join(",", failures));
        }
    }
}
