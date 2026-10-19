package io.github.hquan07.banking.analytics.application;

import io.github.hquan07.banking.analytics.api.AccountSummaryResponse;
import io.github.hquan07.banking.analytics.api.DailyAnalyticsResponse;
import io.github.hquan07.banking.analytics.api.FraudAlertResponse;
import io.github.hquan07.banking.analytics.api.PageResponse;
import io.github.hquan07.banking.analytics.persistence.AnalyticsRepository;
import io.github.hquan07.banking.analytics.support.AnalyticsNotFoundException;
import io.github.hquan07.banking.analytics.support.InvalidAnalyticsRequestException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;

@Service
@Transactional(readOnly = true, timeout = 3)
public class AnalyticsQueryService {

    static final Duration MAX_FRAUD_RANGE = Duration.ofDays(31);

    private final AnalyticsRepository repository;

    public AnalyticsQueryService(AnalyticsRepository repository) {
        this.repository = repository;
    }

    public DailyAnalyticsResponse daily(LocalDate date) {
        List<AnalyticsRepository.DailyRow> rows = repository.daily(date);
        if (rows.isEmpty()) {
            throw new AnalyticsNotFoundException("No finalized daily analytics exists for " + date);
        }
        AnalyticsRepository.DailyRow first = rows.getFirst();
        if (rows.stream().anyMatch(row -> !row.batchRunId().equals(first.batchRunId()))) {
            throw new IllegalStateException("Daily analytics contains mixed batch run identifiers for " + date);
        }
        long count = rows.stream().mapToLong(AnalyticsRepository.DailyRow::transactionCount).sum();
        long failed = rows.stream().mapToLong(AnalyticsRepository.DailyRow::failedCount).sum();
        BigDecimal amount = rows.stream().map(AnalyticsRepository.DailyRow::totalAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        Instant updatedAt = rows.stream().map(AnalyticsRepository.DailyRow::updatedAt)
                .max(Comparator.naturalOrder()).orElse(first.updatedAt());
        List<DailyAnalyticsResponse.CurrencyMetric> currencies = rows.stream()
                .map(row -> new DailyAnalyticsResponse.CurrencyMetric(
                        row.currency(), row.transactionCount(), money(row.totalAmount()), row.failedCount()
                ))
                .toList();
        return new DailyAnalyticsResponse(
                date, first.businessTimezone(), first.batchRunId(), count, money(amount), failed, updatedAt, currencies
        );
    }

    public PageResponse<FraudAlertResponse> fraud(Instant from, Instant to, int page, int size) {
        validateRange(from, to);
        long offset = Math.multiplyExact((long) page, size);
        long total = repository.fraudCount(from, to);
        List<FraudAlertResponse> items = repository.fraud(from, to, size, offset).stream()
                .map(row -> new FraudAlertResponse(
                        row.alertId(), row.eventId(), row.transactionId(), row.accountId(), row.ruleCode(),
                        row.ruleVersion(), row.severity(), nullableMoney(row.amount()), row.currency(),
                        row.observedCount(), row.windowStart(), row.windowEnd(), row.detectedAt()
                ))
                .toList();
        int pages = total == 0 ? 0 : Math.toIntExact((total + size - 1) / size);
        return new PageResponse<>(items, page, size, total, pages);
    }

    public AccountSummaryResponse accountSummary(String accountId) {
        AnalyticsRepository.AccountSummaryRow row = repository.accountSummary(accountId)
                .orElseThrow(() -> new AnalyticsNotFoundException("Account does not exist: " + accountId));
        return new AccountSummaryResponse(
                row.accountId(), row.customerId(), row.currency(), row.accountStatus(), row.inferred(),
                row.firstSeenAt(), row.transactionCount(), money(row.totalAmount()), row.successCount(),
                row.failedCount(), row.pendingCount(), row.reversedCount(), row.firstTransactionAt(),
                row.lastTransactionAt(), row.fraudAlertCount(), row.latestFraudDetectedAt()
        );
    }

    private void validateRange(Instant from, Instant to) {
        if (!from.isBefore(to)) {
            throw new InvalidAnalyticsRequestException("from must be earlier than to");
        }
        if (Duration.between(from, to).compareTo(MAX_FRAUD_RANGE) > 0) {
            throw new InvalidAnalyticsRequestException("Fraud analytics range cannot exceed 31 days");
        }
    }

    private String nullableMoney(BigDecimal value) {
        return value == null ? null : money(value);
    }

    private String money(BigDecimal value) {
        return value.setScale(2).toPlainString();
    }
}
