package io.github.hquan07.banking.batch.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

public record BatchRun(
        UUID batchRunId,
        String jobName,
        LocalDate businessDate,
        String businessTimezone,
        String status,
        long sourceRowCount,
        int outputRowCount,
        String sourceFingerprint,
        PublishedObject goldObject,
        Instant startedAt,
        Instant finishedAt,
        Instant publishedAt
) {
}
