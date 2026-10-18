package io.github.hquan07.banking.batch.model;

import java.time.LocalDate;
import java.util.UUID;

public record BatchOutcome(
        UUID batchRunId,
        LocalDate businessDate,
        long sourceRowCount,
        int outputRowCount,
        String sourceFingerprint,
        String goldObjectKey,
        String goldObjectSha256
) {
}
