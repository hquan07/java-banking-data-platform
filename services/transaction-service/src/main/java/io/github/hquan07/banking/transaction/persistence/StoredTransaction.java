package io.github.hquan07.banking.transaction.persistence;

import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record StoredTransaction(
        UUID transactionId,
        String accountId,
        TransactionType transactionType,
        BigDecimal amount,
        String currency,
        TransactionStatus status,
        Instant occurredAt,
        Instant createdAt,
        String idempotencyKey,
        String requestFingerprint,
        String correlationId
) {
}
