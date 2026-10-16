package io.github.hquan07.banking.transaction.api;

import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;

import java.time.Instant;
import java.util.UUID;

public record TransactionResponse(
        UUID transactionId,
        String accountId,
        TransactionType transactionType,
        String amount,
        String currency,
        TransactionStatus status,
        Instant occurredAt,
        Instant createdAt,
        String correlationId,
        boolean idempotentReplay
) {
}
