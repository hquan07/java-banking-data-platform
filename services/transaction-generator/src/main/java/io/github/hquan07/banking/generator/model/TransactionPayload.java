package io.github.hquan07.banking.generator.model;

public record TransactionPayload(
        String accountId,
        String transactionType,
        String amount,
        String currency
) {
}
