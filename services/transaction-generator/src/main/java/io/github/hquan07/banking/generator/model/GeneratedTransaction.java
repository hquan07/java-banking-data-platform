package io.github.hquan07.banking.generator.model;

public record GeneratedTransaction(
        long sequence,
        String idempotencyKey,
        TransactionPayload payload,
        GeneratorScenario scenario
) {
}
