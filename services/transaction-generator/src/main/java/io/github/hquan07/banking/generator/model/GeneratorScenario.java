package io.github.hquan07.banking.generator.model;

public enum GeneratorScenario {
    NORMAL,
    LARGE_AMOUNT,
    IDEMPOTENT_RETRY,
    INVALID_AMOUNT,
    CURRENCY_MISMATCH
}
