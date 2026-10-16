package io.github.hquan07.banking.transaction.application;

import io.github.hquan07.banking.transaction.persistence.StoredTransaction;

public record CreateTransactionResult(StoredTransaction transaction, boolean idempotentReplay) {
}
