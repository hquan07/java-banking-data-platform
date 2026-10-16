package io.github.hquan07.banking.transaction.support;

public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException() {
        super("The idempotency key was already used for a different request");
    }
}
