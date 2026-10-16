package io.github.hquan07.banking.transaction.support;

public class AccountStateConflictException extends RuntimeException {

    public AccountStateConflictException(String message) {
        super(message);
    }
}
