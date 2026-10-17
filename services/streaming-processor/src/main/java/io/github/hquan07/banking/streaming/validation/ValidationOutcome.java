package io.github.hquan07.banking.streaming.validation;

import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;

public record ValidationOutcome(
        ValidationRoute route,
        ValidatedTransaction transaction,
        DeadLetterEvent failure
) {

    public static ValidationOutcome validated(ValidatedTransaction transaction) {
        return new ValidationOutcome(ValidationRoute.VALIDATED, transaction, null);
    }

    public static ValidationOutcome invalid(DeadLetterEvent failure) {
        return new ValidationOutcome(ValidationRoute.INVALID, null, failure);
    }

    public static ValidationOutcome dlq(DeadLetterEvent failure) {
        return new ValidationOutcome(ValidationRoute.DLQ, null, failure);
    }
}
