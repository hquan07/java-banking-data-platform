package io.github.hquan07.banking.analytics.api;

import java.util.List;

public record ApiError(
        String code,
        String message,
        String correlationId,
        List<FieldViolation> details
) {
    public ApiError {
        details = details == null ? List.of() : List.copyOf(details);
    }
}
