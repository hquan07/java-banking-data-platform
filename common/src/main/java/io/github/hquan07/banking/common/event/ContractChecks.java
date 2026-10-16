package io.github.hquan07.banking.common.event;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.regex.Pattern;

final class ContractChecks {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,63}");
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final Pattern SHA_256 = Pattern.compile("[a-f0-9]{64}");

    private ContractChecks() {
    }

    static String identifier(String value, String field) {
        Objects.requireNonNull(value, field + " is required");
        if (!IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must match " + IDENTIFIER.pattern());
        }
        return value;
    }

    static String requiredText(String value, String field, int maxLength) {
        Objects.requireNonNull(value, field + " is required");
        if (value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(field + " must contain 1 to " + maxLength + " characters");
        }
        return value;
    }

    static String currency(String value) {
        Objects.requireNonNull(value, "currency is required");
        if (!CURRENCY.matcher(value).matches()) {
            throw new IllegalArgumentException("currency must be an uppercase ISO-4217 code");
        }
        return value;
    }

    static BigDecimal monetaryAmount(BigDecimal value) {
        Objects.requireNonNull(value, "amount is required");
        if (value.signum() <= 0) {
            throw new IllegalArgumentException("amount must be greater than zero");
        }
        BigDecimal normalized = value.setScale(2, RoundingMode.UNNECESSARY);
        if (normalized.precision() > 19) {
            throw new IllegalArgumentException("amount must fit NUMERIC(19,2)");
        }
        return normalized;
    }

    static String sha256(String value) {
        Objects.requireNonNull(value, "payloadHash is required");
        if (!SHA_256.matcher(value).matches()) {
            throw new IllegalArgumentException("payloadHash must be a lowercase SHA-256 hex string");
        }
        return value;
    }
}
