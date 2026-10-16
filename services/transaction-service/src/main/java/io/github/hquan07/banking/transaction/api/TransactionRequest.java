package io.github.hquan07.banking.transaction.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.github.hquan07.banking.common.event.TransactionType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;

public record TransactionRequest(
        @NotBlank
        @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
        String accountId,

        @NotNull
        TransactionType transactionType,

        @NotBlank
        @JsonDeserialize(using = StrictStringDeserializer.class)
        @Pattern(
                regexp = "(?:0\\.(?:0[1-9]|[1-9][0-9])|[1-9][0-9]{0,16}\\.[0-9]{2})",
                message = "must be a positive decimal string with exactly two fractional digits"
        )
        String amount,

        @NotBlank
        @Pattern(regexp = "[A-Z]{3}", message = "must be a three-letter uppercase currency code")
        String currency
) {

    public BigDecimal monetaryAmount() {
        return new BigDecimal(amount);
    }
}
