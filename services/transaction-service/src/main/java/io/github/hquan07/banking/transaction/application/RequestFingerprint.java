package io.github.hquan07.banking.transaction.application;

import io.github.hquan07.banking.transaction.api.TransactionRequest;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Component
public class RequestFingerprint {

    public String calculate(TransactionRequest request) {
        String canonicalRequest = String.join("\n",
                request.accountId(),
                request.transactionType().name(),
                request.monetaryAmount().toPlainString(),
                request.currency()
        );
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonicalRequest.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
