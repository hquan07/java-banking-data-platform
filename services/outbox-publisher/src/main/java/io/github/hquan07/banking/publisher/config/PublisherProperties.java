package io.github.hquan07.banking.publisher.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("publisher")
public record PublisherProperties(
        boolean enabled,
        @NotBlank String topic,
        @NotNull Duration pollInterval,
        @Min(1) @Max(10_000) int batchSize,
        @NotNull Duration leaseDuration,
        @NotNull Duration sendTimeout,
        @NotNull Duration initialBackoff,
        @NotNull Duration maxBackoff,
        @DecimalMin("0.0") @DecimalMax("1.0") double jitterFactor
) {

    public PublisherProperties {
        requirePositive(pollInterval, "publisher.poll-interval");
        requirePositive(leaseDuration, "publisher.lease-duration");
        requirePositive(sendTimeout, "publisher.send-timeout");
        requirePositive(initialBackoff, "publisher.initial-backoff");
        requirePositive(maxBackoff, "publisher.max-backoff");
        if (initialBackoff != null && maxBackoff != null && initialBackoff.compareTo(maxBackoff) > 0) {
            throw new IllegalArgumentException("publisher.initial-backoff must not exceed max-backoff");
        }
        if (leaseDuration != null && sendTimeout != null && leaseDuration.compareTo(sendTimeout) <= 0) {
            throw new IllegalArgumentException("publisher.lease-duration must exceed send-timeout");
        }
    }

    private static void requirePositive(Duration value, String property) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(property + " must be positive");
        }
    }
}
