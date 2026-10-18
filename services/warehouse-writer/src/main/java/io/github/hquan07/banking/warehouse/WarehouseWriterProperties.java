package io.github.hquan07.banking.warehouse;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "warehouse")
public record WarehouseWriterProperties(
        boolean enabled,
        @NotBlank String kafkaBootstrapServers,
        @NotBlank String validatedTopic,
        @NotBlank String fraudTopic,
        @NotBlank String aggregatesTopic,
        @NotBlank String dlqTopic,
        @NotBlank String validatedGroupId,
        @NotBlank String fraudGroupId,
        @NotBlank String aggregatesGroupId,
        @Min(1) @Max(5000) int maxPollRecords,
        Duration pollTimeout,
        Duration retryBackoff,
        Duration dlqPublishInterval,
        String startingOffsets
) {
    public WarehouseWriterProperties {
        pollTimeout = pollTimeout == null ? Duration.ofMillis(500) : pollTimeout;
        retryBackoff = retryBackoff == null ? Duration.ofSeconds(2) : retryBackoff;
        dlqPublishInterval = dlqPublishInterval == null ? Duration.ofSeconds(1) : dlqPublishInterval;
        startingOffsets = startingOffsets == null ? "earliest" : startingOffsets.toLowerCase();
    }

    @AssertTrue(message = "warehouse durations must be positive and startingOffsets must be earliest or latest")
    public boolean isValid() {
        return positive(pollTimeout) && positive(retryBackoff) && positive(dlqPublishInterval)
                && ("earliest".equals(startingOffsets) || "latest".equals(startingOffsets));
    }

    private static boolean positive(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }
}
