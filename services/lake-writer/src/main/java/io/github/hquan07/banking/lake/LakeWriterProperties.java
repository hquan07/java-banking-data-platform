package io.github.hquan07.banking.lake;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "lake")
public record LakeWriterProperties(
        boolean enabled,
        @NotBlank String endpoint,
        @NotBlank String accessKey,
        @NotBlank String secretKey,
        @NotBlank String bucket,
        @NotBlank String kafkaBootstrapServers,
        @NotBlank String rawTopic,
        @NotBlank String validatedTopic,
        @NotBlank String rawGroupId,
        @NotBlank String curatedGroupId,
        @Min(2) int maxBatchRecords,
        Duration flushInterval,
        Duration pollTimeout,
        Duration retryBackoff,
        String startingOffsets
) {
    public LakeWriterProperties {
        flushInterval = flushInterval == null ? Duration.ofSeconds(5) : flushInterval;
        pollTimeout = pollTimeout == null ? Duration.ofMillis(500) : pollTimeout;
        retryBackoff = retryBackoff == null ? Duration.ofSeconds(2) : retryBackoff;
        startingOffsets = startingOffsets == null ? "earliest" : startingOffsets.toLowerCase();
    }

    @AssertTrue(message = "lake durations must be positive and startingOffsets must be earliest or latest")
    public boolean isValid() {
        return flushInterval != null && !flushInterval.isNegative() && !flushInterval.isZero()
                && pollTimeout != null && !pollTimeout.isNegative() && !pollTimeout.isZero()
                && retryBackoff != null && !retryBackoff.isNegative() && !retryBackoff.isZero()
                && ("earliest".equals(startingOffsets) || "latest".equals(startingOffsets));
    }
}
