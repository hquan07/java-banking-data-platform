package io.github.hquan07.banking.generator.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties("generator")
public record GeneratorProperties(
        boolean enabled,
        @NotNull URI apiBaseUrl,
        @Min(1) @Max(10_000) int transactionsPerSecond,
        @NotNull Duration duration,
        long seed,
        @NotEmpty List<@Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9_-]{0,63}") String> accountIds,
        @Pattern(regexp = "[A-Z]{3}") String currency,
        @Min(1) @Max(100_000) int maxInFlight,
        @NotNull Duration requestTimeout,
        @Min(1) int burstEveryRequests,
        @Min(0) @Max(10_000) int burstSize
) {

    public GeneratorProperties {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("generator.duration must be positive");
        }
        if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("generator.request-timeout must be positive");
        }
        if (apiBaseUrl != null && !List.of("http", "https").contains(apiBaseUrl.getScheme())) {
            throw new IllegalArgumentException("generator.api-base-url must use http or https");
        }
        accountIds = accountIds == null ? List.of() : List.copyOf(accountIds);
    }
}
