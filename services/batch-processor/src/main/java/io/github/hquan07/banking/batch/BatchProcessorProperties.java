package io.github.hquan07.banking.batch;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.DateTimeException;
import java.time.ZoneId;

@Validated
@ConfigurationProperties(prefix = "batch")
public record BatchProcessorProperties(
        @NotBlank String jobName,
        @NotBlank String businessTimezone,
        @NotBlank String minioEndpoint,
        @NotBlank String minioAccessKey,
        @NotBlank String minioSecretKey,
        @NotBlank String minioBucket
) {
    public ZoneId zoneId() {
        return ZoneId.of(businessTimezone);
    }

    @AssertTrue(message = "batch.business-timezone must be a valid IANA time zone")
    public boolean isTimezoneValid() {
        try {
            zoneId();
            return true;
        } catch (DateTimeException exception) {
            return false;
        }
    }
}
