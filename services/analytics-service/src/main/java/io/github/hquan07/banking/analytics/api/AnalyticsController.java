package io.github.hquan07.banking.analytics.api;

import io.github.hquan07.banking.analytics.application.AnalyticsQueryService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

@Validated
@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsController {

    private final AnalyticsQueryService service;

    public AnalyticsController(AnalyticsQueryService service) {
        this.service = service;
    }

    @GetMapping("/daily")
    ResponseEntity<DailyAnalyticsResponse> daily(
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    ) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(30, TimeUnit.SECONDS).cachePrivate())
                .body(service.daily(date));
    }

    @GetMapping("/fraud")
    PageResponse<FraudAlertResponse> fraud(
            @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(name = "page", defaultValue = "0") @Min(0) @Max(1000) int page,
            @RequestParam(name = "size", defaultValue = "50") @Min(1) @Max(200) int size
    ) {
        return service.fraud(from, to, page, size);
    }

    @GetMapping("/accounts/{accountId}/summary")
    AccountSummaryResponse accountSummary(
            @PathVariable("accountId")
            @Size(min = 1, max = 64)
            @Pattern(regexp = "[A-Za-z0-9._:-]+")
            String accountId
    ) {
        return service.accountSummary(accountId);
    }
}
