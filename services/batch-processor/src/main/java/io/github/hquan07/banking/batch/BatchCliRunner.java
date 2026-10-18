package io.github.hquan07.banking.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;

@Component
public class BatchCliRunner implements ApplicationRunner {

    private final DailyBatchService service;
    private final ObjectMapper objectMapper;

    public BatchCliRunner(DailyBatchService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments arguments) throws Exception {
        if (!arguments.getNonOptionArgs().isEmpty()) {
            throw new IllegalArgumentException("Only --job and --date options are supported");
        }
        String job = requiredSingle(arguments, "job");
        LocalDate date = parseDate(requiredSingle(arguments, "date"));
        Object result = switch (job) {
            case "check-daily-source" -> service.checkSource(date);
            case "daily-transactions" -> service.runDaily(date);
            case "validate-daily-transactions" -> service.validateDaily(date);
            case "publish-report-metadata" -> service.publishReportMetadata(date);
            default -> throw new IllegalArgumentException("Unsupported batch job: " + job);
        };
        System.out.println(objectMapper.writeValueAsString(result));
    }

    private String requiredSingle(ApplicationArguments arguments, String name) {
        List<String> values = arguments.getOptionValues(name);
        if (values == null || values.size() != 1 || values.getFirst().isBlank()) {
            throw new IllegalArgumentException("Exactly one --" + name + " value is required");
        }
        return values.getFirst();
    }

    private LocalDate parseDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("--date must use ISO-8601 YYYY-MM-DD format", exception);
        }
    }
}
