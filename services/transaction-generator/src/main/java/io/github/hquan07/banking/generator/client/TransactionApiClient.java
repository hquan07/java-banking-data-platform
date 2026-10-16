package io.github.hquan07.banking.generator.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.generator.config.GeneratorProperties;
import io.github.hquan07.banking.generator.model.AttemptResult;
import io.github.hquan07.banking.generator.model.GeneratedTransaction;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;

@Component
public class TransactionApiClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI endpoint;
    private final Duration requestTimeout;
    private final Clock clock;

    public TransactionApiClient(GeneratorProperties properties, ObjectMapper objectMapper) {
        this(
                HttpClient.newBuilder().connectTimeout(properties.requestTimeout()).build(),
                objectMapper,
                properties.apiBaseUrl().resolve("/api/v1/transactions"),
                properties.requestTimeout(),
                Clock.systemUTC()
        );
    }

    TransactionApiClient(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            URI endpoint,
            Duration requestTimeout,
            Clock clock
    ) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.endpoint = endpoint;
        this.requestTimeout = requestTimeout;
        this.clock = clock;
    }

    public AttemptResult send(GeneratedTransaction transaction) {
        long startedAt = System.nanoTime();
        try {
            HttpRequest request = HttpRequest.newBuilder(endpoint)
                    .timeout(requestTimeout)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", transaction.idempotencyKey())
                    .header("X-Correlation-ID", correlationId(transaction))
                    .POST(HttpRequest.BodyPublishers.ofString(serialize(transaction)))
                    .build();
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            return new AttemptResult(
                    transaction.scenario(),
                    response.statusCode(),
                    System.nanoTime() - startedAt,
                    null
            );
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return failure(transaction, startedAt, exception);
        } catch (IOException | RuntimeException exception) {
            return failure(transaction, startedAt, exception);
        }
    }

    private String serialize(GeneratedTransaction transaction) throws JsonProcessingException {
        return objectMapper.writeValueAsString(transaction.payload());
    }

    private String correlationId(GeneratedTransaction transaction) {
        return "generator-" + clock.instant().toEpochMilli() + "-" + transaction.sequence();
    }

    private AttemptResult failure(GeneratedTransaction transaction, long startedAt, Exception exception) {
        return new AttemptResult(
                transaction.scenario(),
                0,
                System.nanoTime() - startedAt,
                exception.getClass().getSimpleName()
        );
    }
}
