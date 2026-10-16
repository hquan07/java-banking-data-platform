package io.github.hquan07.banking.generator.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.github.hquan07.banking.generator.model.AttemptResult;
import io.github.hquan07.banking.generator.model.GeneratedTransaction;
import io.github.hquan07.banking.generator.model.GeneratorScenario;
import io.github.hquan07.banking.generator.model.TransactionPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TransactionApiClientTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicReference<String> idempotencyKey = new AtomicReference<>();
    private final AtomicReference<String> correlationId = new AtomicReference<>();
    private final AtomicReference<String> requestBody = new AtomicReference<>();
    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/transactions", request -> {
            idempotencyKey.set(request.getRequestHeaders().getFirst("Idempotency-Key"));
            correlationId.set(request.getRequestHeaders().getFirst("X-Correlation-ID"));
            requestBody.set(new String(request.getRequestBody().readAllBytes()));
            request.sendResponseHeaders(201, -1);
            request.close();
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void sendsStrictApiPayloadAndReliabilityHeaders() throws Exception {
        URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort()
                + "/api/v1/transactions");
        TransactionApiClient client = new TransactionApiClient(
                HttpClient.newHttpClient(),
                objectMapper,
                endpoint,
                Duration.ofSeconds(2),
                Clock.fixed(Instant.parse("2026-10-16T00:00:00Z"), ZoneOffset.UTC)
        );
        GeneratedTransaction transaction = new GeneratedTransaction(
                7,
                "generator-1607-7",
                new TransactionPayload("ACC_1", "PAYMENT", "25.00", "VND"),
                GeneratorScenario.NORMAL
        );

        AttemptResult result = client.send(transaction);

        assertEquals(201, result.httpStatus());
        assertFalse(result.transportFailure());
        assertEquals("generator-1607-7", idempotencyKey.get());
        assertEquals("generator-" + Instant.parse("2026-10-16T00:00:00Z").toEpochMilli() + "-7",
                correlationId.get());
        assertEquals("25.00", objectMapper.readTree(requestBody.get()).required("amount").asText());
    }
}
