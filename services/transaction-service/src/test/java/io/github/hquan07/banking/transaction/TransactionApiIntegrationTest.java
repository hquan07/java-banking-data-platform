package io.github.hquan07.banking.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.common.event.TransactionType;
import io.github.hquan07.banking.transaction.api.TransactionRequest;
import io.github.hquan07.banking.transaction.application.CreateTransactionResult;
import io.github.hquan07.banking.transaction.application.TransactionApplicationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "management.server.port=0")
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class TransactionApiIntegrationTest {

    private static final String VALID_REQUEST = """
            {
              "accountId": "ACC_PHASE2",
              "transactionType": "TRANSFER",
              "amount": "1500000.00",
              "currency": "VND"
            }
            """;

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withDatabaseName("banking")
            .withUsername("banking")
            .withPassword("banking-test-password");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TransactionApplicationService transactionService;

    @BeforeEach
    void seedActiveAccount() {
        removeRollbackTrigger();
        jdbcTemplate.update("DELETE FROM fraud_alerts");
        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM transactions");
        jdbcTemplate.update("DELETE FROM accounts");
        jdbcTemplate.update("DELETE FROM customers");
        jdbcTemplate.update("INSERT INTO customers (customer_id, status) VALUES ('CUS_PHASE2', 'ACTIVE')");
        jdbcTemplate.update("""
                INSERT INTO accounts (account_id, customer_id, currency, status)
                VALUES ('ACC_PHASE2', 'CUS_PHASE2', 'VND', 'ACTIVE')
                """);
    }

    @AfterEach
    void removeTestTrigger() {
        removeRollbackTrigger();
    }

    @Test
    void createsTransactionAndOutboxInOneRequest() throws Exception {
        String responseBody = mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", "phase2-create-001")
                        .header("X-Correlation-ID", "phase2-correlation-001")
                        .content(VALID_REQUEST))
                .andExpect(status().isCreated())
                .andExpect(header().string("X-Correlation-ID", "phase2-correlation-001"))
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.amount").value("1500000.00"))
                .andExpect(jsonPath("$.correlationId").value("phase2-correlation-001"))
                .andExpect(jsonPath("$.idempotentReplay").value(false))
                .andReturn()
                .getResponse()
                .getContentAsString();

        JsonNode response = objectMapper.readTree(responseBody);
        String transactionId = response.required("transactionId").asText();
        assertEquals(1, count("transactions"));
        assertEquals(1, count("outbox_events"));

        String payload = jdbcTemplate.queryForObject(
                "SELECT payload::text FROM outbox_events WHERE aggregate_id = ?::uuid",
                String.class,
                transactionId
        );
        JsonNode event = objectMapper.readTree(payload);
        assertEquals("transaction.created", event.required("event_type").asText());
        assertEquals(transactionId, event.required("transaction_id").asText());
        assertEquals("1500000.00", event.required("amount").asText());
        assertEquals("phase2-correlation-001", event.required("correlation_id").asText());
    }

    @Test
    void replaysSameRequestWithoutCreatingDuplicates() throws Exception {
        String firstBody = create("phase2-replay-001", VALID_REQUEST, "correlation-first", 201);
        String replayBody = create("phase2-replay-001", VALID_REQUEST, "correlation-replay", 200);

        assertEquals(
                objectMapper.readTree(firstBody).required("transactionId").asText(),
                objectMapper.readTree(replayBody).required("transactionId").asText()
        );
        assertTrue(objectMapper.readTree(replayBody).required("idempotentReplay").asBoolean());
        assertEquals("correlation-replay", objectMapper.readTree(replayBody).required("correlationId").asText());
        assertEquals(1, count("transactions"));
        assertEquals(1, count("outbox_events"));
    }

    @Test
    void rejectsReuseOfKeyForDifferentPayload() throws Exception {
        create("phase2-conflict-001", VALID_REQUEST, "correlation-original", 201);
        String differentPayload = VALID_REQUEST.replace("1500000.00", "100.00");

        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", "phase2-conflict-001")
                        .content(differentPayload))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"))
                .andExpect(jsonPath("$.message").value(
                        "The idempotency key was already used for a different request"
                ));

        assertEquals(1, count("transactions"));
        assertEquals(1, count("outbox_events"));
    }

    @Test
    void serializesConcurrentRequestsWithTheSameIdempotencyKey() throws Exception {
        TransactionRequest request = new TransactionRequest(
                "ACC_PHASE2",
                TransactionType.PAYMENT,
                "25.00",
                "VND"
        );
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<CreateTransactionResult> first = executor.submit(() -> {
                start.await();
                return transactionService.create(request, "phase2-concurrent-001", "concurrent-first");
            });
            Future<CreateTransactionResult> second = executor.submit(() -> {
                start.await();
                return transactionService.create(request, "phase2-concurrent-001", "concurrent-second");
            });
            start.countDown();

            List<CreateTransactionResult> results = List.of(first.get(), second.get());
            assertEquals(1, results.stream().filter(CreateTransactionResult::idempotentReplay).count());
            assertEquals(1, results.stream().filter(result -> !result.idempotentReplay()).count());
            assertEquals(1, results.stream().map(result -> result.transaction().transactionId()).distinct().count());
        } finally {
            executor.shutdownNow();
        }

        assertEquals(1, count("transactions"));
        assertEquals(1, count("outbox_events"));
    }

    @Test
    void rollsBackTransactionWhenOutboxInsertFails() throws Exception {
        installRollbackTrigger();

        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", "phase2-rollback-001")
                        .header("X-Correlation-ID", "rollback-test")
                        .content(VALID_REQUEST))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("DEPENDENCY_UNAVAILABLE"))
                .andExpect(jsonPath("$.message").value("A required dependency is temporarily unavailable"));

        assertEquals(0, count("transactions"));
        assertEquals(0, count("outbox_events"));
    }

    @Test
    void validatesRequestAndReturnsSafeErrors() throws Exception {
        String invalidRequest = VALID_REQUEST.replace("1500000.00", "0.00");
        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", "phase2-invalid-001")
                        .content(invalidRequest))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.details[0].field").value("amount"));

        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .content(VALID_REQUEST))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        String numericAmount = VALID_REQUEST.replace("\"1500000.00\"", "1500000.00");
        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", "phase2-numeric-money")
                        .content(numericAmount))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        String unknownField = VALID_REQUEST.replace(
                "\"currency\": \"VND\"",
                "\"currency\": \"VND\", \"unexpected\": true"
        );
        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", "phase2-unknown-field")
                        .content(unknownField))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        assertEquals(0, count("transactions"));
        assertEquals(0, count("outbox_events"));
    }

    @Test
    void retrievesExistingTransactionAndReportsMissingOnes() throws Exception {
        String createdBody = create("phase2-get-001", VALID_REQUEST, "create-correlation", 201);
        String transactionId = objectMapper.readTree(createdBody).required("transactionId").asText();

        mockMvc.perform(get("/api/v1/transactions/{transactionId}", transactionId)
                        .header("X-Correlation-ID", "lookup-correlation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value(transactionId))
                .andExpect(jsonPath("$.correlationId").value("lookup-correlation"));

        mockMvc.perform(get("/api/v1/transactions/{transactionId}",
                        "00000000-0000-0000-0000-000000000999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("RESOURCE_NOT_FOUND"));
    }

    @Test
    void rejectsMissingAccountInactiveAccountAndCurrencyMismatch() throws Exception {
        String missingAccount = VALID_REQUEST.replace("ACC_PHASE2", "ACC_MISSING");
        createExpectingError("phase2-missing-account", missingAccount, 404, "RESOURCE_NOT_FOUND");

        jdbcTemplate.update("UPDATE accounts SET status = 'FROZEN' WHERE account_id = 'ACC_PHASE2'");
        createExpectingError("phase2-frozen-account", VALID_REQUEST, 409, "ACCOUNT_STATE_CONFLICT");

        jdbcTemplate.update("UPDATE accounts SET status = 'ACTIVE' WHERE account_id = 'ACC_PHASE2'");
        String wrongCurrency = VALID_REQUEST.replace("VND", "USD");
        createExpectingError("phase2-currency", wrongCurrency, 409, "ACCOUNT_STATE_CONFLICT");

        assertEquals(0, count("transactions"));
        assertEquals(0, count("outbox_events"));
    }

    private String create(String key, String body, String correlationId, int expectedStatus) throws Exception {
        return mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", key)
                        .header("X-Correlation-ID", correlationId)
                        .content(body))
                .andExpect(status().is(expectedStatus))
                .andReturn()
                .getResponse()
                .getContentAsString();
    }

    private void createExpectingError(String key, String body, int expectedStatus, String expectedCode)
            throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                        .contentType("application/json")
                        .header("Idempotency-Key", key)
                        .content(body))
                .andExpect(status().is(expectedStatus))
                .andExpect(jsonPath("$.code").value(expectedCode));
    }

    private int count(String table) {
        Set<String> allowedTables = Set.of("transactions", "outbox_events");
        if (!allowedTables.contains(table)) {
            throw new IllegalArgumentException("Unsupported table: " + table);
        }
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }

    private void installRollbackTrigger() {
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION reject_phase2_rollback_test()
                RETURNS trigger AS $$
                BEGIN
                    IF NEW.payload->>'correlation_id' = 'rollback-test' THEN
                        RAISE EXCEPTION 'forced outbox failure';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER phase2_rollback_test
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION reject_phase2_rollback_test()
                """);
    }

    private void removeRollbackTrigger() {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS phase2_rollback_test ON outbox_events");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS reject_phase2_rollback_test()");
    }
}
