package io.github.hquan07.banking.common.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContractSchemaTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final SchemaRegistry schemaRegistry = SchemaRegistry.withDefaultDialect(
            SpecificationVersion.DRAFT_2020_12
    );

    @Test
    void transactionEventRoundTripsAndMatchesVersionedSchema() throws Exception {
        TransactionCreatedEvent event = validTransactionEvent();

        String json = objectMapper.writeValueAsString(event);
        List<com.networknt.schema.Error> errors = schema("transaction-created-v1.schema.json")
                .validate(json, InputFormat.JSON, context ->
                        context.executionConfig(config -> config.formatAssertionsEnabled(true)));

        assertTrue(errors.isEmpty(), () -> "schema errors: " + errors);
        assertTrue(json.contains("\"amount\":\"1500000.00\""));
        assertTrue(json.contains("\"occurred_at\""));
        assertEquals(event, objectMapper.readValue(json, TransactionCreatedEvent.class));
    }

    @Test
    void transactionSchemaRejectsUnknownFieldsAndNumericMoney() throws Exception {
        String json = objectMapper.writeValueAsString(validTransactionEvent())
                .replace("\"amount\":\"1500000.00\"", "\"amount\":1500000.00")
                .replaceFirst("}$", ",\"card_number\":\"4111111111111111\"}");

        List<com.networknt.schema.Error> errors = schema("transaction-created-v1.schema.json")
                .validate(json, InputFormat.JSON);

        assertFalse(errors.isEmpty());
    }

    @Test
    void deadLetterEnvelopeMatchesSchemaWithoutEmbeddingRawPayload() throws Exception {
        DeadLetterEvent event = new DeadLetterEvent(
                null,
                "banking.transactions.v1",
                2,
                42,
                FailureClass.SCHEMA,
                "UNSUPPORTED_SCHEMA_VERSION",
                "schema version is not supported",
                Instant.parse("2026-10-16T03:31:00Z"),
                0,
                1,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ReplayStatus.PENDING
        );

        String json = objectMapper.writeValueAsString(event);
        List<com.networknt.schema.Error> errors = schema("dead-letter-event-v1.schema.json")
                .validate(json, InputFormat.JSON, context ->
                        context.executionConfig(config -> config.formatAssertionsEnabled(true)));

        assertTrue(errors.isEmpty(), () -> "schema errors: " + errors);
        assertFalse(json.contains("payload\""));
    }

    @Test
    void fraudAlertMatchesVersionedSchema() throws Exception {
        FraudAlertEvent alert = new FraudAlertEvent(
                UUID.fromString("fa81a807-a1fc-43d7-a558-4d277a997d3c"),
                FraudRule.HIGH_VALUE,
                FraudAlertEvent.RULE_VERSION,
                UUID.fromString("e16bc5b2-7e6b-4c83-9172-36f6d99a79b8"),
                "ACC10001",
                new BigDecimal("100000000.00"),
                "VND",
                1,
                Instant.parse("2026-10-16T03:30:00Z"),
                Instant.parse("2026-10-16T03:31:00Z"),
                Instant.parse("2026-10-16T03:30:00Z"),
                FraudAlertEvent.SCHEMA_VERSION
        );

        String json = objectMapper.writeValueAsString(alert);
        List<com.networknt.schema.Error> errors = schema("fraud-alert-v1.schema.json")
                .validate(json, InputFormat.JSON, context ->
                        context.executionConfig(config -> config.formatAssertionsEnabled(true)));

        assertTrue(errors.isEmpty(), () -> "schema errors: " + errors);
        assertEquals(alert, objectMapper.readValue(json, FraudAlertEvent.class));
    }

    @Test
    void aggregateMatchesVersionedSchema() throws Exception {
        TransactionAggregateEvent aggregate = new TransactionAggregateEvent(
                UUID.fromString("b5adee01-d50c-4946-8212-f48651ea9f1c"),
                "VND:2026-10-16T03:30:00Z",
                Instant.parse("2026-10-16T03:30:00Z"),
                Instant.parse("2026-10-16T03:31:00Z"),
                "VND",
                12,
                new BigDecimal("180000000.00"),
                2,
                TransactionAggregateEvent.SCHEMA_VERSION
        );

        String json = objectMapper.writeValueAsString(aggregate);
        List<com.networknt.schema.Error> errors = schema("transaction-aggregate-v1.schema.json")
                .validate(json, InputFormat.JSON, context ->
                        context.executionConfig(config -> config.formatAssertionsEnabled(true)));

        assertTrue(errors.isEmpty(), () -> "schema errors: " + errors);
        assertEquals(aggregate, objectMapper.readValue(json, TransactionAggregateEvent.class));
    }

    @Test
    void javaContractRejectsInvalidMoneyAndVersion() {
        TransactionCreatedEvent valid = validTransactionEvent();

        assertThrows(IllegalArgumentException.class, () -> new TransactionCreatedEvent(
                valid.eventId(),
                valid.eventType(),
                valid.schemaVersion(),
                valid.transactionId(),
                valid.accountId(),
                valid.transactionType(),
                new BigDecimal("0.00"),
                valid.currency(),
                valid.status(),
                valid.occurredAt(),
                valid.producedAt(),
                valid.correlationId()
        ));

        assertThrows(IllegalArgumentException.class, () -> new TransactionCreatedEvent(
                valid.eventId(),
                valid.eventType(),
                2,
                valid.transactionId(),
                valid.accountId(),
                valid.transactionType(),
                valid.amount(),
                valid.currency(),
                valid.status(),
                valid.occurredAt(),
                valid.producedAt(),
                valid.correlationId()
        ));
    }

    private Schema schema(String fileName) throws IOException {
        try (InputStream stream = getClass().getResourceAsStream("/contracts/" + fileName)) {
            if (stream == null) {
                throw new IllegalStateException("missing contract schema: " + fileName);
            }
            return schemaRegistry.getSchema(
                    new String(stream.readAllBytes(), StandardCharsets.UTF_8),
                    InputFormat.JSON
            );
        }
    }

    private TransactionCreatedEvent validTransactionEvent() {
        return new TransactionCreatedEvent(
                UUID.fromString("e16bc5b2-7e6b-4c83-9172-36f6d99a79b8"),
                TransactionCreatedEvent.EVENT_TYPE,
                TransactionCreatedEvent.SCHEMA_VERSION,
                UUID.fromString("b1aeb84d-40bd-40a6-b2c7-a22ddc71dcb9"),
                "ACC10001",
                TransactionType.TRANSFER,
                new BigDecimal("1500000.00"),
                "VND",
                TransactionStatus.SUCCESS,
                Instant.parse("2026-10-16T03:30:00Z"),
                Instant.parse("2026-10-16T03:30:01Z"),
                "req-20261016-0001"
        );
    }
}
