package io.github.hquan07.banking.streaming.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.hquan07.banking.common.event.FailureClass;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;
import io.github.hquan07.banking.streaming.model.RawKafkaRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TransactionValidatorTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final TransactionValidator validator = new TransactionValidator();

    @Test
    void routesValidContractToValidatedStream() throws Exception {
        TransactionCreatedEvent event = event();

        ValidationOutcome outcome = validator.validate(record(
                event.accountId(), objectMapper.writeValueAsBytes(event)
        ));

        assertEquals(ValidationRoute.VALIDATED, outcome.route());
        assertEquals(event, outcome.transaction().event());
        assertEquals(1_760_000_000_000L, outcome.transaction().sourceTimestamp());
        assertNull(outcome.failure());
    }

    @Test
    void rejectsMalformedJsonWithoutRetainingPayload() {
        ValidationOutcome outcome = validator.validate(record("ACC10001", "{secret".getBytes()));

        assertEquals(ValidationRoute.INVALID, outcome.route());
        assertEquals(FailureClass.SCHEMA, outcome.failure().failureClass());
        assertEquals("MALFORMED_JSON", outcome.failure().failureCode());
        assertNotNull(outcome.failure().payloadHash());
    }

    @Test
    void rejectsUnsupportedSchemaVersion() throws Exception {
        byte[] payload = objectMapper.writeValueAsBytes(event());
        String json = new String(payload).replace("\"schema_version\":1", "\"schema_version\":2");

        ValidationOutcome outcome = validator.validate(record("ACC10001", json.getBytes()));

        assertEquals(ValidationRoute.INVALID, outcome.route());
        assertEquals("UNSUPPORTED_SCHEMA_VERSION", outcome.failure().failureCode());
    }

    @Test
    void rejectsKafkaKeyThatDoesNotMatchAccount() throws Exception {
        ValidationOutcome outcome = validator.validate(record(
                "ACC-OTHER", objectMapper.writeValueAsBytes(event())
        ));

        assertEquals(ValidationRoute.INVALID, outcome.route());
        assertEquals(FailureClass.BUSINESS, outcome.failure().failureClass());
        assertEquals("KAFKA_KEY_MISMATCH", outcome.failure().failureCode());
    }

    private RawKafkaRecord record(String key, byte[] payload) {
        return new RawKafkaRecord(
                "banking.transactions.raw",
                1,
                42,
                1_760_000_000_000L,
                key,
                payload
        );
    }

    private TransactionCreatedEvent event() {
        return new TransactionCreatedEvent(
                UUID.fromString("85d8145e-912b-4127-9580-d369bbe67c0e"),
                TransactionCreatedEvent.EVENT_TYPE,
                TransactionCreatedEvent.SCHEMA_VERSION,
                UUID.fromString("511e5869-0627-4bc4-abda-9401ee58206f"),
                "ACC10001",
                TransactionType.PAYMENT,
                new BigDecimal("125000.00"),
                "VND",
                TransactionStatus.SUCCESS,
                Instant.parse("2026-10-17T02:30:00Z"),
                Instant.parse("2026-10-17T02:30:01Z"),
                "phase5-test"
        );
    }
}
