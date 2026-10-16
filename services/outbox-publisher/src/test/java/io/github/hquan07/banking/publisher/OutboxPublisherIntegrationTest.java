package io.github.hquan07.banking.publisher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.common.event.TransactionType;
import io.github.hquan07.banking.publisher.model.OutboxRecord;
import io.github.hquan07.banking.publisher.model.PublishCycleResult;
import io.github.hquan07.banking.publisher.persistence.OutboxRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "publisher.enabled=false",
        "publisher.topic=banking.transactions.raw",
        "publisher.batch-size=10",
        "publisher.lease-duration=10s",
        "publisher.send-timeout=3s",
        "publisher.initial-backoff=100ms",
        "publisher.max-backoff=1s",
        "publisher.jitter-factor=0",
        "spring.kafka.producer.properties.delivery.timeout.ms=1500",
        "spring.kafka.producer.properties.request.timeout.ms=500",
        "spring.kafka.producer.properties.max.block.ms=1000",
        "management.server.port=0"
})
@Testcontainers(disabledWithoutDocker = true)
class OutboxPublisherIntegrationTest {

    private static final String TOPIC = "banking.transactions.raw";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-alpine")
            .withDatabaseName("banking")
            .withUsername("banking")
            .withPassword("banking-test-password");

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:4.3.1")
    );

    @DynamicPropertySource
    static void runtimeProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private OutboxPublisherService publisherService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeAll
    static void initializeInfrastructure() throws Exception {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers()
        ))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    @BeforeEach
    void cleanDatabase() {
        jdbcTemplate.update("DELETE FROM outbox_events");
        jdbcTemplate.update("DELETE FROM transactions");
        jdbcTemplate.update("DELETE FROM accounts");
        jdbcTemplate.update("DELETE FROM customers");
        jdbcTemplate.update("INSERT INTO customers (customer_id, status) VALUES ('CUS_PHASE3', 'ACTIVE')");
        jdbcTemplate.update("""
                INSERT INTO accounts (account_id, customer_id, currency, status)
                VALUES ('ACC_PHASE3', 'CUS_PHASE3', 'VND', 'ACTIVE')
                """);
    }

    @Test
    void publishesWithAccountKeyOnlyAfterAckAndRecoversAfterKafkaOutage() throws Exception {
        UUID firstEventId = seedOutbox("phase3-publish-1", Instant.parse("2026-10-16T01:00:00Z"));

        PublishCycleResult firstCycle = publisherService.publishAvailable();

        assertEquals(new PublishCycleResult(1, 1, 0), firstCycle);
        assertPublished(firstEventId, 1);
        ConsumedEvent first = consume(firstEventId);
        assertEquals("ACC_PHASE3", first.key());
        assertEquals(firstEventId.toString(), first.payload().required("event_id").asText());

        UUID outageEventId = seedOutbox("phase3-outage-1", Instant.parse("2026-10-16T01:01:00Z"));
        pauseKafka();
        PublishCycleResult outageCycle;
        try {
            outageCycle = publisherService.publishAvailable();
        } finally {
            unpauseKafka();
        }

        assertEquals(new PublishCycleResult(1, 0, 1), outageCycle);
        assertRetainedForRetry(outageEventId);

        jdbcTemplate.update(
                "UPDATE outbox_events SET next_attempt_at = NOW() WHERE event_id = ?",
                outageEventId
        );
        PublishCycleResult recoveryCycle = publisherService.publishAvailable();

        assertEquals(new PublishCycleResult(1, 1, 0), recoveryCycle);
        assertPublished(outageEventId, 2);
        assertEquals("ACC_PHASE3", consume(outageEventId).key());
    }

    @Test
    void concurrentWorkersClaimDisjointRowsAndExpiredLeaseIsRecoverable() throws Exception {
        UUID first = seedOutbox("phase3-claim-1", Instant.parse("2026-10-16T02:00:00Z"));
        UUID second = seedOutbox("phase3-claim-2", Instant.parse("2026-10-16T02:00:01Z"));
        Instant now = Instant.parse("2026-10-16T02:01:00Z");
        UUID firstToken = UUID.randomUUID();
        UUID secondToken = UUID.randomUUID();

        CompletableFuture<List<OutboxRecord>> firstClaim = CompletableFuture.supplyAsync(
                () -> outboxRepository.claimBatch(firstToken, now, Duration.ofSeconds(5), 1)
        );
        CompletableFuture<List<OutboxRecord>> secondClaim = CompletableFuture.supplyAsync(
                () -> outboxRepository.claimBatch(secondToken, now, Duration.ofSeconds(5), 1)
        );
        Set<UUID> claimed = Set.of(
                firstClaim.get().getFirst().eventId(),
                secondClaim.get().getFirst().eventId()
        );
        assertEquals(Set.of(first, second), claimed);

        UUID recoveryToken = UUID.randomUUID();
        List<OutboxRecord> recovered = outboxRepository.claimBatch(
                recoveryToken,
                now.plusSeconds(6),
                Duration.ofSeconds(5),
                10
        );
        assertEquals(Set.of(first, second), recovered.stream().map(OutboxRecord::eventId).collect(java.util.stream.Collectors.toSet()));
        assertTrue(recovered.stream().allMatch(record -> record.attempts() == 2));
    }

    private UUID seedOutbox(String idempotencyKey, Instant occurredAt) throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        jdbcTemplate.update("""
                        INSERT INTO transactions (
                            transaction_id, account_id, transaction_type, amount, currency, status,
                            occurred_at, idempotency_key, request_fingerprint, correlation_id
                        ) VALUES (?, 'ACC_PHASE3', 'PAYMENT', 25.00, 'VND', 'SUCCESS', ?, ?, ?, ?)
                        """,
                transactionId,
                java.sql.Timestamp.from(occurredAt),
                idempotencyKey,
                "a".repeat(64),
                "phase3-integration"
        );
        TransactionCreatedEvent event = new TransactionCreatedEvent(
                eventId,
                TransactionCreatedEvent.EVENT_TYPE,
                TransactionCreatedEvent.SCHEMA_VERSION,
                transactionId,
                "ACC_PHASE3",
                TransactionType.PAYMENT,
                new BigDecimal("25.00"),
                "VND",
                TransactionStatus.SUCCESS,
                occurredAt,
                occurredAt.plusMillis(10),
                "phase3-integration"
        );
        jdbcTemplate.update("""
                        INSERT INTO outbox_events (
                            event_id, aggregate_id, event_type, schema_version, payload,
                            created_at, next_attempt_at
                        ) VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)
                        """,
                eventId,
                transactionId,
                event.eventType(),
                event.schemaVersion(),
                objectMapper.writeValueAsString(event),
                java.sql.Timestamp.from(occurredAt),
                java.sql.Timestamp.from(occurredAt)
        );
        return eventId;
    }

    private void assertPublished(UUID eventId, int expectedAttempts) {
        Map<String, Object> row = jdbcTemplate.queryForMap("""
                SELECT published_at, attempts, claim_token, locked_until, last_error
                FROM outbox_events WHERE event_id = ?
                """, eventId);
        assertNotNull(row.get("published_at"));
        assertEquals(expectedAttempts, row.get("attempts"));
        assertNull(row.get("claim_token"));
        assertNull(row.get("locked_until"));
        assertNull(row.get("last_error"));
    }

    private void assertRetainedForRetry(UUID eventId) {
        Map<String, Object> row = jdbcTemplate.queryForMap("""
                SELECT published_at, attempts, claim_token, locked_until, last_error, next_attempt_at
                FROM outbox_events WHERE event_id = ?
                """, eventId);
        assertNull(row.get("published_at"));
        assertEquals(1, row.get("attempts"));
        assertNull(row.get("claim_token"));
        assertNull(row.get("locked_until"));
        assertNotNull(row.get("last_error"));
        assertNotNull(row.get("next_attempt_at"));
    }

    private ConsumedEvent consume(UUID expectedEventId) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "phase3-test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                for (var record : records) {
                    JsonNode payload = objectMapper.readTree(record.value());
                    if (expectedEventId.toString().equals(payload.required("event_id").asText())) {
                        return new ConsumedEvent(record.key(), payload);
                    }
                }
            }
        }
        throw new AssertionError("Kafka event not received: " + expectedEventId);
    }

    private void pauseKafka() {
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private void unpauseKafka() {
        KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private record ConsumedEvent(String key, JsonNode payload) {
    }
}
