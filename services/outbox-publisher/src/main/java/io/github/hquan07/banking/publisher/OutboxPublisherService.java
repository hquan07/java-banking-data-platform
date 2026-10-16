package io.github.hquan07.banking.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.publisher.config.PublisherProperties;
import io.github.hquan07.banking.publisher.metrics.PublisherMetrics;
import io.github.hquan07.banking.publisher.model.OutboxRecord;
import io.github.hquan07.banking.publisher.model.PublishCycleResult;
import io.github.hquan07.banking.publisher.persistence.OutboxRepository;
import io.github.hquan07.banking.publisher.retry.BackoffPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class OutboxPublisherService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublisherService.class);
    private static final int MAX_ERROR_LENGTH = 1024;

    private final PublisherProperties properties;
    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final BackoffPolicy backoffPolicy;
    private final PublisherMetrics metrics;
    private final Clock clock;

    public OutboxPublisherService(
            PublisherProperties properties,
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            BackoffPolicy backoffPolicy,
            PublisherMetrics metrics,
            Clock clock
    ) {
        this.properties = properties;
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.backoffPolicy = backoffPolicy;
        this.metrics = metrics;
        this.clock = clock;
    }

    public PublishCycleResult publishAvailable() {
        UUID claimToken = UUID.randomUUID();
        Instant now = clock.instant();
        var records = outboxRepository.claimBatch(
                claimToken,
                now,
                properties.leaseDuration(),
                properties.batchSize()
        );
        int published = 0;
        int failed = 0;
        for (OutboxRecord record : records) {
            if (record.attempts() > 1) {
                metrics.recordRetry();
            }
            long startedAt = System.nanoTime();
            try {
                TransactionCreatedEvent event = deserialize(record);
                kafkaTemplate.send(properties.topic(), event.accountId(), record.payload())
                        .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
                if (outboxRepository.markPublished(record.eventId(), claimToken, clock.instant())) {
                    published++;
                    metrics.recordPublished(Duration.ofNanos(System.nanoTime() - startedAt));
                    LOGGER.atInfo()
                            .addKeyValue("event_id", record.eventId())
                            .addKeyValue("transaction_id", record.aggregateId())
                            .addKeyValue("attempt", record.attempts())
                            .log("Published outbox event");
                } else {
                    failed++;
                    metrics.recordFailure();
                    LOGGER.atWarn()
                            .addKeyValue("event_id", record.eventId())
                            .log("Kafka acknowledged event but database claim was no longer owned");
                }
            } catch (Exception exception) {
                failed++;
                metrics.recordFailure();
                Duration delay = backoffPolicy.delayForAttempt(record.attempts());
                boolean released = outboxRepository.markFailed(
                        record.eventId(),
                        claimToken,
                        clock.instant().plus(delay),
                        safeError(exception)
                );
                LOGGER.atWarn()
                        .addKeyValue("event_id", record.eventId())
                        .addKeyValue("attempt", record.attempts())
                        .addKeyValue("retry_delay_ms", delay.toMillis())
                        .addKeyValue("claim_released", released)
                        .log("Outbox publish failed; event retained for retry");
            }
        }
        metrics.updateBacklog(outboxRepository.countUnpublished());
        return new PublishCycleResult(records.size(), published, failed);
    }

    private TransactionCreatedEvent deserialize(OutboxRecord record) throws JsonProcessingException {
        TransactionCreatedEvent event = objectMapper.readValue(record.payload(), TransactionCreatedEvent.class);
        if (!record.eventId().equals(event.eventId())
                || !record.aggregateId().equals(event.transactionId())
                || !record.eventType().equals(event.eventType())
                || record.schemaVersion() != event.schemaVersion()) {
            throw new IllegalArgumentException("Outbox metadata does not match event payload");
        }
        return event;
    }

    private String safeError(Exception exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        String sanitized = root.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message)
                .replace('\n', ' ')
                .replace('\r', ' ');
        return sanitized.substring(0, Math.min(sanitized.length(), MAX_ERROR_LENGTH));
    }
}
