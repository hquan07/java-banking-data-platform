package io.github.hquan07.banking.publisher;

import io.github.hquan07.banking.publisher.model.PublishCycleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(name = "publisher.enabled", havingValue = "true")
public class OutboxPollScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPollScheduler.class);

    private final OutboxPublisherService publisherService;
    private final AtomicBoolean polling = new AtomicBoolean();

    public OutboxPollScheduler(OutboxPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    @Scheduled(fixedDelayString = "${publisher.poll-interval:1s}")
    public void poll() {
        if (!polling.compareAndSet(false, true)) {
            LOGGER.warn("Skipping overlapping outbox poll in the same process");
            return;
        }
        try {
            PublishCycleResult result = publisherService.publishAvailable();
            if (result.claimed() > 0) {
                LOGGER.atInfo()
                        .addKeyValue("claimed", result.claimed())
                        .addKeyValue("published", result.published())
                        .addKeyValue("failed", result.failed())
                        .log("Completed outbox publish cycle");
            }
        } catch (RuntimeException exception) {
            LOGGER.error("Outbox poll failed before individual event handling", exception);
        } finally {
            polling.set(false);
        }
    }
}
