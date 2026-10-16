package io.github.hquan07.banking.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.generator.client.TransactionApiClient;
import io.github.hquan07.banking.generator.config.GeneratorProperties;
import io.github.hquan07.banking.generator.metrics.GeneratorMetrics;
import io.github.hquan07.banking.generator.metrics.GeneratorSummary;
import io.github.hquan07.banking.generator.model.AttemptResult;
import io.github.hquan07.banking.generator.model.GeneratedTransaction;
import io.github.hquan07.banking.generator.plan.TransactionPlanGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Component
public class TransactionGeneratorRunner implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionGeneratorRunner.class);
    private static final int MAX_PLANNED_REQUESTS = 5_000_000;

    private final GeneratorProperties properties;
    private final TransactionPlanGenerator planGenerator;
    private final TransactionApiClient apiClient;
    private final GeneratorMetrics metrics;
    private final ObjectMapper objectMapper;

    public TransactionGeneratorRunner(
            GeneratorProperties properties,
            TransactionPlanGenerator planGenerator,
            TransactionApiClient apiClient,
            GeneratorMetrics metrics,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.planGenerator = planGenerator;
        this.apiClient = apiClient;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments arguments) throws InterruptedException {
        if (!properties.enabled()) {
            LOGGER.info("Transaction generator is disabled; set GENERATOR_ENABLED=true to run it");
            return;
        }

        List<ScheduledTransaction> plan = buildPlan();
        CountDownLatch completed = new CountDownLatch(plan.size());
        Semaphore inFlight = new Semaphore(properties.maxInFlight());
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        long startedAt = System.nanoTime();
        LOGGER.atInfo()
                .addKeyValue("requests", plan.size())
                .addKeyValue("tps", properties.transactionsPerSecond())
                .addKeyValue("duration", properties.duration())
                .addKeyValue("seed", properties.seed())
                .log("Starting synthetic transaction generation");

        try {
            for (ScheduledTransaction scheduled : plan) {
                timer.schedule(
                        () -> workers.submit(() -> execute(scheduled.transaction(), inFlight, completed)),
                        scheduled.delayNanos(),
                        TimeUnit.NANOSECONDS
                );
            }
            Duration waitLimit = properties.duration().plus(properties.requestTimeout()).plusSeconds(30);
            if (!completed.await(waitLimit.toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Generator wait limit reached with {} requests unfinished", completed.getCount());
            }
        } finally {
            timer.shutdownNow();
            workers.shutdownNow();
        }

        GeneratorSummary summary = metrics.summarize(Duration.ofNanos(System.nanoTime() - startedAt));
        LOGGER.info("Generator summary: {}", summaryJson(summary));
    }

    private List<ScheduledTransaction> buildPlan() {
        long baseCount = Math.multiplyExact(
                properties.duration().toNanos(),
                properties.transactionsPerSecond()
        ) / TimeUnit.SECONDS.toNanos(1);
        long bursts = baseCount / properties.burstEveryRequests();
        long total = Math.addExact(baseCount, Math.multiplyExact(bursts, properties.burstSize()));
        if (total > MAX_PLANNED_REQUESTS) {
            throw new IllegalArgumentException("Generator plan exceeds " + MAX_PLANNED_REQUESTS + " requests");
        }

        List<ScheduledTransaction> plan = new ArrayList<>((int) total);
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / properties.transactionsPerSecond();
        long sequence = 0;
        for (long baseIndex = 0; baseIndex < baseCount; baseIndex++) {
            long delay = baseIndex * intervalNanos;
            plan.add(new ScheduledTransaction(delay, planGenerator.next(sequence++)));
            if ((baseIndex + 1) % properties.burstEveryRequests() == 0) {
                for (int burstIndex = 0; burstIndex < properties.burstSize(); burstIndex++) {
                    plan.add(new ScheduledTransaction(delay, planGenerator.next(sequence++)));
                }
            }
        }
        return plan;
    }

    private void execute(
            GeneratedTransaction transaction,
            Semaphore inFlight,
            CountDownLatch completed
    ) {
        boolean acquired = false;
        try {
            inFlight.acquire();
            acquired = true;
            AttemptResult result = apiClient.send(transaction);
            metrics.record(result);
            if (result.transportFailure()) {
                LOGGER.atWarn()
                        .addKeyValue("scenario", result.scenario())
                        .addKeyValue("failure_type", result.failureType())
                        .log("Synthetic transaction transport failed");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            if (acquired) {
                inFlight.release();
            }
            completed.countDown();
        }
    }

    private String summaryJson(GeneratorSummary summary) {
        try {
            return objectMapper.writeValueAsString(summary);
        } catch (JsonProcessingException exception) {
            return summary.toString();
        }
    }

    private record ScheduledTransaction(long delayNanos, GeneratedTransaction transaction) {
    }
}
