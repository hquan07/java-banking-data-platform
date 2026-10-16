package io.github.hquan07.banking.publisher.retry;

import io.github.hquan07.banking.publisher.config.PublisherProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

@Component
public class BackoffPolicy {

    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double jitterFactor;
    private final DoubleSupplier random;

    @Autowired
    public BackoffPolicy(PublisherProperties properties) {
        this(
                properties.initialBackoff(),
                properties.maxBackoff(),
                properties.jitterFactor(),
                ThreadLocalRandom.current()::nextDouble
        );
    }

    BackoffPolicy(
            Duration initialBackoff,
            Duration maxBackoff,
            double jitterFactor,
            DoubleSupplier random
    ) {
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.jitterFactor = jitterFactor;
        this.random = random;
    }

    public Duration delayForAttempt(int attempt) {
        int exponent = Math.max(0, Math.min(attempt - 1, 62));
        long multiplier = 1L << exponent;
        long baseMillis;
        try {
            baseMillis = Math.multiplyExact(initialBackoff.toMillis(), multiplier);
        } catch (ArithmeticException exception) {
            baseMillis = maxBackoff.toMillis();
        }
        baseMillis = Math.min(baseMillis, maxBackoff.toMillis());
        double jitter = (random.getAsDouble() * 2.0 - 1.0) * jitterFactor;
        long jitteredMillis = Math.round(baseMillis * (1.0 + jitter));
        return Duration.ofMillis(Math.max(1, Math.min(jitteredMillis, maxBackoff.toMillis())));
    }
}
