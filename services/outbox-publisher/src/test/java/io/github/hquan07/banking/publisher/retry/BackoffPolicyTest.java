package io.github.hquan07.banking.publisher.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BackoffPolicyTest {

    @Test
    void doublesDelayUntilCapAndAppliesBoundedJitter() {
        BackoffPolicy noJitter = new BackoffPolicy(
                Duration.ofSeconds(1),
                Duration.ofSeconds(5),
                0,
                () -> 0.5
        );
        assertEquals(Duration.ofSeconds(1), noJitter.delayForAttempt(1));
        assertEquals(Duration.ofSeconds(2), noJitter.delayForAttempt(2));
        assertEquals(Duration.ofSeconds(4), noJitter.delayForAttempt(3));
        assertEquals(Duration.ofSeconds(5), noJitter.delayForAttempt(4));
        assertEquals(Duration.ofSeconds(5), noJitter.delayForAttempt(60));

        BackoffPolicy negativeJitter = new BackoffPolicy(
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                0.2,
                () -> 0.0
        );
        assertEquals(Duration.ofSeconds(8), negativeJitter.delayForAttempt(1));
    }
}
