package io.github.hquan07.banking.generator.plan;

import io.github.hquan07.banking.generator.config.GeneratorProperties;
import io.github.hquan07.banking.generator.model.GeneratedTransaction;
import io.github.hquan07.banking.generator.model.GeneratorScenario;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionPlanGeneratorTest {

    @Test
    void producesDeterministicTrafficUsingOnlyConfiguredAccounts() {
        GeneratorProperties properties = properties(1607L);
        TransactionPlanGenerator first = new TransactionPlanGenerator(properties);
        TransactionPlanGenerator second = new TransactionPlanGenerator(properties);

        List<GeneratedTransaction> firstPlan = generate(first, 10_000);
        List<GeneratedTransaction> secondPlan = generate(second, 10_000);

        assertEquals(firstPlan, secondPlan);
        assertTrue(firstPlan.stream().allMatch(transaction -> Set.of("ACC_1", "ACC_2")
                .contains(transaction.payload().accountId())));

        long intentionallyInvalid = firstPlan.stream()
                .filter(transaction -> transaction.scenario() == GeneratorScenario.INVALID_AMOUNT
                        || transaction.scenario() == GeneratorScenario.CURRENCY_MISMATCH)
                .count();
        double invalidPercentage = intentionallyInvalid * 100.0 / firstPlan.size();
        assertTrue(invalidPercentage >= 4.0 && invalidPercentage <= 6.0,
                () -> "invalid percentage was " + invalidPercentage);
    }

    @Test
    void idempotentRetryReusesBothKeyAndPayload() {
        TransactionPlanGenerator generator = new TransactionPlanGenerator(properties(42L));
        List<GeneratedTransaction> plan = generate(generator, 2_000);

        for (int index = 0; index < plan.size(); index++) {
            GeneratedTransaction candidate = plan.get(index);
            if (candidate.scenario() == GeneratorScenario.IDEMPOTENT_RETRY) {
                GeneratedTransaction original = plan.subList(0, index).stream()
                        .filter(previous -> previous.idempotencyKey().equals(candidate.idempotencyKey()))
                        .findFirst()
                        .orElseThrow();
                assertEquals(original.payload(), candidate.payload());
                return;
            }
        }
        throw new AssertionError("deterministic plan did not contain an idempotent retry");
    }

    private List<GeneratedTransaction> generate(TransactionPlanGenerator generator, int count) {
        List<GeneratedTransaction> generated = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            generated.add(generator.next(index));
        }
        return generated;
    }

    private GeneratorProperties properties(long seed) {
        return new GeneratorProperties(
                true,
                URI.create("http://localhost:8080"),
                100,
                Duration.ofSeconds(10),
                seed,
                List.of("ACC_1", "ACC_2"),
                "VND",
                100,
                Duration.ofSeconds(2),
                100,
                10
        );
    }
}
