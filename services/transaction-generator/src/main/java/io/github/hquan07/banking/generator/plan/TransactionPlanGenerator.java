package io.github.hquan07.banking.generator.plan;

import io.github.hquan07.banking.generator.config.GeneratorProperties;
import io.github.hquan07.banking.generator.model.GeneratedTransaction;
import io.github.hquan07.banking.generator.model.GeneratorScenario;
import io.github.hquan07.banking.generator.model.TransactionPayload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.SplittableRandom;

@Component
public class TransactionPlanGenerator {

    private static final List<String> TRANSACTION_TYPES = List.of(
            "DEPOSIT",
            "WITHDRAWAL",
            "PAYMENT",
            "TRANSFER"
    );
    private static final String LARGE_AMOUNT = "99999999999999999.99";

    private final GeneratorProperties properties;
    private final SplittableRandom random;
    private GeneratedTransaction lastReplayable;

    public TransactionPlanGenerator(GeneratorProperties properties) {
        this.properties = properties;
        this.random = new SplittableRandom(properties.seed());
    }

    public GeneratedTransaction next(long sequence) {
        int bucket = random.nextInt(100);
        GeneratorScenario scenario = scenario(bucket);
        if (scenario == GeneratorScenario.IDEMPOTENT_RETRY && lastReplayable != null) {
            return new GeneratedTransaction(
                    sequence,
                    lastReplayable.idempotencyKey(),
                    lastReplayable.payload(),
                    GeneratorScenario.IDEMPOTENT_RETRY
            );
        }

        String accountId = properties.accountIds().get(random.nextInt(properties.accountIds().size()));
        String type = TRANSACTION_TYPES.get(random.nextInt(TRANSACTION_TYPES.size()));
        String amount = amount(scenario);
        String currency = scenario == GeneratorScenario.CURRENCY_MISMATCH
                ? mismatchedCurrency(properties.currency())
                : properties.currency();
        GeneratedTransaction generated = new GeneratedTransaction(
                sequence,
                "generator-" + properties.seed() + "-" + sequence,
                new TransactionPayload(accountId, type, amount, currency),
                scenario == GeneratorScenario.IDEMPOTENT_RETRY ? GeneratorScenario.NORMAL : scenario
        );
        if (generated.scenario() == GeneratorScenario.NORMAL
                || generated.scenario() == GeneratorScenario.LARGE_AMOUNT) {
            lastReplayable = generated;
        }
        return generated;
    }

    private GeneratorScenario scenario(int bucket) {
        if (bucket < 91) {
            return GeneratorScenario.NORMAL;
        }
        if (bucket < 93) {
            return GeneratorScenario.LARGE_AMOUNT;
        }
        if (bucket < 95) {
            return GeneratorScenario.IDEMPOTENT_RETRY;
        }
        if (bucket < 98) {
            return GeneratorScenario.INVALID_AMOUNT;
        }
        return GeneratorScenario.CURRENCY_MISMATCH;
    }

    private String amount(GeneratorScenario scenario) {
        if (scenario == GeneratorScenario.INVALID_AMOUNT) {
            return "0.00";
        }
        if (scenario == GeneratorScenario.LARGE_AMOUNT) {
            return LARGE_AMOUNT;
        }
        long cents = random.nextLong(100, 1_000_000_001L);
        return BigDecimal.valueOf(cents, 2).setScale(2, RoundingMode.UNNECESSARY).toPlainString();
    }

    private String mismatchedCurrency(String configuredCurrency) {
        return "VND".equals(configuredCurrency) ? "USD" : "VND";
    }
}
