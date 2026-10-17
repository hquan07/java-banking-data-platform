package io.github.hquan07.banking.streaming.config;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;

public record StreamingConfig(
        String bootstrapServers,
        String groupId,
        StartingOffsetsMode startingOffsetsMode,
        String rawTopic,
        String validatedTopic,
        String invalidTopic,
        String fraudAlertsTopic,
        String aggregatesTopic,
        String dlqTopic,
        Duration checkpointInterval,
        String checkpointDirectory,
        Duration watermarkOutOfOrderness,
        Duration watermarkIdleness,
        Duration allowedLateness,
        Duration deduplicationTtl,
        BigDecimal highValueThreshold,
        int velocityThreshold,
        int parallelism
) {

    public static StreamingConfig fromEnvironment() {
        return from(System.getenv());
    }

    static StreamingConfig from(Map<String, String> environment) {
        return new StreamingConfig(
                value(environment, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                value(environment, "STREAMING_GROUP_ID", "banking-streaming-v1"),
                startingOffsetsMode(environment),
                value(environment, "RAW_TOPIC", "banking.transactions.raw"),
                value(environment, "VALIDATED_TOPIC", "banking.transactions.validated"),
                value(environment, "INVALID_TOPIC", "banking.transactions.invalid"),
                value(environment, "FRAUD_ALERTS_TOPIC", "banking.transactions.fraud-alerts"),
                value(environment, "AGGREGATES_TOPIC", "banking.transactions.aggregates"),
                value(environment, "DLQ_TOPIC", "banking.transactions.dlq"),
                duration(environment, "CHECKPOINT_INTERVAL", Duration.ofSeconds(5)),
                value(environment, "CHECKPOINT_DIRECTORY", "file:///opt/flink/checkpoints"),
                duration(environment, "WATERMARK_OUT_OF_ORDERNESS", Duration.ofSeconds(15)),
                duration(environment, "WATERMARK_IDLENESS", Duration.ofSeconds(5)),
                duration(environment, "ALLOWED_LATENESS", Duration.ofSeconds(30)),
                duration(environment, "DEDUPLICATION_TTL", Duration.ofHours(24)),
                decimal(environment, "HIGH_VALUE_THRESHOLD", new BigDecimal("100000000.00")),
                positiveInt(environment, "VELOCITY_THRESHOLD", 10),
                positiveInt(environment, "STREAMING_PARALLELISM", 1)
        );
    }

    private static String value(Map<String, String> environment, String key, String fallback) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static Duration duration(Map<String, String> environment, String key, Duration fallback) {
        Duration duration = Duration.parse(value(environment, key, fallback.toString()));
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return duration;
    }

    private static BigDecimal decimal(Map<String, String> environment, String key, BigDecimal fallback) {
        BigDecimal value = new BigDecimal(value(environment, key, fallback.toPlainString()));
        if (value.signum() <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static int positiveInt(Map<String, String> environment, String key, int fallback) {
        int value = Integer.parseInt(value(environment, key, Integer.toString(fallback)));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static StartingOffsetsMode startingOffsetsMode(Map<String, String> environment) {
        String configured = value(
                environment,
                "STREAMING_STARTING_OFFSETS",
                StartingOffsetsMode.COMMITTED_OR_EARLIEST.name()
        );
        try {
            return StartingOffsetsMode.valueOf(configured.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "STREAMING_STARTING_OFFSETS must be COMMITTED_OR_EARLIEST or LATEST",
                    exception
            );
        }
    }

    public enum StartingOffsetsMode {
        COMMITTED_OR_EARLIEST,
        LATEST
    }
}
