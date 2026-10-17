package io.github.hquan07.banking.streaming;

import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.common.event.FraudAlertEvent;
import io.github.hquan07.banking.common.event.TransactionAggregateEvent;
import io.github.hquan07.banking.streaming.config.StreamingConfig;
import io.github.hquan07.banking.streaming.model.RawKafkaRecord;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import io.github.hquan07.banking.streaming.processing.AggregateWindowFunction;
import io.github.hquan07.banking.streaming.processing.DeduplicateByEventId;
import io.github.hquan07.banking.streaming.processing.HighValueFraudFunction;
import io.github.hquan07.banking.streaming.processing.LateEventProcessFunction;
import io.github.hquan07.banking.streaming.processing.VelocityFraudWindowFunction;
import io.github.hquan07.banking.streaming.serde.JsonKafkaRecordSerializer;
import io.github.hquan07.banking.streaming.serde.KeyExtractor;
import io.github.hquan07.banking.streaming.serde.RawKafkaRecordDeserializer;
import io.github.hquan07.banking.streaming.validation.ValidationProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.time.Duration;

public final class StreamingProcessorJob {

    public static final String JOB_NAME = "banking-phase5-streaming";

    private StreamingProcessorJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamingConfig config = StreamingConfig.fromEnvironment();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        configureRuntime(environment, config);

        KafkaSource<RawKafkaRecord> source = KafkaSource.<RawKafkaRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.rawTopic())
                .setGroupId(config.groupId())
                .setStartingOffsets(startingOffsets(config))
                .setDeserializer(new RawKafkaRecordDeserializer())
                .setProperty("isolation.level", "read_committed")
                .build();

        SingleOutputStreamOperator<ValidatedTransaction> validation = environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-kafka-source")
                .uid("raw-kafka-source")
                .process(new ValidationProcessFunction())
                .name("validate-transactions")
                .uid("validate-transactions");

        DataStream<DeadLetterEvent> invalid = validation.getSideOutput(ValidationProcessFunction.INVALID);
        DataStream<DeadLetterEvent> dlq = validation.getSideOutput(ValidationProcessFunction.DLQ);

        SingleOutputStreamOperator<ValidatedTransaction> deduplicated = validation
                .keyBy(transaction -> transaction.event().eventId().toString())
                .process(new DeduplicateByEventId(config.deduplicationTtl()))
                .name("deduplicate-event-id")
                .uid("deduplicate-event-id");

        WatermarkStrategy<ValidatedTransaction> watermarks = WatermarkStrategy
                .<ValidatedTransaction>forBoundedOutOfOrderness(config.watermarkOutOfOrderness())
                .withTimestampAssigner((transaction, previousTimestamp) ->
                        transaction.event().occurredAt().toEpochMilli())
                .withIdleness(config.watermarkIdleness());

        SingleOutputStreamOperator<ValidatedTransaction> onTime = deduplicated
                .assignTimestampsAndWatermarks(watermarks)
                .name("event-time-watermarks")
                .uid("event-time-watermarks")
                .process(new LateEventProcessFunction(config.allowedLateness()))
                .name("route-too-late-events")
                .uid("route-too-late-events");

        DataStream<DeadLetterEvent> late = onTime.getSideOutput(LateEventProcessFunction.LATE_EVENTS);

        onTime
                .map(ValidatedTransaction::event)
                .name("validated-event-payload")
                .uid("validated-event-payload")
                .sinkTo(kafkaSink(
                        config,
                        config.validatedTopic(),
                        "validated",
                        event -> event.accountId()
                ))
                .name("validated-kafka-sink")
                .uid("validated-kafka-sink");

        invalid.union(late)
                .sinkTo(kafkaSink(
                        config,
                        config.invalidTopic(),
                        "invalid",
                        StreamingProcessorJob::failureKey
                ))
                .name("invalid-kafka-sink")
                .uid("invalid-kafka-sink");

        dlq.sinkTo(kafkaSink(
                        config,
                        config.dlqTopic(),
                        "dlq",
                        StreamingProcessorJob::failureKey
                ))
                .name("dlq-kafka-sink")
                .uid("dlq-kafka-sink");

        SingleOutputStreamOperator<FraudAlertEvent> highValueAlerts = onTime
                .process(new HighValueFraudFunction(config.highValueThreshold()))
                .name("high-value-rule")
                .uid("high-value-rule");

        SingleOutputStreamOperator<FraudAlertEvent> velocityAlerts = onTime
                .keyBy(transaction -> transaction.event().accountId())
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(60), Duration.ofSeconds(10)))
                .allowedLateness(config.allowedLateness())
                .process(new VelocityFraudWindowFunction(config.velocityThreshold()))
                .name("velocity-60s-rule")
                .uid("velocity-60s-rule");

        highValueAlerts.union(velocityAlerts)
                .sinkTo(kafkaSink(
                        config,
                        config.fraudAlertsTopic(),
                        "fraud-alerts",
                        FraudAlertEvent::accountId
                ))
                .name("fraud-alerts-kafka-sink")
                .uid("fraud-alerts-kafka-sink");

        onTime
                .keyBy(transaction -> transaction.event().currency())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(config.allowedLateness())
                .process(new AggregateWindowFunction())
                .name("one-minute-aggregates")
                .uid("one-minute-aggregates")
                .sinkTo(kafkaSink(
                        config,
                        config.aggregatesTopic(),
                        "aggregates",
                        TransactionAggregateEvent::windowKey
                ))
                .name("aggregates-kafka-sink")
                .uid("aggregates-kafka-sink");

        environment.execute(JOB_NAME);
    }

    static void configureRuntime(StreamExecutionEnvironment environment, StreamingConfig config) {
        Configuration runtime = new Configuration();
        runtime.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointDirectory());
        runtime.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        runtime.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        runtime.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
        environment.configure(runtime);
        environment.setParallelism(config.parallelism());
        environment.enableCheckpointing(config.checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = environment.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(config.checkpointInterval().toMillis());
        checkpoints.setCheckpointTimeout(Duration.ofMinutes(2).toMillis());
        checkpoints.setMaxConcurrentCheckpoints(1);
        checkpoints.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    private static String failureKey(DeadLetterEvent failure) {
        return failure.eventId() == null ? failure.payloadHash() : failure.eventId().toString();
    }

    private static OffsetsInitializer startingOffsets(StreamingConfig config) {
        return switch (config.startingOffsetsMode()) {
            case COMMITTED_OR_EARLIEST -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case LATEST -> OffsetsInitializer.latest();
        };
    }

    private static <T> KafkaSink<T> kafkaSink(
            StreamingConfig config,
            String topic,
            String transactionSuffix,
            KeyExtractor<T> keyExtractor
    ) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new JsonKafkaRecordSerializer<>(topic, keyExtractor))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("banking-phase5-" + transactionSuffix + "-")
                .setProperty("transaction.timeout.ms", "900000")
                .build();
    }
}
