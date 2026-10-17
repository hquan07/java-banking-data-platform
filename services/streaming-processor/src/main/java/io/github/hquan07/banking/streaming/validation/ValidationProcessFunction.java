package io.github.hquan07.banking.streaming.validation;

import io.github.hquan07.banking.common.event.DeadLetterEvent;
import io.github.hquan07.banking.streaming.model.RawKafkaRecord;
import io.github.hquan07.banking.streaming.model.ValidatedTransaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class ValidationProcessFunction extends ProcessFunction<RawKafkaRecord, ValidatedTransaction> {

    public static final OutputTag<DeadLetterEvent> INVALID = new OutputTag<>("invalid-transactions") { };
    public static final OutputTag<DeadLetterEvent> DLQ = new OutputTag<>("validation-dlq") { };

    private final TransactionValidator validator = new TransactionValidator();

    @Override
    public void processElement(
            RawKafkaRecord record,
            ProcessFunction<RawKafkaRecord, ValidatedTransaction>.Context context,
            Collector<ValidatedTransaction> collector
    ) {
        ValidationOutcome outcome = validator.validate(record);
        switch (outcome.route()) {
            case VALIDATED -> collector.collect(outcome.transaction());
            case INVALID -> context.output(INVALID, outcome.failure());
            case DLQ -> context.output(DLQ, outcome.failure());
        }
    }
}
