package io.github.hquan07.banking.streaming.serde;

import io.github.hquan07.banking.streaming.model.RawKafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public final class RawKafkaRecordDeserializer implements KafkaRecordDeserializationSchema<RawKafkaRecord> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawKafkaRecord> collector) {
        byte[] payload = record.value() == null ? new byte[0] : record.value();
        String key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
        collector.collect(new RawKafkaRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                key,
                payload
        ));
    }

    @Override
    public TypeInformation<RawKafkaRecord> getProducedType() {
        return TypeInformation.of(RawKafkaRecord.class);
    }
}
