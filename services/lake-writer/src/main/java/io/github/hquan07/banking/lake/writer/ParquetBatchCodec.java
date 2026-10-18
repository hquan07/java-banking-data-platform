package io.github.hquan07.banking.lake.writer;

import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.lake.model.DailyAggregatePartial;
import io.github.hquan07.banking.lake.model.KafkaEnvelope;
import io.github.hquan07.banking.lake.model.ValidatedLakeEvent;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

@Component
public class ParquetBatchCodec {

    public static final String SILVER_SCHEMA_VERSION = "silver-transaction-v1";
    public static final String GOLD_SCHEMA_VERSION = "gold-daily-partial-v1";

    static final Schema SILVER_SCHEMA = new Schema.Parser().parse("""
            {
              "type":"record","name":"SilverTransactionV1","namespace":"io.github.hquan07.banking.lake.avro",
              "fields":[
                {"name":"event_id","type":"string"},
                {"name":"transaction_id","type":"string"},
                {"name":"account_id","type":"string"},
                {"name":"transaction_type","type":"string"},
                {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":19,"scale":2}},
                {"name":"currency","type":"string"},
                {"name":"status","type":"string"},
                {"name":"occurred_at","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"produced_at","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"correlation_id","type":["null","string"],"default":null},
                {"name":"source_topic","type":"string"},
                {"name":"source_partition","type":"int"},
                {"name":"source_offset","type":"long"},
                {"name":"source_timestamp","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"payload_sha256","type":"string"},
                {"name":"lake_topic","type":"string"},
                {"name":"lake_partition","type":"int"},
                {"name":"lake_offset","type":"long"},
                {"name":"lake_timestamp","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"schema_version","type":"string"}
              ]
            }
            """);

    static final Schema GOLD_SCHEMA = new Schema.Parser().parse("""
            {
              "type":"record","name":"GoldDailyTransactionPartialV1","namespace":"io.github.hquan07.banking.lake.avro",
              "fields":[
                {"name":"report_date","type":{"type":"int","logicalType":"date"}},
                {"name":"account_id","type":"string"},
                {"name":"currency","type":"string"},
                {"name":"transaction_count","type":"long"},
                {"name":"total_amount","type":{"type":"bytes","logicalType":"decimal","precision":19,"scale":2}},
                {"name":"success_count","type":"long"},
                {"name":"failed_count","type":"long"},
                {"name":"pending_count","type":"long"},
                {"name":"reversed_count","type":"long"},
                {"name":"batch_id","type":"string"},
                {"name":"is_partial","type":"boolean","default":true},
                {"name":"generated_at","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"schema_version","type":"string"}
              ]
            }
            """);

    public byte[] writeSilver(List<CuratedRecord> records) {
        return write(SILVER_SCHEMA, records.stream().map(this::silverRecord).toList());
    }

    public byte[] writeGold(List<DailyAggregatePartial> rows, Instant generatedAt) {
        return write(GOLD_SCHEMA, rows.stream().map(row -> goldRecord(row, generatedAt)).toList());
    }

    private GenericRecord silverRecord(CuratedRecord record) {
        ValidatedLakeEvent validated = record.validated();
        TransactionCreatedEvent event = validated.event();
        GenericRecord row = new GenericData.Record(SILVER_SCHEMA);
        row.put("event_id", event.eventId().toString());
        row.put("transaction_id", event.transactionId().toString());
        row.put("account_id", event.accountId());
        row.put("transaction_type", event.transactionType().name());
        row.put("amount", decimal(event.amount(), SILVER_SCHEMA.getField("amount").schema()));
        row.put("currency", event.currency());
        row.put("status", event.status().name());
        row.put("occurred_at", event.occurredAt().toEpochMilli());
        row.put("produced_at", event.producedAt().toEpochMilli());
        row.put("correlation_id", event.correlationId());
        row.put("source_topic", validated.sourceTopic());
        row.put("source_partition", validated.sourcePartition());
        row.put("source_offset", validated.sourceOffset());
        row.put("source_timestamp", validated.sourceTimestamp());
        row.put("payload_sha256", validated.payloadHash());
        row.put("lake_topic", record.envelope().topic());
        row.put("lake_partition", record.envelope().partition());
        row.put("lake_offset", record.envelope().offset());
        row.put("lake_timestamp", record.envelope().timestamp());
        row.put("schema_version", SILVER_SCHEMA_VERSION);
        return row;
    }

    private GenericRecord goldRecord(DailyAggregatePartial aggregate, Instant generatedAt) {
        GenericRecord row = new GenericData.Record(GOLD_SCHEMA);
        row.put("report_date", Math.toIntExact(aggregate.reportDate().toEpochDay()));
        row.put("account_id", aggregate.accountId());
        row.put("currency", aggregate.currency());
        row.put("transaction_count", aggregate.transactionCount());
        row.put("total_amount", decimal(aggregate.totalAmount(), GOLD_SCHEMA.getField("total_amount").schema()));
        row.put("success_count", aggregate.successCount());
        row.put("failed_count", aggregate.failedCount());
        row.put("pending_count", aggregate.pendingCount());
        row.put("reversed_count", aggregate.reversedCount());
        row.put("batch_id", aggregate.batchId());
        row.put("is_partial", true);
        row.put("generated_at", generatedAt.toEpochMilli());
        row.put("schema_version", GOLD_SCHEMA_VERSION);
        return row;
    }

    private ByteBuffer decimal(BigDecimal value, Schema schema) {
        LogicalTypes.Decimal type = (LogicalTypes.Decimal) schema.getLogicalType();
        return new Conversions.DecimalConversion().toBytes(value.setScale(type.getScale()), schema, type);
    }

    private byte[] write(Schema schema, List<GenericRecord> rows) {
        Path temporary = null;
        try {
            temporary = Files.createTempFile("banking-lake-", ".parquet");
            Files.delete(temporary);
            Configuration configuration = new Configuration(false);
            HadoopOutputFile outputFile = HadoopOutputFile.fromPath(
                    new org.apache.hadoop.fs.Path(temporary.toUri()), configuration
            );
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(configuration)
                    .build()) {
                for (GenericRecord row : rows) {
                    writer.write(row);
                }
            }
            return Files.readAllBytes(temporary);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to encode Parquet micro-batch", exception);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (Exception ignored) {
                    // The OS will reclaim the process-scoped temporary directory if cleanup fails.
                }
            }
        }
    }

    public record CuratedRecord(KafkaEnvelope envelope, ValidatedLakeEvent validated) {
    }
}
