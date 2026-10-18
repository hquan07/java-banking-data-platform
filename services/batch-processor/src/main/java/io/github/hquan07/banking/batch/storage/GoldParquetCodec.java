package io.github.hquan07.banking.batch.storage;

import io.github.hquan07.banking.batch.model.DailyAggregate;
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
import java.util.List;

@Component
public class GoldParquetCodec {

    public static final String SCHEMA_VERSION = "gold-daily-final-v1";

    static final Schema SCHEMA = new Schema.Parser().parse("""
            {
              "type":"record","name":"GoldDailyTransactionFinalV1","namespace":"io.github.hquan07.banking.batch.avro",
              "fields":[
                {"name":"report_date","type":{"type":"int","logicalType":"date"}},
                {"name":"currency","type":"string"},
                {"name":"transaction_count","type":"long"},
                {"name":"total_amount","type":{"type":"bytes","logicalType":"decimal","precision":24,"scale":2}},
                {"name":"success_count","type":"long"},
                {"name":"failed_count","type":"long"},
                {"name":"pending_count","type":"long"},
                {"name":"reversed_count","type":"long"},
                {"name":"source_fingerprint","type":"string"},
                {"name":"is_partial","type":"boolean","default":false},
                {"name":"schema_version","type":"string"}
              ]
            }
            """);

    public byte[] write(List<DailyAggregate> rows, String sourceFingerprint) {
        Path temporary = null;
        try {
            temporary = Files.createTempFile("banking-gold-final-", ".parquet");
            Files.delete(temporary);
            Configuration configuration = new Configuration(false);
            HadoopOutputFile output = HadoopOutputFile.fromPath(
                    new org.apache.hadoop.fs.Path(temporary.toUri()), configuration
            );
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(output)
                    .withSchema(SCHEMA)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(configuration)
                    .build()) {
                for (DailyAggregate aggregate : rows) {
                    writer.write(record(aggregate, sourceFingerprint));
                }
            }
            return Files.readAllBytes(temporary);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to encode final daily Gold Parquet", exception);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (Exception ignored) {
                    // Temporary files are process-scoped and will be reclaimed by the operating system.
                }
            }
        }
    }

    private GenericRecord record(DailyAggregate aggregate, String sourceFingerprint) {
        GenericRecord row = new GenericData.Record(SCHEMA);
        row.put("report_date", Math.toIntExact(aggregate.businessDate().toEpochDay()));
        row.put("currency", aggregate.currency());
        row.put("transaction_count", aggregate.transactionCount());
        row.put("total_amount", decimal(aggregate.totalAmount()));
        row.put("success_count", aggregate.successCount());
        row.put("failed_count", aggregate.failedCount());
        row.put("pending_count", aggregate.pendingCount());
        row.put("reversed_count", aggregate.reversedCount());
        row.put("source_fingerprint", sourceFingerprint);
        row.put("is_partial", false);
        row.put("schema_version", SCHEMA_VERSION);
        return row;
    }

    private ByteBuffer decimal(BigDecimal value) {
        Schema schema = SCHEMA.getField("total_amount").schema();
        LogicalTypes.Decimal type = (LogicalTypes.Decimal) schema.getLogicalType();
        return new Conversions.DecimalConversion().toBytes(value.setScale(type.getScale()), schema, type);
    }
}
