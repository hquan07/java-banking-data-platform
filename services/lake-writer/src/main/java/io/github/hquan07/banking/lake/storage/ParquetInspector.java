package io.github.hquan07.banking.lake.storage;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class ParquetInspector {

    public long rowCount(byte[] content) {
        Path temporary = null;
        try {
            temporary = Files.createTempFile("banking-lake-verify-", ".parquet");
            Files.write(temporary, content);
            long count = 0;
            HadoopInputFile inputFile = HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(temporary.toUri()), new Configuration(false)
            );
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                while (reader.read() != null) {
                    count++;
                }
            }
            return count;
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to read Parquet object", exception);
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
}
