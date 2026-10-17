package io.github.hquan07.banking.streaming.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamingConfigTest {

    @Test
    void appliesSafeDefaultsAndParsesOverrides() {
        StreamingConfig defaults = StreamingConfig.from(Map.of());
        StreamingConfig overridden = StreamingConfig.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "CHECKPOINT_INTERVAL", "PT10S",
                "WATERMARK_IDLENESS", "PT3S",
                "STREAMING_STARTING_OFFSETS", "latest",
                "STREAMING_PARALLELISM", "2"
        ));

        assertEquals("kafka:9092", defaults.bootstrapServers());
        assertEquals(Duration.ofSeconds(15), defaults.watermarkOutOfOrderness());
        assertEquals(Duration.ofSeconds(5), defaults.watermarkIdleness());
        assertEquals(StreamingConfig.StartingOffsetsMode.COMMITTED_OR_EARLIEST, defaults.startingOffsetsMode());
        assertEquals("localhost:9092", overridden.bootstrapServers());
        assertEquals(Duration.ofSeconds(10), overridden.checkpointInterval());
        assertEquals(Duration.ofSeconds(3), overridden.watermarkIdleness());
        assertEquals(StreamingConfig.StartingOffsetsMode.LATEST, overridden.startingOffsetsMode());
        assertEquals(2, overridden.parallelism());
    }

    @Test
    void rejectsNonPositiveDurationsAndParallelism() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamingConfig.from(Map.of("CHECKPOINT_INTERVAL", "PT0S")));
        assertThrows(IllegalArgumentException.class,
                () -> StreamingConfig.from(Map.of("WATERMARK_IDLENESS", "PT0S")));
        assertThrows(IllegalArgumentException.class,
                () -> StreamingConfig.from(Map.of("STREAMING_PARALLELISM", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> StreamingConfig.from(Map.of("STREAMING_STARTING_OFFSETS", "middle")));
    }
}
