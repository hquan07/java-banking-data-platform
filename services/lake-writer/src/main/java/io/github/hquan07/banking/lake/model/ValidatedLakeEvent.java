package io.github.hquan07.banking.lake.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;

@JsonIgnoreProperties(ignoreUnknown = false)
public record ValidatedLakeEvent(
        TransactionCreatedEvent event,
        @JsonProperty("sourceTopic") String sourceTopic,
        @JsonProperty("sourcePartition") int sourcePartition,
        @JsonProperty("sourceOffset") long sourceOffset,
        @JsonProperty("sourceTimestamp") long sourceTimestamp,
        @JsonProperty("payloadHash") String payloadHash
) {
}
