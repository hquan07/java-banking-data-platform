package io.github.hquan07.banking.streaming.model;

import io.github.hquan07.banking.common.event.TransactionCreatedEvent;

import java.io.Serializable;

public record ValidatedTransaction(
        TransactionCreatedEvent event,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        long sourceTimestamp,
        String payloadHash
) implements Serializable {
}
