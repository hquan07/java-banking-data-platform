package io.github.hquan07.banking.warehouse.model;

import java.util.UUID;

public record PendingDlqRecord(UUID failureId, String messageKey, byte[] payload) {
}
