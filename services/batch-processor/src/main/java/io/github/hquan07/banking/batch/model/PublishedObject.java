package io.github.hquan07.banking.batch.model;

public record PublishedObject(
        String key,
        String sha256,
        long size,
        long rowCount,
        String schemaVersion
) {
}
