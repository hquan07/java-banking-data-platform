package io.github.hquan07.banking.lake.model;

public record LakeObjectDescriptor(
        String key,
        String contentType,
        String contentEncoding,
        long size,
        String sha256,
        long rowCount,
        String schemaVersion
) {
}
