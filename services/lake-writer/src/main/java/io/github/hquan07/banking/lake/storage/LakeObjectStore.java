package io.github.hquan07.banking.lake.storage;

import io.github.hquan07.banking.lake.model.LakeObjectDescriptor;

import java.util.List;

public interface LakeObjectStore {

    void verifyBucket();

    LakeObjectDescriptor putIfAbsent(
            String key,
            byte[] content,
            String contentType,
            String contentEncoding,
            long rowCount,
            String schemaVersion
    );

    byte[] read(String key);

    List<String> list(String prefix);
}
