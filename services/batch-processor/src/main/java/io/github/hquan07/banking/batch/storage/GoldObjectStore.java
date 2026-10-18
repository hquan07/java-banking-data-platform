package io.github.hquan07.banking.batch.storage;

import io.github.hquan07.banking.batch.model.PublishedObject;

import java.time.LocalDate;

public interface GoldObjectStore {

    void verifyBucket();

    PublishedObject publishDaily(
            LocalDate businessDate,
            byte[] content,
            long rowCount,
            String schemaVersion,
            String sourceFingerprint
    );

    void verify(PublishedObject object);

    String publishManifest(LocalDate businessDate, byte[] content);
}
