package io.github.hquan07.banking.batch.storage;

import java.security.MessageDigest;
import java.util.HexFormat;

public final class ContentHashes {

    private ContentHashes() {
    }

    public static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to calculate SHA-256", exception);
        }
    }
}
