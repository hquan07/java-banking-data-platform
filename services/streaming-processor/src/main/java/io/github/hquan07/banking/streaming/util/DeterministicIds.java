package io.github.hquan07.banking.streaming.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class DeterministicIds {

    private DeterministicIds() {
    }

    public static UUID from(String namespace, String value) {
        return UUID.nameUUIDFromBytes((namespace + ":" + value).getBytes(StandardCharsets.UTF_8));
    }
}
