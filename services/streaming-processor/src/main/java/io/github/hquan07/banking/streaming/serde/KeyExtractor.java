package io.github.hquan07.banking.streaming.serde;

import java.io.Serializable;

@FunctionalInterface
public interface KeyExtractor<T> extends Serializable {
    String key(T value);
}
