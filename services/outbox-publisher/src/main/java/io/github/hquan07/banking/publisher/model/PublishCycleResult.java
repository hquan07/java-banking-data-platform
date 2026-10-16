package io.github.hquan07.banking.publisher.model;

public record PublishCycleResult(int claimed, int published, int failed) {

    public static PublishCycleResult empty() {
        return new PublishCycleResult(0, 0, 0);
    }
}
