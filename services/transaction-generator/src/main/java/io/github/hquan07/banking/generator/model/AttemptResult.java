package io.github.hquan07.banking.generator.model;

public record AttemptResult(
        GeneratorScenario scenario,
        int httpStatus,
        long latencyNanos,
        String failureType
) {

    public boolean transportFailure() {
        return failureType != null;
    }

    public boolean accepted() {
        return httpStatus >= 200 && httpStatus < 300;
    }
}
