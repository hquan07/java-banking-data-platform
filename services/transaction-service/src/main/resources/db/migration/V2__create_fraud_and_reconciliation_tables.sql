CREATE TABLE fraud_alerts (
    alert_id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions (transaction_id),
    rule_code VARCHAR(64) NOT NULL,
    window_id VARCHAR(128) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fraud_alert_business_key
        UNIQUE (transaction_id, rule_code, window_id),
    CONSTRAINT chk_fraud_alerts_severity
        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'))
);

CREATE INDEX idx_fraud_alerts_detected_at
    ON fraud_alerts (detected_at DESC);

CREATE TABLE processing_runs (
    run_id UUID PRIMARY KEY,
    run_type VARCHAR(64) NOT NULL,
    source_name VARCHAR(128) NOT NULL,
    source_range_start VARCHAR(256),
    source_range_end VARCHAR(256),
    input_count BIGINT NOT NULL DEFAULT 0,
    output_count BIGINT NOT NULL DEFAULT 0,
    input_checksum CHAR(64),
    amount_sum NUMERIC(24, 2),
    status VARCHAR(16) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    CONSTRAINT chk_processing_runs_counts
        CHECK (input_count >= 0 AND output_count >= 0),
    CONSTRAINT chk_processing_runs_checksum
        CHECK (input_checksum IS NULL OR input_checksum ~ '^[a-f0-9]{64}$'),
    CONSTRAINT chk_processing_runs_status
        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT chk_processing_runs_finished_at
        CHECK (
            (status = 'RUNNING' AND finished_at IS NULL)
            OR (status IN ('SUCCEEDED', 'FAILED') AND finished_at IS NOT NULL)
        )
);

CREATE INDEX idx_processing_runs_type_started_at
    ON processing_runs (run_type, started_at DESC);
