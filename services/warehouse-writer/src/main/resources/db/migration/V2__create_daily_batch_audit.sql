CREATE TABLE batch_runs (
    batch_run_id UUID PRIMARY KEY,
    job_name VARCHAR(64) NOT NULL,
    business_date DATE NOT NULL,
    business_timezone VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL,
    source_row_count BIGINT,
    output_row_count INTEGER,
    source_currency_totals JSONB,
    output_currency_totals JSONB,
    source_fingerprint CHAR(64),
    gold_object_key VARCHAR(512),
    gold_object_sha256 CHAR(64),
    gold_object_size BIGINT,
    gold_schema_version VARCHAR(64),
    dq_checks JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    error_code VARCHAR(100),
    error_message VARCHAR(1024),
    CONSTRAINT ck_batch_run_status CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT ck_batch_run_source_count CHECK (source_row_count IS NULL OR source_row_count >= 0),
    CONSTRAINT ck_batch_run_output_count CHECK (output_row_count IS NULL OR output_row_count >= 0),
    CONSTRAINT ck_batch_run_source_hash CHECK (
        source_fingerprint IS NULL OR source_fingerprint ~ '^[a-f0-9]{64}$'
    ),
    CONSTRAINT ck_batch_run_object_hash CHECK (
        gold_object_sha256 IS NULL OR gold_object_sha256 ~ '^[a-f0-9]{64}$'
    ),
    CONSTRAINT ck_batch_run_timestamps CHECK (
        finished_at IS NULL OR finished_at >= started_at
    )
);

CREATE INDEX idx_batch_runs_job_date ON batch_runs(job_name, business_date, started_at DESC);
CREATE INDEX idx_batch_runs_unpublished ON batch_runs(business_date, finished_at DESC)
    WHERE status = 'SUCCEEDED' AND published_at IS NULL;
