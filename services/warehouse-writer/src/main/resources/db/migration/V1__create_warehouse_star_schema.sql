CREATE TABLE dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    customer_status VARCHAR(32) NOT NULL DEFAULT 'UNKNOWN',
    is_inferred BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dim_customer (customer_id, customer_status, is_inferred)
VALUES ('UNKNOWN', 'UNKNOWN', TRUE);

CREATE TABLE dim_account (
    account_key BIGSERIAL PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL UNIQUE,
    customer_key BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    currency VARCHAR(3) NOT NULL,
    account_status VARCHAR(32) NOT NULL DEFAULT 'UNKNOWN',
    is_inferred BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_dim_account_currency CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    calendar_year SMALLINT NOT NULL,
    calendar_quarter SMALLINT NOT NULL,
    calendar_month SMALLINT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE fact_transactions (
    transaction_id UUID PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    account_key BIGINT NOT NULL REFERENCES dim_account(account_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    transaction_type VARCHAR(32) NOT NULL,
    amount NUMERIC(19,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    correlation_id VARCHAR(128),
    schema_version INTEGER NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    source_timestamp TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_fact_transactions_amount CHECK (amount > 0),
    CONSTRAINT ck_fact_transactions_currency CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_fact_transactions_source CHECK (source_partition >= 0 AND source_offset >= 0)
);

CREATE INDEX idx_fact_transactions_account_date ON fact_transactions(account_key, date_key);
CREATE INDEX idx_fact_transactions_occurred_at ON fact_transactions(occurred_at);

CREATE TABLE fact_fraud_alerts (
    alert_id UUID PRIMARY KEY,
    event_id UUID,
    transaction_id UUID REFERENCES fact_transactions(transaction_id),
    account_key BIGINT NOT NULL REFERENCES dim_account(account_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    rule_code VARCHAR(64) NOT NULL,
    rule_version VARCHAR(32) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    amount NUMERIC(19,2),
    currency VARCHAR(3) NOT NULL,
    observed_count BIGINT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fact_fraud_rule_window UNIQUE (event_id, rule_code, window_start, window_end),
    CONSTRAINT ck_fact_fraud_window CHECK (window_end > window_start),
    CONSTRAINT ck_fact_fraud_count CHECK (observed_count > 0),
    CONSTRAINT ck_fact_fraud_amount CHECK (amount IS NULL OR amount > 0),
    CONSTRAINT ck_fact_fraud_currency CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE INDEX idx_fact_fraud_detected_at ON fact_fraud_alerts(detected_at);

CREATE TABLE agg_transaction_windows (
    aggregate_id UUID PRIMARY KEY,
    window_key VARCHAR(128) NOT NULL UNIQUE,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount NUMERIC(24,2) NOT NULL,
    failed_count BIGINT NOT NULL,
    schema_version INTEGER NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_agg_window CHECK (window_end > window_start),
    CONSTRAINT ck_agg_window_counts CHECK (
        transaction_count > 0 AND failed_count >= 0 AND failed_count <= transaction_count
    ),
    CONSTRAINT ck_agg_window_amount CHECK (total_amount > 0),
    CONSTRAINT ck_agg_window_currency CHECK (currency ~ '^[A-Z]{3}$')
);

-- Phase 8 is the only writer of this authoritative daily table.
CREATE TABLE agg_daily_transactions (
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    currency VARCHAR(3) NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_amount NUMERIC(24,2) NOT NULL,
    failed_count BIGINT NOT NULL,
    batch_run_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_key, currency),
    CONSTRAINT ck_agg_daily_counts CHECK (
        transaction_count >= 0 AND failed_count >= 0 AND failed_count <= transaction_count
    ),
    CONSTRAINT ck_agg_daily_amount CHECK (total_amount >= 0),
    CONSTRAINT ck_agg_daily_currency CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE processed_kafka_records (
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    record_type VARCHAR(32) NOT NULL,
    event_identifier VARCHAR(128),
    outcome VARCHAR(16) NOT NULL DEFAULT 'PROCESSING',
    payload_hash CHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_topic, source_partition, source_offset),
    CONSTRAINT ck_processed_source CHECK (source_partition >= 0 AND source_offset >= 0),
    CONSTRAINT ck_processed_outcome CHECK (outcome IN ('PROCESSING', 'ACCEPTED', 'REJECTED')),
    CONSTRAINT ck_processed_hash CHECK (payload_hash ~ '^[a-f0-9]{64}$')
);

CREATE TABLE warehouse_rejections (
    rejection_id UUID PRIMARY KEY,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    failure_code VARCHAR(100) NOT NULL,
    failure_message VARCHAR(1024) NOT NULL,
    raw_payload BYTEA NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE TABLE warehouse_dlq_outbox (
    failure_id UUID PRIMARY KEY,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    message_key VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error VARCHAR(1024),
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX idx_warehouse_dlq_pending ON warehouse_dlq_outbox(created_at) WHERE published_at IS NULL;

CREATE TABLE warehouse_reconciliation_runs (
    run_id UUID PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_offsets JSONB NOT NULL,
    raw_count INTEGER NOT NULL,
    valid_count INTEGER NOT NULL,
    invalid_count INTEGER NOT NULL,
    unique_count INTEGER NOT NULL,
    inserted_count INTEGER NOT NULL,
    updated_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    duplicate_delivery_count INTEGER NOT NULL,
    currency_totals JSONB NOT NULL,
    warehouse_currency_totals JSONB NOT NULL,
    amount_deviation JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_reconciliation_counts CHECK (
        raw_count >= 0 AND valid_count >= 0 AND invalid_count >= 0 AND unique_count >= 0
        AND inserted_count >= 0 AND updated_count >= 0 AND rejected_count >= 0
        AND duplicate_delivery_count >= 0 AND raw_count = valid_count + invalid_count + duplicate_delivery_count
    )
);
