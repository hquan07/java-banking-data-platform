ALTER TABLE agg_daily_transactions
    ADD CONSTRAINT fk_agg_daily_batch_run
    FOREIGN KEY (batch_run_id) REFERENCES batch_runs(batch_run_id);
