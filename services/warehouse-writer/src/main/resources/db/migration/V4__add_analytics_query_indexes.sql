CREATE INDEX idx_fact_fraud_detected_alert
    ON fact_fraud_alerts(detected_at DESC, alert_id DESC);

CREATE INDEX idx_fact_fraud_account_detected
    ON fact_fraud_alerts(account_key, detected_at DESC);
