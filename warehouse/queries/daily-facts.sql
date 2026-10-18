-- Phase 7 operational roll-up. Phase 8 owns the authoritative daily table.
SELECT d.calendar_date, f.currency,
       COUNT(*) AS transaction_count,
       SUM(f.amount) AS total_amount,
       COUNT(*) FILTER (WHERE f.status = 'FAILED') AS failed_count
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.calendar_date, f.currency
ORDER BY d.calendar_date DESC, f.currency;
