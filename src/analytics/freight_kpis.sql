-- Business questions answered from the gold layer.
-- Run on a SQL warehouse after `USE CATALOG logistics_dev;`

-- 1. On-time delivery % by mode, last 30 days
SELECT transport_mode,
       SUM(shipments_delivered)                                             AS delivered,
       ROUND(SUM(on_time_pct * shipments_delivered) / SUM(shipments_delivered), 2) AS on_time_pct,
       ROUND(AVG(avg_transit_hours), 1)                                     AS avg_transit_hours
FROM gold.daily_operations_kpis
WHERE delivery_date >= current_date() - INTERVAL 30 DAYS
GROUP BY transport_mode
ORDER BY transport_mode;

-- 2. Shipments currently late and still moving (operations exception list)
SELECT shipment_id, house_bill_number, customer_id, transport_mode, origin_code, destination_code,
       shipment_status, planned_delivery_ts, last_event_ts, exception_count
FROM gold.shipment_status
WHERE is_overdue_in_transit
ORDER BY planned_delivery_ts;

-- 3. Top 10 lanes by gross margin
SELECT transport_mode, origin_code, destination_code, shipments, on_time_pct,
       revenue_usd, gross_margin_usd, ROUND(100 * gross_margin_usd / revenue_usd, 1) AS margin_pct,
       revenue_per_kg
FROM gold.lane_performance
WHERE revenue_usd > 0
ORDER BY gross_margin_usd DESC
LIMIT 10;

-- 4. Carrier scorecard (procurement review)
SELECT carrier_name, transport_mode, contract_type, shipments, on_time_pct,
       exception_rate_pct, avg_transit_hours, carrier_spend_usd, cost_per_kg
FROM gold.carrier_scorecard
ORDER BY transport_mode, on_time_pct DESC;

-- 5. Least profitable strategic / key accounts
SELECT customer_name, account_tier, industry, shipments, revenue_usd, gross_margin_pct, on_time_pct
FROM gold.customer_profitability
WHERE account_tier IN ('STRATEGIC', 'KEY') AND revenue_usd IS NOT NULL
ORDER BY gross_margin_pct
LIMIT 15;

-- 6. Customs dwell time by import country
SELECT import_country,
       SUM(entries)                                                        AS entries,
       ROUND(SUM(avg_clearance_hours * entries) / SUM(entries), 1)         AS avg_clearance_hours,
       MAX(p90_clearance_hours)                                            AS worst_broker_p90_hours,
       SUM(duties_usd)                                                     AS duties_usd
FROM gold.customs_clearance_kpis
GROUP BY import_country
ORDER BY avg_clearance_hours DESC;

-- 7. Accounts receivable aging
SELECT aging_bucket, SUM(open_invoices) AS open_invoices, SUM(open_amount_usd) AS open_amount_usd
FROM gold.ar_aging
GROUP BY aging_bucket
ORDER BY CASE aging_bucket WHEN 'CURRENT' THEN 0 WHEN '1-30' THEN 1 WHEN '31-60' THEN 2
                           WHEN '61-90' THEN 3 ELSE 4 END;

-- 8. Customer tier history (SCD2)
SELECT customer_id, customer_name, account_tier, credit_terms_days, __START_AT, __END_AT
FROM silver.customers
WHERE customer_id IN (SELECT customer_id FROM silver.customers GROUP BY customer_id HAVING COUNT(*) > 1)
ORDER BY customer_id, __START_AT;
