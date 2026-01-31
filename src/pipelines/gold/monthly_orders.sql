-- =============================================================================
-- Gold Layer: Monthly Orders KPIs
-- =============================================================================
-- Description: Monthly aggregated order metrics for trend analysis
-- Source: silver_orders_enriched
-- Grain: One row per month
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.gold_monthly_orders (
    CONSTRAINT positive_revenue
        EXPECT (total_revenue >= 0)
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Monthly order KPIs for trend analysis and reporting'
TBLPROPERTIES (
    'quality' = 'gold'
)
CLUSTER BY AUTO
AS SELECT
    order_month,
    order_year,

    -- Volume metrics
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN is_delivered THEN 1 END) AS delivered_orders,
    COUNT(CASE WHEN is_canceled THEN 1 END) AS canceled_orders,

    -- Revenue metrics
    SUM(total_order_value) AS total_revenue,
    SUM(total_price) AS product_revenue,
    SUM(total_freight) AS freight_revenue,
    AVG(total_order_value) AS avg_order_value,

    -- Customer metrics
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_order_value) / NULLIF(COUNT(DISTINCT customer_id), 0) AS revenue_per_customer,

    -- Review metrics
    AVG(review_score) AS avg_review_score,
    ROUND(COUNT(CASE WHEN review_sentiment = 'positive' THEN 1 END) * 100.0 / NULLIF(COUNT(review_score), 0), 2) AS positive_review_rate,

    -- Operational metrics
    AVG(delivery_days) AS avg_delivery_days,
    ROUND(COUNT(CASE WHEN is_delivered THEN 1 END) * 100.0 / COUNT(*), 2) AS delivery_rate,
    ROUND(COUNT(CASE WHEN is_canceled THEN 1 END) * 100.0 / COUNT(*), 2) AS cancellation_rate,

    -- Month-over-month comparison placeholders
    LAG(SUM(total_order_value)) OVER (ORDER BY order_month) AS prev_month_revenue,
    LAG(COUNT(*)) OVER (ORDER BY order_month) AS prev_month_orders,

    current_timestamp() AS _refreshed_at

FROM ${catalog}.silver.silver_orders_enriched
WHERE order_month IS NOT NULL
GROUP BY order_month, order_year
ORDER BY order_month DESC;
