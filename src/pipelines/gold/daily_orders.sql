-- =============================================================================
-- Gold Layer: Daily Orders KPIs
-- =============================================================================
-- Description: Daily aggregated order metrics for business intelligence
-- Source: silver_orders_enriched
-- Grain: One row per day
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_orders (
    -- Quality gate: Ensure revenue is non-negative
    CONSTRAINT positive_revenue
        EXPECT (total_revenue >= 0)
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Daily order KPIs including revenue, volume, and delivery metrics'
TBLPROPERTIES (
    'quality' = 'gold'
)
CLUSTER BY AUTO
AS SELECT
    order_date,

    -- Volume metrics
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN is_delivered THEN 1 END) AS delivered_orders,
    COUNT(CASE WHEN is_canceled THEN 1 END) AS canceled_orders,
    COUNT(CASE WHEN delivered_early THEN 1 END) AS early_deliveries,
    COUNT(CASE WHEN delivered_late THEN 1 END) AS late_deliveries,

    -- Revenue metrics
    SUM(total_order_value) AS total_revenue,
    SUM(total_price) AS product_revenue,
    SUM(total_freight) AS freight_revenue,
    AVG(total_order_value) AS avg_order_value,

    -- Item metrics
    SUM(item_count) AS total_items,
    AVG(item_count) AS avg_items_per_order,
    SUM(unique_products) AS total_unique_products,

    -- Customer metrics
    COUNT(DISTINCT customer_id) AS unique_customers,

    -- Payment metrics
    AVG(max_installments) AS avg_installments,

    -- Review metrics
    AVG(review_score) AS avg_review_score,
    COUNT(CASE WHEN review_sentiment = 'positive' THEN 1 END) AS positive_reviews,
    COUNT(CASE WHEN review_sentiment = 'negative' THEN 1 END) AS negative_reviews,

    -- Delivery metrics
    AVG(delivery_days) AS avg_delivery_days,
    AVG(delivery_vs_estimate_days) AS avg_delivery_vs_estimate,

    -- Rates
    ROUND(COUNT(CASE WHEN is_delivered THEN 1 END) * 100.0 / COUNT(*), 2) AS delivery_rate,
    ROUND(COUNT(CASE WHEN is_canceled THEN 1 END) * 100.0 / COUNT(*), 2) AS cancellation_rate,
    ROUND(COUNT(CASE WHEN delivered_late THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN is_delivered THEN 1 END), 0), 2) AS late_delivery_rate,

    current_timestamp() AS _refreshed_at

FROM silver_orders_enriched
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date DESC;
