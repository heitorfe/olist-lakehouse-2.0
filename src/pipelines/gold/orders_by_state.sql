-- =============================================================================
-- Gold Layer: Orders by Customer State
-- =============================================================================
-- Description: Geographic distribution of orders by customer state
-- Sources: silver_orders_enriched, silver_customers
-- Grain: One row per state per day
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.gold_orders_by_state (
    CONSTRAINT positive_revenue
        EXPECT (total_revenue >= 0)
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Daily order metrics by customer state for geographic analysis'
TBLPROPERTIES (
    'quality' = 'gold'
)
CLUSTER BY AUTO
AS SELECT
    o.order_date,
    c.customer_state,
    CASE c.customer_state
        WHEN 'SP' THEN 'Southeast'
        WHEN 'RJ' THEN 'Southeast'
        WHEN 'MG' THEN 'Southeast'
        WHEN 'ES' THEN 'Southeast'
        WHEN 'PR' THEN 'South'
        WHEN 'SC' THEN 'South'
        WHEN 'RS' THEN 'South'
        WHEN 'MS' THEN 'Midwest'
        WHEN 'MT' THEN 'Midwest'
        WHEN 'GO' THEN 'Midwest'
        WHEN 'DF' THEN 'Midwest'
        WHEN 'BA' THEN 'Northeast'
        WHEN 'SE' THEN 'Northeast'
        WHEN 'AL' THEN 'Northeast'
        WHEN 'PE' THEN 'Northeast'
        WHEN 'PB' THEN 'Northeast'
        WHEN 'RN' THEN 'Northeast'
        WHEN 'CE' THEN 'Northeast'
        WHEN 'PI' THEN 'Northeast'
        WHEN 'MA' THEN 'Northeast'
        ELSE 'Other'
    END AS customer_region,

    -- Volume metrics
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.is_delivered THEN 1 END) AS delivered_orders,

    -- Revenue metrics
    SUM(o.total_order_value) AS total_revenue,
    AVG(o.total_order_value) AS avg_order_value,

    -- Customer metrics
    COUNT(DISTINCT o.customer_id) AS unique_customers,

    -- Review metrics
    AVG(o.review_score) AS avg_review_score,

    -- Delivery metrics
    AVG(o.delivery_days) AS avg_delivery_days,

    current_timestamp() AS _refreshed_at

FROM ${catalog}.silver.silver_orders_enriched o
INNER JOIN ${catalog}.silver.silver_customers c ON o.customer_id = c.customer_id
WHERE o.order_date IS NOT NULL
GROUP BY o.order_date, c.customer_state
ORDER BY o.order_date DESC, total_revenue DESC;
