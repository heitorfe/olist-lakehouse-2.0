-- =============================================================================
-- Gold Layer: Orders by Customer State
-- =============================================================================
-- Description: Geographic distribution of orders by customer state
-- Sources: silver_orders, silver_order_items, silver_customers, silver_order_reviews
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
AS
WITH order_totals AS (
    SELECT
        order_id,
        SUM(total_item_value) AS total_order_value
    FROM ${catalog}.silver.silver_order_items
    GROUP BY order_id
)
SELECT
    DATE(o.order_purchase_timestamp) AS order_date,
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
    COUNT(CASE WHEN o.order_status = 'delivered' THEN 1 END) AS delivered_orders,

    -- Revenue metrics
    SUM(COALESCE(ot.total_order_value, 0)) AS total_revenue,
    AVG(COALESCE(ot.total_order_value, 0)) AS avg_order_value,

    -- Customer metrics
    COUNT(DISTINCT o.customer_id) AS unique_customers,

    -- Review metrics
    AVG(r.review_score) AS avg_review_score,

    -- Delivery metrics
    AVG(
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
            THEN DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)
            ELSE NULL
        END
    ) AS avg_delivery_days,

    current_timestamp() AS _refreshed_at

FROM ${catalog}.silver.silver_orders o
INNER JOIN ${catalog}.silver.silver_customers c ON o.customer_id = c.customer_id
LEFT JOIN order_totals ot ON o.order_id = ot.order_id
LEFT JOIN ${catalog}.silver.silver_order_reviews r ON o.order_id = r.order_id
WHERE DATE(o.order_purchase_timestamp) IS NOT NULL
GROUP BY DATE(o.order_purchase_timestamp), c.customer_state
ORDER BY order_date DESC, total_revenue DESC;
