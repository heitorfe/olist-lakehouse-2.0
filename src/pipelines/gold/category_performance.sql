-- =============================================================================
-- Gold Layer: Category Performance (GMV by Product Category)
-- =============================================================================
-- Description: Monthly product category performance metrics
-- Sources: silver_order_items, silver_orders, silver_products
-- Grain: One row per category per month
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.gold_category_performance (
    CONSTRAINT positive_gmv
        EXPECT (gmv >= 0)
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Monthly product category performance including GMV and item metrics'
TBLPROPERTIES (
    'quality' = 'gold'
)
CLUSTER BY AUTO
AS SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
    COALESCE(p.product_category_name, 'uncategorized') AS product_category,

    -- Volume metrics
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(1) AS total_items_sold,
    COUNT(DISTINCT oi.product_id) AS unique_products,
    COUNT(DISTINCT oi.seller_id) AS unique_sellers,

    -- Revenue metrics
    SUM(oi.price) AS gmv,
    SUM(oi.freight_value) AS freight_revenue,
    SUM(oi.total_item_value) AS total_revenue,
    AVG(oi.price) AS avg_item_price,

    -- Product characteristics
    AVG(p.product_weight_kg) AS avg_product_weight_kg,
    AVG(p.product_volume_cm3) AS avg_product_volume_cm3,

    -- Size distribution
    COUNT(CASE WHEN p.size_category = 'small' THEN 1 END) AS small_items,
    COUNT(CASE WHEN p.size_category = 'medium' THEN 1 END) AS medium_items,
    COUNT(CASE WHEN p.size_category = 'large' THEN 1 END) AS large_items,
    COUNT(CASE WHEN p.size_category = 'extra_large' THEN 1 END) AS extra_large_items,

    -- Review metrics
    AVG(r.review_score) AS avg_review_score,

    -- Month-over-month comparison
    LAG(SUM(oi.price)) OVER (PARTITION BY COALESCE(p.product_category_name, 'uncategorized') ORDER BY DATE_TRUNC('month', o.order_purchase_timestamp)) AS prev_month_gmv,

    current_timestamp() AS _refreshed_at

FROM ${catalog}.silver.silver_order_items oi
INNER JOIN ${catalog}.silver.silver_orders o ON oi.order_id = o.order_id
LEFT JOIN ${catalog}.silver.silver_products p ON oi.product_id = p.product_id
LEFT JOIN ${catalog}.silver.silver_order_reviews r ON oi.order_id = r.order_id
WHERE DATE_TRUNC('month', o.order_purchase_timestamp) IS NOT NULL
GROUP BY
    DATE_TRUNC('month', o.order_purchase_timestamp),
    COALESCE(p.product_category_name, 'uncategorized')
ORDER BY order_month DESC, gmv DESC;
