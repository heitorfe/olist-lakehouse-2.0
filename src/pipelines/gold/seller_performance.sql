-- =============================================================================
-- Gold Layer: Seller Performance (GMV by Seller)
-- =============================================================================
-- Description: Daily seller performance metrics including GMV and ratings
-- Sources: silver_order_items, silver_orders, silver_order_reviews, silver_sellers
-- Grain: One row per seller per day
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_seller_performance (
    CONSTRAINT positive_gmv
        EXPECT (gmv >= 0)
        ON VIOLATION FAIL UPDATE
)
COMMENT 'Daily seller performance metrics including GMV, order volume, and ratings'
TBLPROPERTIES (
    'quality' = 'gold'
)
CLUSTER BY AUTO
AS SELECT
    DATE(o.order_purchase_timestamp) AS order_date,
    oi.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_region,

    -- Volume metrics
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(1) AS total_items_sold,
    COUNT(DISTINCT oi.product_id) AS unique_products_sold,

    -- Revenue metrics (GMV = Gross Merchandise Value)
    SUM(oi.price) AS gmv,
    SUM(oi.freight_value) AS freight_collected,
    SUM(oi.total_item_value) AS total_revenue,
    AVG(oi.price) AS avg_item_price,

    -- Customer reach
    COUNT(DISTINCT o.customer_id) AS unique_customers,

    -- Review metrics (aggregated from order reviews)
    AVG(r.review_score) AS avg_review_score,
    COUNT(CASE WHEN r.sentiment = 'positive' THEN 1 END) AS positive_reviews,
    COUNT(CASE WHEN r.sentiment = 'negative' THEN 1 END) AS negative_reviews,

    current_timestamp() AS _refreshed_at

FROM silver_order_items oi
INNER JOIN silver_orders o ON oi.order_id = o.order_id
INNER JOIN silver_sellers s ON oi.seller_id = s.seller_id
LEFT JOIN silver_order_reviews r ON oi.order_id = r.order_id
WHERE DATE(o.order_purchase_timestamp) IS NOT NULL
GROUP BY
    DATE(o.order_purchase_timestamp),
    oi.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_region
ORDER BY order_date DESC, gmv DESC;
