-- =============================================================================
-- Silver Layer: Orders Enriched (Denormalized Order View)
-- =============================================================================
-- Description: Combines orders with items, payments, and reviews for analysis
-- Sources: silver_orders, silver_order_items, silver_order_payments, silver_order_reviews
-- Pattern: Materialized view joining fact and dimension data
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.silver_orders_enriched
COMMENT 'Enriched order data with aggregated items, payments, and review metrics'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS
WITH order_items_agg AS (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        COUNT(DISTINCT product_id) AS unique_products,
        COUNT(DISTINCT seller_id) AS unique_sellers,
        SUM(price) AS total_price,
        SUM(freight_value) AS total_freight,
        SUM(total_item_value) AS total_order_value,
        AVG(price) AS avg_item_price
    FROM ${catalog}.silver.silver_order_items
    GROUP BY order_id
),
order_payments_agg AS (
    SELECT
        order_id,
        COUNT(*) AS payment_count,
        SUM(payment_value) AS total_payment_value,
        MAX(payment_installments) AS max_installments,
        COLLECT_SET(payment_type) AS payment_types
    FROM ${catalog}.silver.silver_order_payments
    GROUP BY order_id
),
order_reviews_agg AS (
    SELECT
        order_id,
        MAX(review_score) AS review_score,
        MAX(sentiment) AS review_sentiment,
        MAX(has_comment) AS has_review_comment
    FROM ${catalog}.silver.silver_order_reviews
    GROUP BY order_id
)
SELECT
    -- Order identifiers
    o.order_id,
    o.customer_id,

    -- Order details
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.delivery_days,
    o.delivery_vs_estimate_days,

    -- Item metrics
    COALESCE(oi.item_count, 0) AS item_count,
    COALESCE(oi.unique_products, 0) AS unique_products,
    COALESCE(oi.unique_sellers, 0) AS unique_sellers,
    COALESCE(oi.total_price, 0) AS total_price,
    COALESCE(oi.total_freight, 0) AS total_freight,
    COALESCE(oi.total_order_value, 0) AS total_order_value,
    oi.avg_item_price,

    -- Payment metrics
    COALESCE(op.payment_count, 0) AS payment_count,
    COALESCE(op.total_payment_value, 0) AS total_payment_value,
    COALESCE(op.max_installments, 0) AS max_installments,
    op.payment_types,

    -- Review metrics
    r.review_score,
    r.review_sentiment,
    COALESCE(r.has_review_comment, FALSE) AS has_review_comment,

    -- Derived: Order flags
    CASE WHEN o.order_status = 'delivered' THEN TRUE ELSE FALSE END AS is_delivered,
    CASE WHEN o.order_status = 'canceled' THEN TRUE ELSE FALSE END AS is_canceled,
    CASE WHEN o.delivery_vs_estimate_days < 0 THEN TRUE ELSE FALSE END AS delivered_early,
    CASE WHEN o.delivery_vs_estimate_days > 0 THEN TRUE ELSE FALSE END AS delivered_late,

    -- Time dimensions for analysis
    DATE(o.order_purchase_timestamp) AS order_date,
    DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
    DATE_TRUNC('quarter', o.order_purchase_timestamp) AS order_quarter,
    YEAR(o.order_purchase_timestamp) AS order_year,
    DAYOFWEEK(o.order_purchase_timestamp) AS order_day_of_week,
    HOUR(o.order_purchase_timestamp) AS order_hour,

    -- Audit
    current_timestamp() AS _processed_at

FROM ${catalog}.silver.silver_orders o
LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id
LEFT JOIN order_payments_agg op ON o.order_id = op.order_id
LEFT JOIN order_reviews_agg r ON o.order_id = r.order_id;
