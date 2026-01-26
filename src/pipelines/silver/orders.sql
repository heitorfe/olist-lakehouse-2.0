-- =============================================================================
-- Silver Layer: Orders (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates order data from bronze layer
-- Source: bronze_orders
-- Data Quality: DROP rows with invalid order_id or customer_id
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_orders (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_order_id
        EXPECT (order_id IS NOT NULL AND LENGTH(TRIM(order_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_customer_id
        EXPECT (customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_order_status
        EXPECT (order_status IN ('created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'unavailable', 'canceled'))
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_purchase_timestamp
        EXPECT (order_purchase_timestamp IS NOT NULL)
        ON VIOLATION DROP ROW,
    -- Warning constraints
    CONSTRAINT no_rescued_data
        EXPECT (_rescued_data IS NULL)
)
COMMENT 'Cleansed order data with validated timestamps and status values'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifiers
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,

    -- Order status (normalized)
    LOWER(TRIM(order_status)) AS order_status,

    -- Timestamps (converted to proper types)
    TO_TIMESTAMP(order_purchase_timestamp) AS order_purchase_timestamp,
    TO_TIMESTAMP(order_approved_at) AS order_approved_at,
    TO_TIMESTAMP(order_delivered_carrier_date) AS order_delivered_carrier_date,
    TO_TIMESTAMP(order_delivered_customer_date) AS order_delivered_customer_date,
    TO_TIMESTAMP(order_estimated_delivery_date) AS order_estimated_delivery_date,

    -- Derived fields
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN DATEDIFF(TO_TIMESTAMP(order_delivered_customer_date), TO_TIMESTAMP(order_purchase_timestamp))
        ELSE NULL
    END AS delivery_days,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
        THEN DATEDIFF(TO_TIMESTAMP(order_estimated_delivery_date), TO_TIMESTAMP(order_delivered_customer_date))
        ELSE NULL
    END AS delivery_vs_estimate_days,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_orders);
