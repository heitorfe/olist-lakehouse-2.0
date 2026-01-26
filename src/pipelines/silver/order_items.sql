-- =============================================================================
-- Silver Layer: Order Items (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates order item data from bronze layer
-- Source: bronze_order_items
-- Data Quality: DROP rows with invalid IDs or negative prices
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_order_items (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_order_id
        EXPECT (order_id IS NOT NULL AND LENGTH(TRIM(order_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_order_item_id
        EXPECT (order_item_id IS NOT NULL AND order_item_id > 0)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_product_id
        EXPECT (product_id IS NOT NULL AND LENGTH(TRIM(product_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_seller_id
        EXPECT (seller_id IS NOT NULL AND LENGTH(TRIM(seller_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_price
        EXPECT (price IS NOT NULL AND price >= 0)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_freight
        EXPECT (freight_value IS NOT NULL AND freight_value >= 0)
        ON VIOLATION DROP ROW,
    -- Warning constraints
    CONSTRAINT no_rescued_data
        EXPECT (_rescued_data IS NULL)
)
COMMENT 'Cleansed order item data with validated prices and calculated totals'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifiers
    TRIM(order_id) AS order_id,
    CAST(order_item_id AS INT) AS order_item_id,
    TRIM(product_id) AS product_id,
    TRIM(seller_id) AS seller_id,

    -- Shipping deadline
    TO_TIMESTAMP(shipping_limit_date) AS shipping_limit_date,

    -- Financial data (as DECIMAL for precision)
    CAST(price AS DECIMAL(10,2)) AS price,
    CAST(freight_value AS DECIMAL(10,2)) AS freight_value,

    -- Calculated fields
    CAST(price AS DECIMAL(10,2)) + CAST(freight_value AS DECIMAL(10,2)) AS total_item_value,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_order_items);
