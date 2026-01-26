-- =============================================================================
-- CDC Silver Layer: Product CDC Processing (AUTO CDC)
-- =============================================================================
-- Description: Processes product changes using AUTO CDC (APPLY CHANGES)
-- Source: bronze_cdc_products
-- Pattern: SCD Type 1 (current state) and SCD Type 2 (history tracking)
-- =============================================================================

-- =============================================================================
-- Staging: Validated CDC Events
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE stg_cdc_products (
    CONSTRAINT valid_product_id
        EXPECT (product_id IS NOT NULL AND LENGTH(TRIM(product_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_operation
        EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE'))
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_sequence
        EXPECT (sequence_number IS NOT NULL)
        ON VIOLATION DROP ROW
)
COMMENT 'Validated CDC events for products'
TBLPROPERTIES ('quality' = 'staging')
AS SELECT
    sequence_number,
    operation,
    change_timestamp,
    TRIM(product_id) AS product_id,
    LOWER(TRIM(REPLACE(product_category_name, '_', ' '))) AS product_category_name,
    CAST(product_name_lenght AS INT) AS product_name_length,
    CAST(product_description_lenght AS INT) AS product_description_length,
    CAST(product_photos_qty AS INT) AS product_photos_qty,
    CAST(product_weight_g AS DECIMAL(10,2)) AS product_weight_g,
    CAST(product_length_cm AS DECIMAL(10,2)) AS product_length_cm,
    CAST(product_height_cm AS DECIMAL(10,2)) AS product_height_cm,
    CAST(product_width_cm AS DECIMAL(10,2)) AS product_width_cm,
    _ingested_at
FROM STREAM(bronze_cdc_products);

-- =============================================================================
-- SCD Type 1: Current Product State
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_products_current
COMMENT 'Current state of products (SCD Type 1) - Updated via CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
);

APPLY CHANGES INTO silver_products_current
FROM STREAM stg_cdc_products
KEYS (product_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 1;

-- =============================================================================
-- SCD Type 2: Product History
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_products_history
COMMENT 'Historical product changes (SCD Type 2) - Full audit trail via CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
);

APPLY CHANGES INTO silver_products_history
FROM STREAM stg_cdc_products
KEYS (product_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 2;
