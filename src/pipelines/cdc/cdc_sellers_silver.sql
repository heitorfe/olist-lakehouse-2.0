-- =============================================================================
-- CDC Silver Layer: Seller CDC Processing (AUTO CDC)
-- =============================================================================
-- Description: Processes seller changes using AUTO CDC (CREATE FLOW syntax)
-- Source: bronze_cdc_sellers
-- Pattern: SCD Type 1 (current state) and SCD Type 2 (history tracking)
-- =============================================================================

-- =============================================================================
-- Staging: Validated CDC Events
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.stg_cdc_sellers (
    CONSTRAINT valid_seller_id
        EXPECT (seller_id IS NOT NULL AND LENGTH(TRIM(seller_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_operation
        EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE'))
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_sequence
        EXPECT (sequence_number IS NOT NULL)
        ON VIOLATION DROP ROW
)
COMMENT 'Validated CDC events for sellers'
TBLPROPERTIES ('quality' = 'staging')
AS SELECT
    sequence_number,
    operation,
    change_timestamp,
    TRIM(seller_id) AS seller_id,
    CAST(seller_zip_code_prefix AS INT) AS seller_zip_code_prefix,
    INITCAP(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,
    _ingested_at
FROM STREAM(${catalog}.bronze.bronze_cdc_sellers);

-- =============================================================================
-- SCD Type 1: Current Seller State
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_sellers_current
COMMENT 'Current state of sellers (SCD Type 1) - Updated via AUTO CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
);

CREATE FLOW sellers_current_flow
AS AUTO CDC INTO ${catalog}.silver.silver_sellers_current
FROM stream(${catalog}.silver.stg_cdc_sellers)
KEYS (seller_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 1;

-- =============================================================================
-- SCD Type 2: Seller History
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_sellers_history
COMMENT 'Historical seller changes (SCD Type 2) - Full audit trail via AUTO CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
);

CREATE FLOW sellers_history_flow
AS AUTO CDC INTO ${catalog}.silver.silver_sellers_history
FROM stream(${catalog}.silver.stg_cdc_sellers)
KEYS (seller_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 2;
