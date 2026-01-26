-- =============================================================================
-- CDC Silver Layer: Customer CDC Processing (AUTO CDC)
-- =============================================================================
-- Description: Processes customer changes using AUTO CDC (CREATE FLOW syntax)
-- Source: bronze_cdc_customers
-- Pattern: SCD Type 1 (current state) and SCD Type 2 (history tracking)
-- =============================================================================

-- =============================================================================
-- Staging: Validated CDC Events
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE stg_cdc_customers (
    CONSTRAINT valid_customer_id
        EXPECT (customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_operation
        EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE'))
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_sequence
        EXPECT (sequence_number IS NOT NULL)
        ON VIOLATION DROP ROW
)
COMMENT 'Validated CDC events for customers'
TBLPROPERTIES ('quality' = 'staging')
AS SELECT
    sequence_number,
    operation,
    change_timestamp,
    TRIM(customer_id) AS customer_id,
    TRIM(customer_unique_id) AS customer_unique_id,
    CAST(customer_zip_code_prefix AS INT) AS customer_zip_code_prefix,
    INITCAP(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,
    _ingested_at
FROM STREAM(bronze_cdc_customers);

-- =============================================================================
-- SCD Type 1: Current Customer State
-- =============================================================================
-- Maintains only the current state of each customer (no history)
-- Useful for: Current lookups, joins with fact tables
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_customers_current
COMMENT 'Current state of customers (SCD Type 1) - Updated via AUTO CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
);

CREATE FLOW customers_current_flow
AS AUTO CDC INTO silver_customers_current
FROM stream(stg_cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 1;

-- =============================================================================
-- SCD Type 2: Customer History
-- =============================================================================
-- Maintains full history of customer changes with validity periods
-- Useful for: Point-in-time analysis, audit trails, compliance
-- Generated columns: __START_AT, __END_AT, __IS_CURRENT
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_customers_history
COMMENT 'Historical customer changes (SCD Type 2) - Full audit trail via AUTO CDC'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
);

CREATE FLOW customers_history_flow
AS AUTO CDC INTO silver_customers_history
FROM stream(stg_cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 2;
