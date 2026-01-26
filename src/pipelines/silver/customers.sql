-- =============================================================================
-- Silver Layer: Customers (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates customer data from bronze layer
-- Source: bronze_customers
-- Data Quality: DROP rows with invalid customer_id or customer_unique_id
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_customers (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_customer_id
        EXPECT (customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_unique_id
        EXPECT (customer_unique_id IS NOT NULL AND LENGTH(TRIM(customer_unique_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_zip_code
        EXPECT (customer_zip_code_prefix IS NOT NULL)
        ON VIOLATION DROP ROW,
    -- Warning constraints (track but don't drop)
    CONSTRAINT no_rescued_data
        EXPECT (_rescued_data IS NULL)
)
COMMENT 'Cleansed customer data with validated IDs and normalized location fields'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifiers (trimmed and validated)
    TRIM(customer_id) AS customer_id,
    TRIM(customer_unique_id) AS customer_unique_id,

    -- Location data (normalized)
    CAST(customer_zip_code_prefix AS INT) AS customer_zip_code_prefix,
    INITCAP(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_customers);
