-- =============================================================================
-- CDC Bronze Layer: Customer Changes (AutoLoader)
-- =============================================================================
-- Description: Ingests CDC change feed for customers from CSV files
-- Source: /Volumes/${catalog}/raw/olist/cdc/customers/
-- Pattern: Streaming table capturing INSERT/UPDATE/DELETE operations
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_cdc_customers
COMMENT 'Raw CDC change feed for customer data'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- CDC metadata
    CAST(sequence_number AS BIGINT) AS sequence_number,
    UPPER(TRIM(operation)) AS operation,  -- INSERT, UPDATE, DELETE
    TO_TIMESTAMP(change_timestamp) AS change_timestamp,

    -- Customer data
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,

    -- Ingestion metadata
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    "/Volumes/${catalog}/raw/olist/cdc/customers/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
