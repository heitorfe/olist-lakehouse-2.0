-- =============================================================================
-- Bronze Layer: Customers (AutoLoader - Append Only)
-- =============================================================================
-- Description: Ingests raw customer data from CSV files using Auto Loader
-- Source: /Volumes/${catalog}/raw/olist/customers/
-- Pattern: Streaming table with schema inference
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.bronze_customers
COMMENT 'Raw customer data ingested from CSV files via Auto Loader'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'customer_id'
)
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    "/Volumes/${catalog}/raw/olist/customers/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
