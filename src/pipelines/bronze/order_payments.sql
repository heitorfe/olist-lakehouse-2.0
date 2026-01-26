-- =============================================================================
-- Bronze Layer: Order Payments (AutoLoader - Append Only)
-- =============================================================================
-- Description: Ingests raw payment data from CSV files using Auto Loader
-- Source: /Volumes/${catalog}/raw/olist/order_payments/
-- Pattern: Streaming table - payments are immutable after creation
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_order_payments
COMMENT 'Raw order payment data ingested from CSV files via Auto Loader'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'order_id'
)
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/raw/olist/order_payments/',
    format => 'csv',
    header => true,
    inferSchema => true,
    rescuedDataColumn => '_rescued_data'
);
