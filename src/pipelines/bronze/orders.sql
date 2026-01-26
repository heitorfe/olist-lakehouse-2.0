-- =============================================================================
-- Bronze Layer: Orders (AutoLoader - Append Only)
-- =============================================================================
-- Description: Ingests raw order data from CSV files using Auto Loader
-- Source: /Volumes/${catalog}/raw/olist/orders/
-- Pattern: Streaming table - orders are immutable after creation
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw order data ingested from CSV files via Auto Loader'
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
    "/Volumes/${catalog}/raw/olist/orders/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
