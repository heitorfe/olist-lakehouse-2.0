-- =============================================================================
-- Bronze Layer: Order Items (AutoLoader - Append Only)
-- =============================================================================
-- Description: Ingests raw order item data from CSV files using Auto Loader
-- Source: /Volumes/${catalog}/raw/olist/order_items/
-- Pattern: Streaming table - order items are immutable
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_order_items
COMMENT 'Raw order item data ingested from CSV files via Auto Loader'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'order_id,order_item_id'
)
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    "/Volumes/${catalog}/raw/olist/order_items/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
