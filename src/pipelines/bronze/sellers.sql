-- =============================================================================
-- Bronze Layer: Sellers (AutoLoader - Append Only)
-- =============================================================================
-- Description: Ingests raw seller data from CSV files using Auto Loader
-- Source: /Volumes/${catalog}/raw/olist/sellers/
-- Note: Initial seller load - CDC updates handled separately
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_sellers
COMMENT 'Raw seller data ingested from CSV files via Auto Loader'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'seller_id'
)
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/raw/olist/sellers/',
    format => 'csv',
    header => true,
    inferSchema => true,
    rescuedDataColumn => '_rescued_data'
);
