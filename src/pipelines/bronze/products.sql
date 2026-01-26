-- =============================================================================
-- Bronze Layer: Products (AutoLoader - Append Only)
-- =============================================================================
-- Description: Ingests raw product data from CSV files using Auto Loader
-- Source: /Volumes/${catalog}/raw/olist/products/
-- Note: Initial product load - CDC updates handled separately
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_products
COMMENT 'Raw product data ingested from CSV files via Auto Loader'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'product_id'
)
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    "/Volumes/${catalog}/raw/olist/products/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
