-- =============================================================================
-- Bronze Layer: Geolocation (AutoLoader - Reference Data)
-- =============================================================================
-- Description: Ingests geolocation reference data from CSV files
-- Source: /Volumes/${catalog}/raw/olist/geolocation/
-- Pattern: Reference/dimension table for geographic lookups
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_geolocation
COMMENT 'Raw geolocation reference data ingested from CSV files via Auto Loader'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true',
    'pipelines.autoOptimize.zOrderCols' = 'geolocation_zip_code_prefix'
)
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    "/Volumes/${catalog}/raw/olist/geolocation/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
