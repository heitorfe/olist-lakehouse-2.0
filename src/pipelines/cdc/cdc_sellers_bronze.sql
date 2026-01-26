-- =============================================================================
-- CDC Bronze Layer: Seller Changes (AutoLoader)
-- =============================================================================
-- Description: Ingests CDC change feed for sellers from CSV files
-- Source: /Volumes/${catalog}/raw/olist/cdc/sellers/
-- Pattern: Streaming table capturing INSERT/UPDATE/DELETE operations
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_cdc_sellers
COMMENT 'Raw CDC change feed for seller data'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- CDC metadata
    CAST(sequence_number AS BIGINT) AS sequence_number,
    UPPER(TRIM(operation)) AS operation,
    TO_TIMESTAMP(change_timestamp) AS change_timestamp,

    -- Seller data
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,

    -- Ingestion metadata
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/raw/olist/cdc/sellers/',
    format => 'csv',
    header => true,
    inferSchema => true,
    rescuedDataColumn => '_rescued_data'
);
