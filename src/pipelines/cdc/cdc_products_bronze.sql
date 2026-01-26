-- =============================================================================
-- CDC Bronze Layer: Product Changes (AutoLoader)
-- =============================================================================
-- Description: Ingests CDC change feed for products from CSV files
-- Source: /Volumes/${catalog}/raw/olist/cdc/products/
-- Pattern: Streaming table capturing INSERT/UPDATE/DELETE operations
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_cdc_products
COMMENT 'Raw CDC change feed for product data'
TBLPROPERTIES (
    'quality' = 'bronze',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- CDC metadata
    CAST(sequence_number AS BIGINT) AS sequence_number,
    UPPER(TRIM(operation)) AS operation,
    TO_TIMESTAMP(change_timestamp) AS change_timestamp,

    -- Product data
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,

    -- Ingestion metadata
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    "/Volumes/${catalog}/raw/olist/cdc/products/",
    format => "csv",
    header => true,
    inferSchema => true,
    rescuedDataColumn => "_rescued_data"
);
