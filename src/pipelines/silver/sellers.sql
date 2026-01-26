-- =============================================================================
-- Silver Layer: Sellers (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates seller data from bronze layer
-- Source: bronze_sellers
-- Data Quality: DROP rows with invalid seller_id
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_sellers (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_seller_id
        EXPECT (seller_id IS NOT NULL AND LENGTH(TRIM(seller_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_zip_code
        EXPECT (seller_zip_code_prefix IS NOT NULL)
        ON VIOLATION DROP ROW
)
COMMENT 'Cleansed seller data with validated IDs and normalized location fields'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifier
    TRIM(seller_id) AS seller_id,

    -- Location data (normalized)
    CAST(seller_zip_code_prefix AS INT) AS seller_zip_code_prefix,
    INITCAP(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,

    -- Derived: Region classification
    CASE UPPER(TRIM(seller_state))
        WHEN 'SP' THEN 'Southeast'
        WHEN 'RJ' THEN 'Southeast'
        WHEN 'MG' THEN 'Southeast'
        WHEN 'ES' THEN 'Southeast'
        WHEN 'PR' THEN 'South'
        WHEN 'SC' THEN 'South'
        WHEN 'RS' THEN 'South'
        WHEN 'MS' THEN 'Midwest'
        WHEN 'MT' THEN 'Midwest'
        WHEN 'GO' THEN 'Midwest'
        WHEN 'DF' THEN 'Midwest'
        WHEN 'BA' THEN 'Northeast'
        WHEN 'SE' THEN 'Northeast'
        WHEN 'AL' THEN 'Northeast'
        WHEN 'PE' THEN 'Northeast'
        WHEN 'PB' THEN 'Northeast'
        WHEN 'RN' THEN 'Northeast'
        WHEN 'CE' THEN 'Northeast'
        WHEN 'PI' THEN 'Northeast'
        WHEN 'MA' THEN 'Northeast'
        WHEN 'PA' THEN 'North'
        WHEN 'AM' THEN 'North'
        WHEN 'AP' THEN 'North'
        WHEN 'RR' THEN 'North'
        WHEN 'AC' THEN 'North'
        WHEN 'RO' THEN 'North'
        WHEN 'TO' THEN 'North'
        ELSE 'Unknown'
    END AS seller_region,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_sellers);
