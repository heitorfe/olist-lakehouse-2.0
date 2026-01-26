-- =============================================================================
-- Silver Layer: Products (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and validates product data from bronze layer
-- Source: bronze_products
-- Data Quality: DROP rows with invalid product_id
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_products (
    -- Data Quality Constraints (DROP invalid rows)
    CONSTRAINT valid_product_id
        EXPECT (product_id IS NOT NULL AND LENGTH(TRIM(product_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_weight
        EXPECT (product_weight_g IS NULL OR product_weight_g >= 0)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_dimensions
        EXPECT (
            (product_length_cm IS NULL OR product_length_cm >= 0) AND
            (product_height_cm IS NULL OR product_height_cm >= 0) AND
            (product_width_cm IS NULL OR product_width_cm >= 0)
        )
        ON VIOLATION DROP ROW
)
COMMENT 'Cleansed product data with validated dimensions and calculated volume'
TBLPROPERTIES (
    'quality' = 'silver',
    'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
    -- Primary identifier
    TRIM(product_id) AS product_id,

    -- Category (normalized)
    LOWER(TRIM(REPLACE(product_category_name, '_', ' '))) AS product_category_name,

    -- Product attributes
    CAST(product_name_lenght AS INT) AS product_name_length,
    CAST(product_description_lenght AS INT) AS product_description_length,
    CAST(product_photos_qty AS INT) AS product_photos_qty,

    -- Physical dimensions
    CAST(product_weight_g AS DECIMAL(10, 2)) AS product_weight_g,
    CAST(product_length_cm AS DECIMAL(10, 2)) AS product_length_cm,
    CAST(product_height_cm AS DECIMAL(10, 2)) AS product_height_cm,
    CAST(product_width_cm AS DECIMAL(10, 2)) AS product_width_cm,

    -- Derived: Volume in cubic centimeters
    CASE
        WHEN product_length_cm IS NOT NULL
            AND product_height_cm IS NOT NULL
            AND product_width_cm IS NOT NULL
        THEN CAST(product_length_cm AS DECIMAL(10, 2)) *
             CAST(product_height_cm AS DECIMAL(10, 2)) *
             CAST(product_width_cm AS DECIMAL(10, 2))
        ELSE NULL
    END AS product_volume_cm3,

    -- Derived: Weight in kilograms
    CASE
        WHEN product_weight_g IS NOT NULL
        THEN CAST(product_weight_g AS DECIMAL(10, 2)) / 1000.0
        ELSE NULL
    END AS product_weight_kg,

    -- Size category
    CASE
        WHEN product_weight_g IS NULL THEN 'unknown'
        WHEN CAST(product_weight_g AS INT) < 500 THEN 'small'
        WHEN CAST(product_weight_g AS INT) < 2000 THEN 'medium'
        WHEN CAST(product_weight_g AS INT) < 10000 THEN 'large'
        ELSE 'extra_large'
    END AS size_category,

    -- Audit columns
    _source_file,
    _file_modified_at,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM(bronze_products);
