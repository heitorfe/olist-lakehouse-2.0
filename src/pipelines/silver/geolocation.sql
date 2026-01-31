-- =============================================================================
-- Silver Layer: Geolocation (Data Quality & Transformation)
-- =============================================================================
-- Description: Cleanses and deduplicates geolocation reference data
-- Source: bronze_geolocation
-- Pattern: Materialized view for aggregated reference data
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.silver.silver_geolocation
COMMENT 'Cleansed and deduplicated geolocation reference data with averaged coordinates'
TBLPROPERTIES (
    'quality' = 'silver'
)
AS
WITH deduplicated AS (
    SELECT
        CAST(geolocation_zip_code_prefix AS INT) AS zip_code_prefix,
        INITCAP(TRIM(geolocation_city)) AS city,
        UPPER(TRIM(geolocation_state)) AS state,
        AVG(CAST(geolocation_lat AS DOUBLE)) AS latitude,
        AVG(CAST(geolocation_lng AS DOUBLE)) AS longitude,
        COUNT(*) AS sample_count
    FROM ${catalog}.bronze.bronze_geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL
        AND geolocation_lat IS NOT NULL
        AND geolocation_lng IS NOT NULL
        AND _rescued_data IS NULL
    GROUP BY
        CAST(geolocation_zip_code_prefix AS INT),
        INITCAP(TRIM(geolocation_city)),
        UPPER(TRIM(geolocation_state))
)
SELECT
    zip_code_prefix,
    city,
    state,
    ROUND(latitude, 6) AS latitude,
    ROUND(longitude, 6) AS longitude,
    sample_count,
    -- Region classification
    CASE state
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
    END AS region,
    current_timestamp() AS _processed_at
FROM deduplicated;
