-- =========================================================
-- silver_geolocation
-- (schema típico do Olist: zip_prefix, lat, lng, city, state)
-- =========================================================

CREATE OR REFRESH STREAMING TABLE silver_geolocation (
  CONSTRAINT sg_zip_prefix_not_null EXPECT (zip_code_prefix IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sg_lat_valid EXPECT (CAST(lat AS DOUBLE) BETWEEN -90 AND 90) ON VIOLATION DROP ROW,
  CONSTRAINT sg_lng_valid EXPECT (CAST(lng AS DOUBLE) BETWEEN -180 AND 180) ON VIOLATION DROP ROW
)
AS
SELECT
  CAST(TRIM(geolocation_zip_code_prefix) AS INT) AS zip_code_prefix,
  CAST(TRIM(geolocation_lat) AS DOUBLE) AS lat,
  CAST(TRIM(geolocation_lng) AS DOUBLE) AS lng,
  TRIM(INITCAP(geolocation_city)) AS city,
  TRIM(geolocation_state) AS state,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_geolocation;