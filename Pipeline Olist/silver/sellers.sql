-- =========================================================
-- silver_sellers
-- =========================================================
CREATE OR REFRESH STREAMING TABLE silver_sellers (
  CONSTRAINT ss_seller_id_not_null EXPECT (seller_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT ss_zip_prefix_not_null EXPECT (seller_zip_code_prefix IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  TRIM(seller_id) AS seller_id,
  CAST(TRIM(seller_zip_code_prefix) AS INT) AS seller_zip_code_prefix,
  TRIM(INITCAP(seller_city)) AS seller_city,
  TRIM(seller_state) AS seller_state,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_sellers;