-- =========================================================
-- silver_products
-- =========================================================
CREATE OR REFRESH STREAMING TABLE silver_products (
  CONSTRAINT sp_product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sp_category_not_null EXPECT (product_category_name IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  TRIM(product_id) AS product_id,
  TRIM(product_category_name) AS product_category_name,
  CAST(TRIM(product_name_lenght) AS INT) AS product_name_lenght,
  CAST(TRIM(product_description_lenght) AS INT) AS product_description_lenght,
  CAST(TRIM(product_photos_qty) AS INT) AS product_photos_qty,
  CAST(TRIM(product_weight_g) AS INT) AS product_weight_g,
  CAST(TRIM(product_length_cm) AS INT) AS product_length_cm,
  CAST(TRIM(product_height_cm) AS INT) AS product_height_cm,
  CAST(TRIM(product_width_cm) AS INT) AS product_width_cm,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_products;