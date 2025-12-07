-- =========================================================
-- silver_order_items (1 linha por order_id + order_item_id)
-- =========================================================
CREATE OR REFRESH STREAMING TABLE silver_order_items (
  CONSTRAINT oi_order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT oi_product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT oi_seller_id_not_null EXPECT (seller_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT oi_price_positive EXPECT (CAST(price AS DOUBLE) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT oi_freight_non_negative EXPECT (CAST(freight_value AS DOUBLE) >= 0) ON VIOLATION DROP ROW
)
AS
SELECT
  TRIM(order_id) AS order_id,
  CAST(TRIM(order_item_id) AS INT) AS order_item_id,
  TRIM(product_id) AS product_id,
  TRIM(seller_id) AS seller_id,
  TO_TIMESTAMP(TRIM(shipping_limit_date), 'yyyy-MM-dd HH:mm:ss') AS shipping_limit_date,
  CAST(TRIM(price) AS DOUBLE) AS price,
  CAST(TRIM(freight_value) AS DOUBLE) AS freight_value,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_order_items;