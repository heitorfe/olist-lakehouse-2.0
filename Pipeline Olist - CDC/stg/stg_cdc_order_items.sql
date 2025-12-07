CREATE OR REFRESH STREAMING TABLE stg_cdc_order_items
AS
SELECT
  operation,
  CAST(sequenceNum AS BIGINT) AS sequenceNum,

  TRIM(order_id) AS order_id,
  CAST(TRIM(order_item_id) AS INT) AS order_item_id,
  TRIM(product_id) AS product_id,
  TRIM(seller_id) AS seller_id,
  TO_TIMESTAMP(TRIM(shipping_limit_date), 'yyyy-MM-dd HH:mm:ss') AS shipping_limit_date,
  CAST(TRIM(price) AS DOUBLE) AS price,
  CAST(TRIM(freight_value) AS DOUBLE) AS freight_value,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_cdc_order_items;
