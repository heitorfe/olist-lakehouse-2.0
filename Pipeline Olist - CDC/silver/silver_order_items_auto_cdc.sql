CREATE OR REFRESH STREAMING TABLE silver_order_items (
  order_id STRING,
  order_item_id INT,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date TIMESTAMP,
  price DOUBLE,
  freight_value DOUBLE,
  processed_time TIMESTAMP,
  silver_layer_version STRING,

  CONSTRAINT oi_order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT oi_product_id_not_null EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT oi_seller_id_not_null EXPECT (seller_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT oi_price_positive EXPECT (price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT oi_freight_non_negative EXPECT (freight_value >= 0) ON VIOLATION DROP ROW
);

CREATE FLOW flow_silver_order_items
AS AUTO CDC INTO silver_order_items
FROM STREAM(stg_cdc_order_items)
  KEYS (order_id, order_item_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum)
  STORED AS SCD TYPE 1;
