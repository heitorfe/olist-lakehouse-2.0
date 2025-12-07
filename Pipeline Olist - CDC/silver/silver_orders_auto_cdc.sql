CREATE OR REFRESH STREAMING TABLE silver_orders (
  order_id STRING,
  customer_id STRING,
  status STRING,
  purchase_timestamp TIMESTAMP,
  approved_at TIMESTAMP,
  delivered_carrier_date DATE,
  delivered_customer_date DATE,
  estimated_delivery_date DATE,
  processed_time TIMESTAMP,
  silver_layer_version STRING,

  CONSTRAINT orders_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT orders_customer_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT orders_status_valid EXPECT (
    status IN ('created','approved','invoiced','processing','shipped','delivered','canceled','unavailable')
  )
);

CREATE FLOW flow_silver_orders
AS AUTO CDC INTO silver_orders
FROM STREAM(stg_cdc_orders)
  KEYS (order_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum)
  STORED AS SCD TYPE 1;
