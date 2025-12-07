CREATE OR REFRESH STREAMING TABLE silver_order_payments (
  order_id STRING,
  payment_sequential INT,
  payment_type STRING,
  payment_installments INT,
  payment_value DOUBLE,
  processed_time TIMESTAMP,
  silver_layer_version STRING,

  CONSTRAINT op_order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT op_payment_seq_not_null EXPECT (payment_sequential IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT op_payment_value_non_negative EXPECT (payment_value >= 0) ON VIOLATION DROP ROW
);

CREATE FLOW flow_silver_order_payments
AS AUTO CDC INTO silver_order_payments
FROM STREAM(stg_cdc_order_payments)
  KEYS (order_id, payment_sequential)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum)
  STORED AS SCD TYPE 1;
