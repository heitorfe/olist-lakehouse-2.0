CREATE OR REFRESH STREAMING TABLE stg_cdc_order_payments
AS
SELECT
  operation,
  CAST(sequenceNum AS BIGINT) AS sequenceNum,

  TRIM(order_id) AS order_id,
  CAST(TRIM(payment_sequential) AS INT) AS payment_sequential,
  TRIM(payment_type) AS payment_type,
  CAST(TRIM(payment_installments) AS INT) AS payment_installments,
  CAST(TRIM(payment_value) AS DOUBLE) AS payment_value,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_cdc_order_payments;
