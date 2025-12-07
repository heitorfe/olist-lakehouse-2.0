-- =========================================================
-- silver_order_payments (1 linha por pagamento)
-- =========================================================
CREATE OR REFRESH STREAMING TABLE silver_order_payments (
  CONSTRAINT op_order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT op_payment_seq_not_null EXPECT (payment_sequential IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT op_payment_value_non_negative EXPECT (CAST(payment_value AS DOUBLE) >= 0) ON VIOLATION DROP ROW
)
AS
SELECT
  TRIM(order_id) AS order_id,
  CAST(TRIM(payment_sequential) AS INT) AS payment_sequential,
  TRIM(payment_type) AS payment_type,
  CAST(TRIM(payment_installments) AS INT) AS payment_installments,
  CAST(TRIM(payment_value) AS DOUBLE) AS payment_value,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_order_payments;
