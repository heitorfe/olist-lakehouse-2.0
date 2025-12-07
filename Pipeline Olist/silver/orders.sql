-- =========================================================
-- silver_orders  (1 linha por order_id)
-- =========================================================
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT orders_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT orders_customer_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT orders_status_valid EXPECT (
    status IN ('created','approved','invoiced','processing','shipped','delivered','canceled','unavailable')
  )
)
AS
SELECT
  TRIM(order_id) AS order_id,
  TRIM(customer_id) AS customer_id,
  TRIM(order_status) AS status,

  TO_TIMESTAMP(TRIM(order_purchase_timestamp), 'yyyy-MM-dd HH:mm:ss') AS purchase_timestamp,
  TO_TIMESTAMP(TRIM(order_approved_at), 'yyyy-MM-dd HH:mm:ss') AS approved_at,
  TO_DATE(TRIM(order_delivered_carrier_date), 'yyyy-MM-dd HH:mm:ss') AS delivered_carrier_date,
  TO_DATE(TRIM(order_delivered_customer_date), 'yyyy-MM-dd HH:mm:ss') AS delivered_customer_date,
  TO_DATE(TRIM(order_estimated_delivery_date), 'yyyy-MM-dd HH:mm:ss') AS estimated_delivery_date,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_orders;
