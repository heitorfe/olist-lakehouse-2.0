CREATE OR REFRESH STREAMING TABLE stg_cdc_orders
AS
SELECT
  operation,
  CAST(sequenceNum AS BIGINT) AS sequenceNum,

  TRIM(order_id) AS order_id,
  TRIM(customer_id) AS customer_id,
  TRIM(order_status) AS status,

  TO_TIMESTAMP(TRIM(order_purchase_timestamp), 'yyyy-MM-dd HH:mm:ss') AS purchase_timestamp,
  TO_TIMESTAMP(TRIM(order_approved_at), 'yyyy-MM-dd HH:mm:ss')        AS approved_at,
  TO_DATE(TRIM(order_delivered_carrier_date), 'yyyy-MM-dd HH:mm:ss')  AS delivered_carrier_date,
  TO_DATE(TRIM(order_delivered_customer_date), 'yyyy-MM-dd HH:mm:ss') AS delivered_customer_date,
  TO_DATE(TRIM(order_estimated_delivery_date), 'yyyy-MM-dd HH:mm:ss') AS estimated_delivery_date,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_cdc_orders;
