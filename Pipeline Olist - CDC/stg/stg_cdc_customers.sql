CREATE OR REFRESH STREAMING TABLE stg_cdc_customers
AS
SELECT
  operation,
  CAST(sequenceNum AS BIGINT) AS sequenceNum,

  TRIM(customer_id) AS customer_id,
  TRIM(customer_unique_id) AS customer_unique_id,
  CAST(TRIM(customer_zip_code_prefix) AS INT) AS customer_zip_code_prefix,
  TRIM(INITCAP(customer_city)) AS customer_city,
  TRIM(customer_state) AS customer_state,

  CURRENT_TIMESTAMP() AS processed_time,
  '1.0' AS silver_layer_version
FROM STREAM bronze_cdc_customers;
