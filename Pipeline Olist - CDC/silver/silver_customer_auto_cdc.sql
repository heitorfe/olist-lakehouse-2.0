-- Target (schema + expectations/constraints)
CREATE OR REFRESH STREAMING TABLE silver_customers (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix INT,
  customer_city STRING,
  customer_state STRING,
  processed_time TIMESTAMP,
  silver_layer_version STRING,

  CONSTRAINT customer_id_not_null
    EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,

  CONSTRAINT customer_unique_id_not_null
    EXPECT (customer_unique_id IS NOT NULL) ON VIOLATION DROP ROW
);

-- Flow AUTO CDC (aplica UPSERT/DELETE na target)
CREATE FLOW flow_silver_customers
AS AUTO CDC INTO silver_customers
FROM STREAM(stg_cdc_customers)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum)
  STORED AS SCD TYPE 1;
