CREATE OR REFRESH STREAMING TABLE bronze_cdc_order_payments
AS
SELECT
  *,
  _metadata.file_path              AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/cdc/order_payments/order_payments_*.csv',
  format => 'csv',
  header => true
);
