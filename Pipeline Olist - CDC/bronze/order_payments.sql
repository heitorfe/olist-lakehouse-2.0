CREATE OR REFRESH STREAMING TABLE bronze_order_payments
AS
SELECT *, 
_metadata.file_path AS source_file,
_metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/order_payments/order_payments_*.csv',
  format => 'csv'
)