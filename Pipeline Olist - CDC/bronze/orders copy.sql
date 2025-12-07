CREATE OR REFRESH STREAMING TABLE bronze_orders
AS
SELECT
  *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/orders/orders_*.csv',
  format => 'csv'
);
