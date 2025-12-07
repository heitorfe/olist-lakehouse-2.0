CREATE OR REFRESH STREAMING TABLE bronze_cdc_customers
AS
SELECT
  *,
  _metadata.file_path              AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/cdc/customers/customers_*.csv',
  format => 'csv',
  header => true
);
