CREATE OR REFRESH STREAMING TABLE bronze_products
AS
SELECT *, 
_metadata.file_path AS source_file,
_metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/products/products_*.csv',
  format => 'csv'
)