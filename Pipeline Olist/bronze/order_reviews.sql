CREATE OR REFRESH STREAMING TABLE bronze_order_reviews
AS
SELECT *, 
_metadata.file_path AS source_file,
_metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/order_reviews/order_reviews_*.csv',
  format => 'csv',
  multiline => true
)