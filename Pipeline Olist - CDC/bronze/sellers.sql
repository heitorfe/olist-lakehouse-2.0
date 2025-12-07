CREATE OR REFRESH STREAMING TABLE bronze_sellers
AS
SELECT *, 
_metadata.file_path AS source_file,
_metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/sellers/sellers_*.csv',
  format => 'csv'
)