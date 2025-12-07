CREATE OR REFRESH STREAMING TABLE bronze_geolocation
AS
SELECT *, 
_metadata.file_path AS source_file,
_metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/dev/raw/olist/geolocation/geolocation_*.csv',
  format => 'csv'
)