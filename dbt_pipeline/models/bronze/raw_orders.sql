{{ config(materialized='streaming_table', schema='bronze') }}

-- {{ source('landing_zone', 'orders') }}

SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_time
FROM STREAM read_files(
    '/Volumes/main/volume/task/dbt_pipeline/orders/', 
    format => 'csv', 
    header => true
)