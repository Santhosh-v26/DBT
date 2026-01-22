{{ config(materialized='table', schema='bronze') }}

-- {{ source('landing_zone', 'products') }}

SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_time
FROM read_files(
    '/Volumes/main/volume/task/dbt_pipeline/products/', 
    format => 'csv', 
    header => true
)