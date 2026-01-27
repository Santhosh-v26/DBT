{{ config(materialized='table', catalog = 'main',schema='dbt_bronze') }}

-- {{ source('landing_zone', 'customers') }}

SELECT 
    *,
    _metadata.file_path AS source_file,
    _metadata.file_modification_time AS file_time
FROM read_files(
    '/Volumes/main/volume/task/dbt_pipeline/customers/', 
    format => 'csv', 
    header => true
)