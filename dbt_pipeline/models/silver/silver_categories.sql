{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT * FROM {{ ref('raw_categories') }}
WHERE category_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY category_id 
    ORDER BY file_time DESC
) = 1