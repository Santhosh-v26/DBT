{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT * FROM {{ ref('raw_customers') }}
WHERE customer_id IS NOT null
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY file_time DESC
) = 1
