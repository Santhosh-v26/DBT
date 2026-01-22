{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    schema='silver'
) }}

SELECT * FROM {{ ref('raw_payments') }}
WHERE payment_id IS NOT NULL
{% if is_incremental() %}
  AND file_time > (SELECT MAX(file_time) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payment_id 
    ORDER BY file_time DESC
) = 1