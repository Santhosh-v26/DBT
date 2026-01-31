{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite' if var('load_type','delta') == 'full_load' else 'merge',
    unique_key='order_id',
    schema='silver'
) }}

SELECT * FROM {{ ref('raw_orders') }}
WHERE
    order_id IS NOT NULL

    {% if is_incremental() and var('load_type','delta') == 'delta' %}
        -- 2nd Run Logic: Only grab new records based on time
        AND file_time > (SELECT coalesce(max(file_time), '1991-01-01') FROM {{ this }})
    {% endif %}
-- 1st Run & 3rd Run: No filter is applied, so we select the whole table

QUALIFY row_number() OVER (
    PARTITION BY order_id
    ORDER BY file_time DESC
) = 1
