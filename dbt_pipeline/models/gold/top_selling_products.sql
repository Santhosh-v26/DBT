{{ config(
    materialized='table',
    schema='gold'
) }}

WITH product_metrics AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(o.amount) AS total_revenue,
        SUM(o.amount / p.price) AS total_quantity_sold
    FROM {{ ref('silver_products') }} p
    JOIN {{ ref('silver_orders') }} o ON p.product_id = o.product_id
    GROUP BY 1, 2
)

SELECT 
    *,
    DENSE_RANK() OVER (
        ORDER BY total_revenue DESC
    ) AS rank_by_product
FROM product_metrics