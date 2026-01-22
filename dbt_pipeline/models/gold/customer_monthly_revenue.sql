{{ config(
    materialized='table',
    schema='gold'
) }}

WITH monthly_summary AS (
    SELECT 
        c.customer_name,
        date_format(o.order_date, 'yyyy-MM') AS revenue_month,
        SUM(p.amount) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM {{ ref('silver_customers') }} c
    JOIN {{ ref('silver_orders') }} o ON c.customer_id = o.customer_id
    JOIN {{ ref('silver_payments') }} p ON o.order_id = p.order_id
    GROUP BY 1, 2
)

SELECT 
    *,
    DENSE_RANK() OVER (
        PARTITION BY revenue_month 
        ORDER BY total_revenue DESC
    ) AS rank_by_customer
FROM monthly_summary