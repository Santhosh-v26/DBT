{{ config(
    materialized='table',
    schema='gold'
) }}

WITH customers AS (
    SELECT
        customer_id,
        customer_name,
        email
    FROM {{ ref('silver_customers') }}
),

orders AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        amount,
        order_date
    FROM {{ ref('silver_orders') }}
)

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_spent,
    MAX(o.order_date) AS last_order_date
FROM customers AS c
LEFT JOIN orders AS o
    ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3
