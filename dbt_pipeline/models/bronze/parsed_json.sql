{{ config(
    materialized = 'table',
    schema = 'bronze'
) }}

WITH parsed_json AS (
    SELECT
        from_json(
            json_data,
            'struct<
                order_id:int,
                customer:struct<
                    id:int,
                    name:string,
                    city:string
                >,
                amount:double,
                payment_method:string,
                items:array<struct<
                    product:string,
                    qty:int,
                    price:int
                >>,
                order_date:string
            >'
        ) AS parsed
    FROM {{ source('source', 'test_json') }}
)

SELECT
    parsed.order_id,
    parsed.customer.id AS customer_id,
    parsed.customer.name AS customer_name,
    parsed.customer.city AS customer_city,
    parsed.amount,
    parsed.payment_method,
    parsed.order_date,
    exploded.product,
    exploded.qty,
    exploded.price,
    exploded.qty * exploded.price AS item_total
FROM parsed_json
    LATERAL VIEW explode(parsed.items) as exploded;
