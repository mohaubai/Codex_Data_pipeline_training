{{ config(materialized='table') }}

SELECT order_id, customer_id, amount, order_date
FROM {{ ref('clean_orders') }}
JOIN {{ ref('dim_customer') }} ON clean_orders.customer_name = dim_customer.customer_name
AND clean_orders.city = dim_customer.city
