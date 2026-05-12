{{ config(materialized='table') }}

SELECT * FROM {{source('codex_de', 'raw_orders')}}
WHERE amount > 0 AND order_date IS NOT NULL
