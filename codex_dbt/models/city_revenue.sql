{{ config(materialized='table') }}

SELECT 
	city, 
	SUM(amount) AS total_amount,
	COUNT(*) AS total_orders 
FROM {{ ref('clean_orders') }}
GROUP BY city
