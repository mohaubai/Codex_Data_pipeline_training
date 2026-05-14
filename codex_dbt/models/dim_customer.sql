{{ config(materialized='table') }}

WITH unique_customers AS (
	SELECT DISTINCT customer_name, city
	FROM {{ ref('clean_orders') }}
)
SELECT 
	customer_name, 
	city, 
	ROW_NUMBER() OVER (ORDER BY customer_name) AS customer_id 
FROM unique_customers

