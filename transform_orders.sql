DROP TABLE IF EXISTS clean_orders;
DROP TABLE IF EXISTS city_revenue;
DROP TABLE IF EXISTS rejected_orders;

CREATE TABLE clean_orders AS
SELECT * FROM csv_orders
WHERE amount > 0 AND order_date IS NOT NULL;

CREATE TABLE rejected_orders AS
SELECT * FROM csv_orders
WHERE amount <= 0 OR order_date IS NULL;

CREATE TABLE city_revenue AS
SELECT city, SUM(amount) AS total_amount, COUNT(*) AS total_orders
FROM clean_orders
GROUP BY city;