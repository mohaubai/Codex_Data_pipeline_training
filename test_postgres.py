from sqlalchemy import create_engine, text
import pandas

engine = create_engine("postgresql+psycopg://postgres:postgres@postgres:5432/postgres")

df = pandas.read_csv('./orders.csv')
df['order_date'] = pandas.to_datetime(df['order_date'], errors='coerce')

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS csv_orders (
            order_id INT, 
            customer_name TEXT,
            city TEXT,
            amount INT,
            order_date DATE
        );
    """))

    df.to_sql('csv_orders', conn, if_exists='replace', index=False)

    result = conn.execute(text('SELECT COUNT(*) FROM csv_orders;'))
    count = result.scalar()

with engine.begin() as conn:
    # conn.execute(text("DROP TABLE IF EXISTS clean_orders;"))
    # conn.execute(text("DROP TABLE IF EXISTS rejected_orders;"))
    # conn.execute(text("DROP TABLE IF EXISTS city_revenue;"))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS clean_orders (
            order_id INT PRIMARY KEY,
            customer_name TEXT,
            city TEXT,
            amount INT,
            order_date DATE
        );
    """))

    conn.execute(text("""
        INSERT INTO clean_orders (order_id, customer_name, city, amount, order_date)
        SELECT * FROM csv_orders
        WHERE amount > 0 AND order_date IS NOT NULL
        ON CONFLICT (order_id) DO NOTHING;
    """))

    conn.execute(text("""CREATE TABLE IF NOT EXISTS rejected_orders (
        order_id INT PRIMARY KEY,
        customer_name TEXT,
        city TEXT,
        amount INT,
        order_date DATE
    );"""))

    conn.execute(text("""
        INSERT INTO rejected_orders (order_id, customer_name, city, amount, order_date)
        SELECT * FROM csv_orders
        WHERE amount <= 0 OR order_date IS NULL
        ON CONFLICT (order_id) DO NOTHING;
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS city_revenue (
            city TEXT UNIQUE,
            total_amount INT,
            total_rows INT
        );
    """))

    conn.execute(text("TRUNCATE city_revenue"))

    conn.execute(text("""
        INSERT INTO city_revenue (city, total_amount, total_rows)
        SELECT city, SUM(amount), COUNT(*) FROM clean_orders
        GROUP BY city
        ON CONFLICT (city) DO UPDATE
        SET total_amount = city_revenue.total_amount + EXCLUDED.total_amount,
        total_rows = city_revenue.total_rows + EXCLUDED.total_rows
    """))

    orders_greater_than_average = conn.execute(text("""
        WITH avg_calculation AS (
            SELECT AVG(amount) AS avg_amount
            FROM clean_orders
        )
        SELECT amount FROM clean_orders
        WHERE amount > (SELECT avg_amount FROM avg_calculation) 
    """))

    #CTEs
    customer_spending = conn.execute(text("""
        WITH each_customer_total AS (
            SELECT customer_name, SUM(amount) as total_amount
            FROM clean_orders
            GROUP BY customer_name
        ),
        average_amount AS (
            SELECT AVG(total_amount) as avg_amount
            FROM each_customer_total
        )
        SELECT customer_name, total_amount
        FROM each_customer_total
        WHERE total_amount > (SELECT avg_amount FROM average_amount)
    """))

    #window_functions - using RANK(), DENSE_RANK(), ROW_NUMBER()
    rank_cities_by_revenue = conn.execute(text("""
        SELECT
            city,
            total_amount,
            RANK() OVER (ORDER BY total_amount DESC) AS revenue_rank
        FROM city_revenue
    """))
    dense_rank_cities_by_revenue = conn.execute(text("""
        SELECT
            city,
            total_amount,
            DENSE_RANK() OVER (ORDER BY total_amount DESC) AS revenue_rank
        FROM city_revenue
    """))
    row_rank_cities_by_revenue = conn.execute(text("""
        SELECT
            city,
            total_amount,
            ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS revenue_rank
        FROM city_revenue
    """))

    #practical scenario for PARTITION BY()
    rank_each_customer_by_city = conn.execute(text("""
        SELECT customer_name, city, amount, RANK() OVER(PARTITION BY city ORDER BY amount DESC) as city_rank
        FROM clean_orders
    """))

    #LAG and LEAD
    previous_and_next_values = conn.execute(text("""
        WITH previous_amount_value AS (
            SELECT 
                customer_name,
                order_date,
                amount,
                LAG(amount) OVER (PARTITION BY customer_name ORDER BY order_date) AS previous_amount
            FROM clean_orders
        )
        SELECT customer_name,
            order_date,
            amount,
            previous_amount,
            amount - COALESCE(previous_amount, 0) AS difference
        FROM previous_amount_value
    """))

    add_flag_column = conn.execute(text("""
        SELECT
            customer_name,
            amount,
            CASE
                WHEN amount > 500 THEN 'High'
                WHEN amount <= 200 THEN 'Medium'
                ELSE 'Low'
            END AS Flag
        FROM clean_orders
    """))

    get_rows_clean_orders = conn.execute(text("""
        SELECT COUNT(*) FROM clean_orders;
    """))

    query_clean_orders = conn.execute(text("SELECT COUNT(*) FROM clean_orders;"))
    query_rejected_orders = conn.execute(text("SELECT COUNT(*) FROM rejected_orders;"))
    query_city_revenue = conn.execute(text("SELECT * FROM city_revenue;"))

#print("csv_orders table created")
# print(query_clean_orders.scalar())
#print(query_rejected_orders.scalar())
# print([{"city":r.city, "total_amount": r.total_amount, "total_count": r.total_rows} for r in query_city_revenue])
# print([{"amount": order.amount} for order in orders_greater_than_average])
# print([d for d in customer_spending])
# print([{'city': d.city, 'total_amount': d.total_amount, 'rank': d.revenue_rank} for d in rank_cities_by_revenue])
# print([{'city': d.city, 'total_amount': d.total_amount, 'rank': d.revenue_rank} for d in dense_rank_cities_by_revenue])
# print([{'city': d.city, 'total_amount': d.total_amount, 'rank': d.revenue_rank} for d in row_rank_cities_by_revenue])
# print([{'customer_name': d.customer_name, 'city': d.city, 'amount': d.amount, 'rank': d.city_rank} for d in rank_each_customer_by_city])
#print([d for d in previous_and_next_values])
#print([d for d in add_flag_column])
print(get_rows_clean_orders.scalar())