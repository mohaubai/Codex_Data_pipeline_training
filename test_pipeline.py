from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg://postgres:postgres@localhost:5433/postgres")

def test_pipeline_no_negative_amounts():
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT COUNT(*) 
                                    FROM clean_orders
                                    WHERE amount <= 0;
                                    """))
        count = result.scalar()
        assert count == 0, f"Found {count} negative amounts in clean_orders"

def test_null_order_dates():
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT count(*) FROM clean_orders
                                        WHERE order_date IS NULL;
        """))
        count = result.scalar()
        assert count == 0, f"FOUND {count} No NULL dates data."

def test_duplicate_data_rows():
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT COUNT(*) FROM (
            SELECT order_id FROM clean_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) duplicates"""))
        count = result.scalar()
        assert count == 0, f"Found {count} No duplicates"