
import sqlite3

with sqlite3.connect("analytics.db") as conn:
    conn.row_factory = sqlite3.Row

    checks = {
        "Recent ETL runs": """
            SELECT * FROM etl_run_log
            ORDER BY rowid DESC
            LIMIT 10
        """,
        "Quarantine summary": """
            SELECT *
            FROM etl_quarantine
            LIMIT 15
        """,
        "Customer sample": """
            SELECT * FROM dim_customers
            LIMIT 5
        """,
        "Order sample": """
            SELECT * FROM fct_orders
            LIMIT 10
        """,
    }

    for title, sql in checks.items():
        print(f"\n=== {title} ===")
        rows = conn.execute(sql).fetchall()

        if not rows:
            print("(no rows)")
            continue

        print(" | ".join(rows[0].keys()))
        for row in rows:
            print(" | ".join(str(value) for value in row))