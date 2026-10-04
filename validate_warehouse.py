
import sqlite3

with sqlite3.connect("analytics.db") as conn:
    conn.row_factory = sqlite3.Row

    checks = {
        "Orphan orders": """
            SELECT o.order_id, o.customer_id
            FROM fct_orders o
            LEFT JOIN dim_customers c
                ON c.customer_id = o.customer_id
            WHERE c.customer_id IS NULL
        """,
        "Duplicate customer IDs": """
            SELECT customer_id, COUNT(*) AS n
            FROM dim_customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        """,
        "Duplicate order IDs": """
            SELECT order_id, COUNT(*) AS n
            FROM fct_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """,
        "Invalid order amounts": """
            SELECT order_id, total_amount, usd_amount
            FROM fct_orders
            WHERE total_amount <= 0 OR usd_amount <= 0
        """,
        "Invalid USD calculations": """
            SELECT order_id, total_amount,
                   exchange_rate_used, usd_amount
            FROM fct_orders
            WHERE ABS(
                total_amount * exchange_rate_used - usd_amount
            ) > 0.01
        """,
        "Quarantine counts by run": """
            SELECT run_id, COUNT(*) AS rejected_rows
            FROM etl_quarantine
            GROUP BY run_id
        """,
    }

    for title, sql in checks.items():
        rows = conn.execute(sql).fetchall()
        print(f"\n=== {title}: {len(rows)} finding(s) ===")

        for row in rows:
            print(dict(row))

        if not rows:
            print("No issues found.")