import sqlite3

DB_PATH = "analytics.db"

with sqlite3.connect(DB_PATH) as conn:
    tables = conn.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name
    """).fetchall()

    print("=== Tables ===")
    for (table_name,) in tables:
        print(f"\n{table_name}")

        count = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0]
        print(f"Rows: {count}")

    print("\n=== Warehouse Schema ===")
    for table in ("dim_customers", "fct_orders"):
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name=?",
            (table,)
        ).fetchone()

        if exists:
            columns = conn.execute(
                f'PRAGMA table_info("{table}")'
            ).fetchall()
            print(f"\n{table}:")
            for column in columns:
                print(f"  {column[1]} ({column[2]})")
        else:
            print(f"\nMISSING TABLE: {table}")