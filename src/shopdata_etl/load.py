"""Idempotent SQLite warehouse loading with per-run audit records."""
from __future__ import annotations
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS dim_customers (
      customer_id INTEGER PRIMARY KEY, full_name TEXT, email TEXT, phone TEXT, signup_date TEXT
    );
    CREATE TABLE IF NOT EXISTS fct_orders (
      order_id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, order_date TEXT NOT NULL,
      total_amount REAL NOT NULL, currency TEXT NOT NULL, status TEXT,
      exchange_rate_used REAL NOT NULL, usd_amount REAL NOT NULL,
      FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
    );
    CREATE TABLE IF NOT EXISTS etl_quarantine (
      quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
      record_type TEXT NOT NULL, record_key TEXT, rejection_reason TEXT NOT NULL,
      record_json TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS etl_run_log (
      run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
      source_customers INTEGER DEFAULT 0, source_orders INTEGER DEFAULT 0,
      customers_upserted INTEGER DEFAULT 0, orders_upserted INTEGER DEFAULT 0,
      rejected_orders INTEGER DEFAULT 0, error_message TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_fct_orders_customer_id ON fct_orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_fct_orders_order_date ON fct_orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_quarantine_run_id ON etl_quarantine(run_id);
    """)

def _upsert_frame(conn: sqlite3.Connection, table: str, df: pd.DataFrame, key: str) -> int:
    if df.empty: return 0
    columns = list(df.columns)
    quoted = ", ".join('"' + c.replace('"','""') + '"' for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(f'"{c}"=excluded."{c}"' for c in columns if c != key)
    sql = f'INSERT INTO "{table}" ({quoted}) VALUES ({placeholders}) ON CONFLICT("{key}") DO UPDATE SET {updates}'
    conn.executemany(sql, [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False, name=None)])
    return len(df)

def load_incremental(customers: pd.DataFrame, orders: pd.DataFrame, rejected: pd.DataFrame, db_path: str | Path, run_id: str, started_at: str, source_customer_count: int, source_order_count: int) -> None:
    path = Path(db_path).resolve(); path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _ensure_schema(conn)
        try:
            conn.execute("BEGIN")
            conn.execute("INSERT INTO etl_run_log(run_id,started_at,status,source_customers,source_orders) VALUES (?,?,?,?,?)", (run_id, started_at, "running", source_customer_count, source_order_count))
            existing_customer_ids = {int(row[0]) for row in conn.execute("SELECT customer_id FROM dim_customers")}
            incoming_customer_ids = set(pd.to_numeric(customers["customer_id"], errors="coerce").dropna().astype(int)) if not customers.empty else set()
            available_customer_ids = existing_customer_ids | incoming_customer_ids
            if not orders.empty:
                order_customer_ids = set(pd.to_numeric(orders["customer_id"], errors="coerce").dropna().astype(int))
                orphan_ids = order_customer_ids - available_customer_ids
                if orphan_ids:
                    raise ValueError(f"Refusing to load orders for missing customers: {sorted(orphan_ids)}")
            customers_written = _upsert_frame(conn, "dim_customers", customers, "customer_id")
            orders_written = _upsert_frame(conn, "fct_orders", orders, "order_id")
            now = datetime.now(timezone.utc).isoformat()
            quarantine_rows = []
            for row in rejected.to_dict("records"):
                quarantine_rows.append((run_id, "order", str(row.get("order_id", "")), str(row["rejection_reason"]), json.dumps(row, default=str, ensure_ascii=False), now))
            conn.executemany("INSERT INTO etl_quarantine(run_id,record_type,record_key,rejection_reason,record_json,created_at) VALUES (?,?,?,?,?,?)", quarantine_rows)
            conn.execute("UPDATE etl_run_log SET finished_at=?,status='success',customers_upserted=?,orders_upserted=?,rejected_orders=? WHERE run_id=?", (now, customers_written, orders_written, len(rejected), run_id))
            conn.commit()
        except Exception as exc:
            conn.rollback()
            # Keep failure audit outside the failed transaction.
            now = datetime.now(timezone.utc).isoformat()
            conn.execute("INSERT OR REPLACE INTO etl_run_log(run_id,started_at,finished_at,status,source_customers,source_orders,error_message) VALUES (?,?,?,?,?,?,?)", (run_id, started_at, now, "failed", source_customer_count, source_order_count, str(exc)[:2000]))
            conn.commit()
            raise
