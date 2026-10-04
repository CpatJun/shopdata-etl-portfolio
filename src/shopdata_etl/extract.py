"""Read-only extraction from the legacy SQLite source."""
from pathlib import Path
import sqlite3
import pandas as pd

def extract_source(db_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = Path(db_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Source database not found: {path}")
    uri = f"file:{path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        customers = pd.read_sql_query("SELECT * FROM vw_raw_customers", conn)
        orders = pd.read_sql_query("SELECT * FROM vw_raw_orders", conn)
        rates = pd.read_sql_query("SELECT * FROM vw_exchange_rates", conn)
    return customers, orders, rates
