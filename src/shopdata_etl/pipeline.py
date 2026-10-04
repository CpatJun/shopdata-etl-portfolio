"""Prefect orchestration for the ShopData portfolio pipeline."""
from __future__ import annotations
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
try:
    from prefect import flow, task
except ImportError:  # lets pure unit tests run before optional orchestration deps are installed
    def task(fn=None, **kwargs):
        def wrap(f): f.fn=f; return f
        return wrap(fn) if fn else wrap
    def flow(fn=None, **kwargs):
        def wrap(f): f.fn=f; return f
        return wrap(fn) if fn else wrap
from .config import SOURCE_DB, TARGET_DB
from .extract import extract_source
from .transform import transform_customers, transform_orders
from .load import load_incremental
LOGGER=logging.getLogger(__name__)

@task(name="extract-source", retries=2, retry_delay_seconds=2)
def extract_task(source_db: str | Path):
    return extract_source(source_db)

@task(name="transform-customers")
def transform_customers_task(df): return transform_customers(df)

@task(name="transform-orders")
def transform_orders_task(orders, rates, valid_customer_ids):
    return transform_orders(orders, rates, valid_customer_ids=valid_customer_ids)

@task(name="load-warehouse")
def load_task(customers, orders, rejected, target_db, run_id, started_at, source_customer_count, source_order_count):
    load_incremental(customers, orders, rejected, target_db, run_id, started_at, source_customer_count, source_order_count)

@flow(name="ShopData Analytics Engineering Pipeline", log_prints=True)
def etl_flow(source_db: str | Path = SOURCE_DB, target_db: str | Path = TARGET_DB) -> dict:
    started_at=datetime.now(timezone.utc).isoformat(); run_id=str(uuid.uuid4())
    LOGGER.info("Starting ETL run_id=%s", run_id)
    customers, orders, rates = extract_task(source_db)
    clean_customers=transform_customers_task(customers)
    customer_ids = set(clean_customers["customer_id"].astype(int).tolist())
    clean_orders, rejected=transform_orders_task(orders, rates, customer_ids)
    load_task(clean_customers, clean_orders, rejected, target_db, run_id, started_at, len(customers), len(orders))
    result={"run_id":run_id,"customers_upserted":len(clean_customers),"orders_upserted":len(clean_orders),"rejected_orders":len(rejected),"target_db":str(target_db)}
    LOGGER.info("ETL completed: %s", result)
    return result
