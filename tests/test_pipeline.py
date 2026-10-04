import sqlite3
import pandas as pd
import pytest
from shopdata_etl.transform import standardize_phone, transform_customers, transform_orders
from shopdata_etl.load import load_incremental

def test_standardize_phone():
    assert standardize_phone("+1 (555) 123-4567") == "15551234567"
    assert standardize_phone(None) is None
    assert standardize_phone("---") is None

def test_customer_dedup_keeps_latest_signup():
    df=pd.DataFrame({"customer_id":[1,1,2],"full_name":["Old","New","Bob"],"email":[None," NEW@EXAMPLE.COM ",None],"phone":["+1 22","(33) 44",None],"signup_date":["2023-01-01","2023-03-01","2023-02-01"]})
    out=transform_customers(df).set_index("customer_id")
    assert out.loc[1,"full_name"] == "New"
    assert out.loc[1,"email"] == "new@example.com"
    assert out.loc[2,"email"] == "unknown@domain.com"

def test_currency_conversion_and_missing_rate_quarantine():
    orders=pd.DataFrame({"order_id":[1,2,3],"customer_id":[10,10,11],"order_date":["2023-01-01"]*3,"total_amount":[100,50,-1],"currency":["EUR","JPY","USD"],"status":["COMPLETE"]*3})
    rates=pd.DataFrame({"date":["2023-01-01"],"currency":["EUR"],"rate_to_usd":[1.1]})
    valid,rejected=transform_orders(orders,rates)
    assert valid.set_index("order_id").loc[1,"usd_amount"] == 110
    assert set(rejected.rejection_reason) == {"missing_exchange_rate","invalid_or_nonpositive_amount"}

def test_duplicate_order_ids_quarantines_all_duplicates():
    orders=pd.DataFrame({"order_id":[7,7],"customer_id":[1,1],"order_date":["2023-01-01"]*2,"total_amount":[10,20],"currency":["USD","USD"]})
    rates=pd.DataFrame(columns=["date","currency","rate_to_usd"])
    valid,rejected=transform_orders(orders,rates)
    assert valid.empty and len(rejected)==2
    assert set(rejected.rejection_reason)=={"duplicate_order_id"}

def test_duplicate_rate_key_fails_fast():
    orders=pd.DataFrame({"order_id":[1],"customer_id":[1],"order_date":["2023-01-01"],"total_amount":[10],"currency":["EUR"]})
    rates=pd.DataFrame({"date":["2023-01-01"]*2,"currency":["EUR"]*2,"rate_to_usd":[1.1,1.2]})
    with pytest.raises(ValueError, match="Duplicate exchange-rate keys"):
        transform_orders(orders,rates)

def test_incremental_load_is_idempotent_and_auditable(tmp_path):
    db=tmp_path/"analytics.db"
    customers=pd.DataFrame({"customer_id":[1],"full_name":["Alice"],"email":["a@example.com"],"phone":["123"],"signup_date":["2023-01-01"]})
    orders=pd.DataFrame({"order_id":[1],"customer_id":[1],"order_date":["2023-01-02"],"total_amount":[10.0],"currency":["USD"],"status":["COMPLETE"],"exchange_rate_used":[1.0],"usd_amount":[10.0]})
    rejected=pd.DataFrame({"order_id":[2],"rejection_reason":["missing_exchange_rate"]})
    for run_id in ("run-1","run-2"):
        load_incremental(customers,orders,rejected,db,run_id,"2023-01-01T00:00:00+00:00",1,2)
    with sqlite3.connect(db) as conn:
        assert conn.execute("select count(*) from dim_customers").fetchone()[0]==1
        assert conn.execute("select count(*) from fct_orders").fetchone()[0]==1
        assert conn.execute("select count(*) from etl_run_log where status='success'").fetchone()[0]==2
        assert conn.execute("select count(*) from etl_quarantine").fetchone()[0]==2

def test_sample_pipeline(tmp_path):
    from shopdata_etl.pipeline import etl_flow
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    result=getattr(etl_flow,"fn",etl_flow)(source_db=root/"shopdata.db",target_db=tmp_path/"analytics.db")
    assert result["customers_upserted"] > 0
    assert result["orders_upserted"] > 0
    with sqlite3.connect(tmp_path/"analytics.db") as conn:
        assert conn.execute("select count(*) from etl_run_log where status='success'").fetchone()[0]==1

def test_invalid_identifiers_are_quarantined():
    orders=pd.DataFrame({"order_id":["not-an-id", 2],"customer_id":[1, "bad-customer"],"order_date":["2023-01-01"]*2,"total_amount":[10,20],"currency":["USD","USD"]})
    rates=pd.DataFrame(columns=["date","currency","rate_to_usd"])
    valid,rejected=transform_orders(orders,rates)
    assert valid.empty
    assert set(rejected.rejection_reason)=={"invalid_order_id","missing_or_invalid_customer_id"}


def test_orphan_customer_orders_are_quarantined():
    orders = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [10, 99],
        "order_date": ["2023-01-01", "2023-01-01"],
        "total_amount": [25, 40],
        "currency": ["USD", "USD"],
    })
    rates = pd.DataFrame(columns=["date", "currency", "rate_to_usd"])
    valid, rejected = transform_orders(orders, rates, valid_customer_ids={10})
    assert valid["order_id"].tolist() == [1]
    assert rejected[["order_id", "rejection_reason"]].to_dict("records") == [
        {"order_id": 2, "rejection_reason": "customer_not_found"}
    ]


def test_non_integer_customer_id_is_quarantined():
    orders = pd.DataFrame({
        "order_id": [1],
        "customer_id": [1.5],
        "order_date": ["2023-01-01"],
        "total_amount": [10],
        "currency": ["USD"],
    })
    rates = pd.DataFrame(columns=["date", "currency", "rate_to_usd"])
    valid, rejected = transform_orders(orders, rates, valid_customer_ids={1})
    assert valid.empty
    assert rejected.iloc[0]["rejection_reason"] == "missing_or_invalid_customer_id"


def test_loader_rejects_orphan_order(tmp_path):
    db = tmp_path / "warehouse.db"
    customers = pd.DataFrame({
        "customer_id": [1], "full_name": ["Alice"], "email": ["a@example.com"],
        "phone": ["123"], "signup_date": ["2023-01-01"],
    })
    orders = pd.DataFrame({
        "order_id": [99], "customer_id": [404], "order_date": ["2023-01-02"],
        "total_amount": [10.0], "currency": ["USD"], "status": "COMPLETE",
        "exchange_rate_used": [1.0], "usd_amount": [10.0],
    })
    rejected = pd.DataFrame(columns=["order_id", "rejection_reason"])
    with pytest.raises(ValueError, match="missing customers"):
        load_incremental(customers, orders, rejected, db, "orphan-run",
                         "2023-01-01T00:00:00+00:00", 1, 1)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM fct_orders").fetchone()[0] == 0
        assert conn.execute("SELECT status FROM etl_run_log WHERE run_id='orphan-run'").fetchone()[0] == "failed"
