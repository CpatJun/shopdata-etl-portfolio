"""Pure transformation functions: no database or orchestration dependency."""
from __future__ import annotations
import logging
import re
import pandas as pd

LOGGER = logging.getLogger(__name__)

def standardize_phone(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits or None

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    required = {"customer_id", "full_name", "email", "phone", "signup_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Customer source missing columns: {sorted(missing)}")
    result = df.copy()
    result["customer_id"] = pd.to_numeric(result["customer_id"], errors="coerce")
    result["signup_date"] = pd.to_datetime(result["signup_date"], errors="coerce", utc=True)
    result["email"] = result["email"].fillna("").astype(str).str.strip().str.lower()
    result["email"] = result["email"].replace("", "unknown@domain.com")
    result["phone"] = result["phone"].map(standardize_phone)
    result = result.dropna(subset=["customer_id", "signup_date"])
    result["customer_id"] = result["customer_id"].astype("int64")
    result = result.sort_values(["customer_id", "signup_date"], kind="stable")
    result = result.drop_duplicates("customer_id", keep="last")
    result["signup_date"] = result["signup_date"].dt.strftime("%Y-%m-%d")
    return result.reset_index(drop=True)

def _prepare_rates(rates: pd.DataFrame) -> pd.DataFrame:
    required = {"currency", "date", "rate_to_usd"}
    missing = required - set(rates.columns)
    if missing:
        raise ValueError(f"Exchange-rate source missing columns: {sorted(missing)}")
    result = rates[["date", "currency", "rate_to_usd"]].copy()
    result["rate_date"] = pd.to_datetime(result["date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    result["currency_key"] = result["currency"].astype("string").str.strip().str.upper()
    result["rate"] = pd.to_numeric(result["rate_to_usd"], errors="coerce")
    result = result.dropna(subset=["rate_date", "currency_key"])
    result = result[(result["currency_key"] != "") & result["rate"].notna() & (result["rate"] > 0)]
    dupes = result.duplicated(["rate_date", "currency_key"], keep=False)
    if dupes.any():
        keys = result.loc[dupes, ["rate_date", "currency_key"]].drop_duplicates().to_dict("records")
        raise ValueError(f"Duplicate exchange-rate keys: {keys[:5]}")
    return result[["rate_date", "currency_key", "rate"]]

def transform_orders(
    orders: pd.DataFrame,
    rates: pd.DataFrame,
    valid_customer_ids: set[int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return valid and rejected orders under explicit portfolio quality rules.

    USD uses a rate of 1.0. Other currencies require an exact-date positive rate.
    Missing currency/rates and orphan customer references are quarantined rather than
    silently imputed, preserving traceability and avoiding misleading revenue values.
    If valid_customer_ids is omitted, customer existence is not checked here.
    """
    required = {"order_id", "customer_id", "order_date", "total_amount", "currency"}
    missing = required - set(orders.columns)
    if missing:
        raise ValueError(f"Order source missing columns: {sorted(missing)}")
    df = orders.copy().reset_index(drop=True)
    df["_row_id"] = range(len(df))
    df["_date"] = pd.to_datetime(df["order_date"], errors="coerce", utc=True)
    df["_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    df["_currency"] = df["currency"].astype("string").str.strip().str.upper()
    order_id_numeric = pd.to_numeric(df["order_id"], errors="coerce")
    customer_id_numeric = pd.to_numeric(df["customer_id"], errors="coerce")
    duplicate_ids = df["order_id"].notna() & df["order_id"].duplicated(keep=False)
    reasons: dict[int, str] = {}
    for idx, row in df.iterrows():
        if pd.isna(row["order_id"]): reasons[idx] = "missing_order_id"
        elif pd.isna(order_id_numeric.loc[idx]): reasons[idx] = "invalid_order_id"
        elif bool(duplicate_ids.loc[idx]): reasons[idx] = "duplicate_order_id"
        elif pd.isna(customer_id_numeric.loc[idx]) or float(customer_id_numeric.loc[idx]) % 1 != 0: reasons[idx] = "missing_or_invalid_customer_id"
        elif valid_customer_ids is not None and int(customer_id_numeric.loc[idx]) not in valid_customer_ids: reasons[idx] = "customer_not_found"
        elif pd.isna(row["_date"]): reasons[idx] = "invalid_order_date"
        elif pd.isna(row["_amount"]) or row["_amount"] <= 0: reasons[idx] = "invalid_or_nonpositive_amount"
        elif pd.isna(row["_currency"]) or not str(row["_currency"]).strip(): reasons[idx] = "missing_currency"
    candidates = df.loc[[i for i in df.index if i not in reasons]].copy()
    candidates["order_date"] = candidates["_date"].dt.strftime("%Y-%m-%d")
    candidates["currency"] = candidates["_currency"].astype(str)
    prepared_rates = _prepare_rates(rates)
    candidates = candidates.merge(prepared_rates, left_on=["order_date", "currency"], right_on=["rate_date", "currency_key"], how="left", validate="many_to_one")
    for idx, row in candidates.iterrows():
        if row["currency"] != "USD" and pd.isna(row["rate"]):
            reasons[int(row["_row_id"])] = "missing_exchange_rate"
    valid = candidates[~candidates["_row_id"].isin(reasons)].copy()
    valid.loc[valid["currency"] == "USD", "rate"] = 1.0
    valid["exchange_rate_used"] = valid["rate"].astype(float)
    valid["usd_amount"] = (valid["_amount"] * valid["exchange_rate_used"]).round(2)
    valid["order_date"] = valid["_date"].dt.strftime("%Y-%m-%d")
    source_cols = list(orders.columns)
    valid = valid[source_cols + ["exchange_rate_used", "usd_amount"]].reset_index(drop=True)
    rejected = df.loc[sorted(reasons), list(orders.columns)].copy() if reasons else df.iloc[0:0][list(orders.columns)].copy()
    rejected["rejection_reason"] = [reasons[i] for i in rejected.index]
    rejected = rejected.reset_index(drop=True)
    if not rejected.empty:
        LOGGER.warning("Quarantined %d order rows: %s", len(rejected), rejected["rejection_reason"].value_counts().to_dict())
    return valid, rejected
