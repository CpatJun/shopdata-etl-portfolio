
# ShopData Analytics Engineering Pipeline

![Tests](https://github.com/CpatJun/shopdata-etl-portfolio/actions/workflows/tests.yml/badge.svg)

A portfolio-focused ETL project demonstrating reliable extraction, transformation, incremental loading, data-quality quarantine, audit logging, and analytical SQL. It uses the provided SQLite source as a reproducible sample, while the design emphasizes production-oriented behaviors.
  

## Architecture

```text

Read-only shopdata.db
   ├── raw_customers
   ├── raw_orders
   └── exchange_rates
          │
          ▼
   Extract (SQLite read-only)
          ▼
   Transform + data-quality rules
      ├── clean customer dimension
      ├── clean USD order facts
      └── rejected rows + reason codes
          │
          ▼
   Transactional incremental UPSERT
      ├── dim_customers
      ├── fct_orders
      ├── etl_quarantine
      └── etl_run_log
          │
          ▼
   clv_report.sql / BI consumption
```

## Key engineering choices

- **Idempotent loads:** business keys (`customer_id`, `order_id`) are primary keys and rows are upserted instead of replacing the whole warehouse.
- **Quarantine, don't silently guess:** missing currency and non-USD orders with missing/invalid daily exchange rates are quarantined rather than treated as USD. This avoids silently changing the meaning of revenue; any fallback should be an explicit, approved business rule.
- **Auditability:** each successful run records counts and status in `etl_run_log`; rejected rows are recorded with a run ID, reason, and source-row JSON in `etl_quarantine`.
- **Atomic writes:** warehouse updates, quarantine inserts, and success status are committed in one transaction. Failed writes are rolled back and a failure status is recorded separately.
- **Read-only extraction:** source database is opened in SQLite read-only mode.
- **Exact-date FX matching:** rates are matched by order calendar date and currency. No silent previous-day carry-forward is performed.
- **Referential integrity:** orders referencing customer IDs absent from the cleaned customer dimension are quarantined before loading. The loader also checks for orphan references, and newly created warehouses define a foreign key from `fct_orders.customer_id` to `dim_customers.customer_id`.
- **Duplicate source keys:** duplicate order IDs are all quarantined because choosing one arbitrarily could alter revenue. Duplicate exchange-rate keys fail fast.
- **Orchestration:** Prefect 3 tasks and flow provide a clear place to add scheduling, retries, and observability.



## Project layout

```text
shopdata-etl-portfolio/
├── .github/
│   └── workflows/
│       └── tests.yml
├── src/
│   └── shopdata_etl/
│       ├── __init__.py
│       ├── config.py
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── pipeline.py
├── tests/
│   └── test_pipeline.py
├── pipeline.py
├── validate_warehouse.py
├── inspect_warehouse.py
├── check_db.py
├── shopdata.db
├── clv_report.sql
├── clv_report.csv
├── exploration.sql
├── requirements.txt
├── pyproject.toml
├── .gitignore
└── README.md
```

- `src/shopdata_etl/` — ETL implementation
- `tests/` — automated tests
- `pipeline.py` — ETL command-line entry point
- `validate_warehouse.py` — warehouse data-quality checks
- `clv_report.sql` — customer lifetime value report
- `exploration.sql` — source data exploration
- `.github/workflows/tests.yml` — automated GitHub Actions tests

## Requirements

- Python 3.12+
- SQLite (included with Python)
- Dependencies in `requirements.txt`

## Setup

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

macOS/Linux:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run

```bash
python -m pytest -q
python pipeline.py
```

Override paths with environment variables if needed:

```bash
# macOS/Linux
SHOPDATA_SOURCE_DB=/path/to/shopdata.db SHOPDATA_TARGET_DB=/path/to/analytics.db python pipeline.py
```

## Inspect the output

Open `analytics.db` in a SQLite client, then run `clv_report.sql`. Useful audit queries:

```sql
SELECT status, COUNT(*) FROM etl_run_log GROUP BY status;
SELECT rejection_reason, COUNT(*) FROM etl_quarantine GROUP BY rejection_reason ORDER BY COUNT(*) DESC;
SELECT * FROM etl_run_log ORDER BY started_at DESC;
```

## Data quality policy

- Customers are deduplicated by `customer_id`, keeping the latest valid `signup_date`. Missing emails use `unknown@domain.com`; phone values are normalized to digits or NULL.
- Orders with missing/duplicate IDs, missing/invalid/orphan customer IDs, invalid dates, non-positive/non-numeric amounts, missing currency, or missing exact-date FX rates are quarantined with reason codes.
- USD uses an exchange rate of 1.0. Other currencies require a positive exact-date rate. USD amounts are rounded to two decimals.
- Orphan customer references are quarantined before load and checked again at the load boundary.

## Known limitations / next steps

1. The sample source has no `updated_at` column, so this version re-reads the source snapshot and performs idempotent key-based upserts; it is not a watermark-based incremental extractor yet.
2. For large datasets, use a server database/warehouse and bulk loading rather than SQLite.
3. Add source freshness checks, alerting, and coverage thresholds before production deployment.
4. Add Docker deployment and a scheduled Prefect deployment after the local flow is stable.


## Data exploration

Run `exploration.sql` against `shopdata.db`. The queries inspect duplicate customers, missing contact fields, invalid amounts, incomplete order fields, orphan customer references, duplicate rates, and invalid rates.
