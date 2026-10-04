# ShopData Analytics Engineering Pipeline

![Tests](https://github.com/CpatJun/shopdata-etl-portfolio/actions/workflows/tests.yml/badge.svg)

A portfolio-focused ETL project demonstrating reliable extraction, transformation, idempotent warehouse loading with key-based upserts, data-quality quarantine, audit logging, and analytical SQL. It uses a reproducible SQLite sample dataset while emphasizing production-oriented engineering practices.

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
      ├── Clean customer dimension
      ├── Clean USD order facts
      └── Rejected rows + reason codes
          │
          ▼
   Transactional idempotent UPSERT
      ├── dim_customers
      ├── fct_orders
      ├── etl_quarantine
      └── etl_run_log
          │
          ▼
   clv_report.sql / BI consumption
```

## Key engineering choices

* **Idempotent loads:** Business keys (`customer_id`, `order_id`) are used for key-based upserts instead of replacing the entire warehouse.
* **Quarantine, don't silently guess:** Missing currencies and non-USD orders without valid daily exchange rates are quarantined rather than incorrectly treated as USD. Any fallback should be an explicit, approved business rule.
* **Auditability:** Each successful run records counts and status in `etl_run_log`. Rejected rows are recorded in `etl_quarantine` with a run ID, rejection reason, and source-row JSON.
* **Atomic writes:** Warehouse updates, quarantine inserts, and successful run status are committed in one transaction. Failed writes are rolled back, and failure status is recorded separately.
* **Read-only extraction:** The source database is opened in SQLite read-only mode.
* **Exact-date FX matching:** Exchange rates are matched by order date and currency. No silent previous-day carry-forward is performed.
* **Referential integrity:** Orders referencing customers absent from the cleaned customer dimension are quarantined before loading. The loader also checks for orphan references, and newly created warehouses define a foreign key from `fct_orders.customer_id` to `dim_customers.customer_id`.
* **Duplicate source keys:** Duplicate order IDs are quarantined rather than resolved arbitrarily, avoiding potentially incorrect revenue totals. Duplicate exchange-rate keys fail fast.
* **Orchestration:** Prefect 3 tasks and flows provide a foundation for scheduling, retries, and observability.

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

* `src/shopdata_etl/` — ETL implementation
* `tests/` — Automated tests
* `pipeline.py` — ETL command-line entry point
* `validate_warehouse.py` — Warehouse data-quality checks
* `inspect_warehouse.py` — Warehouse inspection and ETL run summary
* `clv_report.sql` — Customer lifetime value report
* `exploration.sql` — Source data exploration queries
* `.github/workflows/tests.yml` — Automated GitHub Actions tests

## Requirements

* Python 3.12+
* SQLite, included with Python
* Dependencies listed in `requirements.txt`

## Setup

### Windows PowerShell

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### macOS/Linux

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Run

Run the automated tests:

```bash
python -m pytest -q
```

Run the ETL pipeline:

```bash
python pipeline.py
```

The pipeline reads from `shopdata.db` and writes the analytics warehouse to `analytics.db` using the project's configured paths.

To override the database paths on macOS/Linux:

```bash
SHOPDATA_SOURCE_DB=/path/to/shopdata.db SHOPDATA_TARGET_DB=/path/to/analytics.db python pipeline.py
```

## Inspect the output

Open `analytics.db` in a SQLite client and run `clv_report.sql` to generate the customer-level analytical report.

Useful audit queries:

```sql
SELECT status, COUNT(*)
FROM etl_run_log
GROUP BY status;

SELECT rejection_reason, COUNT(*)
FROM etl_quarantine
GROUP BY rejection_reason
ORDER BY COUNT(*) DESC;

SELECT *
FROM etl_run_log
ORDER BY started_at DESC;
```

## Sample analytical output

The `clv_report.sql` query produces a customer-level report from the analytics warehouse.

| Customer        | Completed orders | Historical value (USD) |
| --------------- | ---------------: | ---------------------: |
| Alice Smith     |                2 |                 486.00 |
| Fiona Gallagher |                1 |                 450.00 |
| Diana Prince    |                2 |                 389.50 |
| Bob Jones       |                1 |                 220.00 |

The report also includes customer cohorts based on signup month. Historical value represents the total USD value of completed orders in the sample dataset; it is not a prediction of future customer spending.

To reproduce the report, run `clv_report.sql` against `analytics.db`.

## Data quality policy

* **Customers:** Deduplicated by `customer_id`, keeping the latest valid `signup_date`. Missing emails use `unknown@domain.com`; phone values are normalized to digits or `NULL`.
* **Orders:** Rows with missing or duplicate IDs, missing/invalid/orphan customer IDs, invalid dates, non-positive or non-numeric amounts, missing currencies, or missing exact-date FX rates are quarantined with reason codes.
* **Currency conversion:** USD uses an exchange rate of `1.0`. Other currencies require a positive exact-date exchange rate. USD amounts are rounded to two decimal places.
* **Referential integrity:** Orphan customer references are quarantined before loading and checked again at the load boundary.

## Known limitations / next steps

1. The sample source has no `updated_at` column, so this version re-reads the source snapshot and performs idempotent key-based upserts. It is not yet a watermark-based incremental extractor.
2. For larger datasets, use a server database or data warehouse and bulk loading instead of SQLite.
3. Add source freshness checks, alerting, and data-quality coverage thresholds before production deployment.
4. Add Docker deployment and a scheduled Prefect deployment after the local flow is stable.

## Data exploration

Run `exploration.sql` against `shopdata.db` to inspect duplicate customers, missing contact fields, invalid amounts, incomplete order fields, orphan customer references, duplicate exchange-rate keys, and invalid exchange rates.
