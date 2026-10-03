# Superstore Data Pipeline: Design

Date: 2026-10-03

## Goal

Build an ELT pipeline for the Superstore sales dataset as a learning/portfolio project. Raw Excel data is loaded into DuckDB, cleaned and validated, and modelled into a star schema plus analytics marts. Airflow orchestrates the run and dbt does the transformations. The project follows the same general idea as the public `Superstore_dbt_airflow_project` repo but is built from scratch and differs from it in layer naming, incremental loading, snapshots, a data quality gate, extra marts and run logging.

## Source data

- File: `Superstore.xlsx` (copied to `data/raw/Superstore.xlsx`).
- One sheet, `Sample - Superstore`, 9,994 data rows, 21 columns: Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Customer Name, Segment, Country, City, State, Postal Code, Region, Product ID, Category, Sub-Category, Product Name, Sales, Quantity, Discount, Profit.
- Dates are stored as Excel serial numbers (for example 42682 = 2016-11-08).
- `Postal Code` is numeric in the file and must be treated as text.

## Stack

- Orchestration: Apache Airflow 3.x, run with Docker Compose (Docker Desktop is required and is not yet installed on the dev machine).
- Transformation: dbt Core with the `dbt-duckdb` adapter, installed inside the Airflow image and run through `BashOperator`.
- Warehouse: a single DuckDB file shared through a mounted volume.
- Loader: Python with pandas and openpyxl.

## Repository layout

```
superstore_project/
├── airflow/
│   ├── dags/superstore_pipeline.py
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── requirements.txt
├── data/raw/Superstore.xlsx
├── scripts/load_bronze.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── bronze/sources.yml
│   │   ├── silver/
│   │   ├── gold/
│   │   └── marts/
│   ├── snapshots/
│   └── tests/
├── tests/                  # pytest for the loader
├── docs/superpowers/specs/
└── README.md
```

## Layers

### Bronze (`bronze` schema)

`scripts/load_bronze.py` reads the Excel file and writes `bronze.raw_superstore`.

- Column names are only normalised to snake_case. Values are not cleaned.
- The table is created on first run. Later runs append only rows whose `row_id` is not already present, so a file with extra rows loads incrementally and re-running with the same file adds nothing.
- Each loaded row carries `_batch_id`, `_loaded_at` and `_source_file`.
- The script returns the number of rows loaded so the DAG can log it.
- Missing file, missing sheet or missing expected columns raise a clear error.

### Silver (`silver` schema)

- `silver_superstore`: valid rows only, typed. Excel serials become `DATE`, `postal_code` is `VARCHAR`, numeric measures are `DOUBLE`/`INTEGER`, text fields are trimmed.
- `silver_rejects`: rows that break a rule, with a `reject_reason` column. Rules: `sales <= 0`, `ship_date < order_date`, null `row_id`/`order_id`/`customer_id`/`product_id`, `discount` outside 0 to 1.
- Snapshots `snap_customer` and `snap_product` use the `check` strategy and keep SCD2 history. The dataset is static, so each key has a single version until the source changes.

### Gold (`gold` schema)

- Dimensions: `dim_customer`, `dim_product`, `dim_location`, `dim_date`, each with a surrogate key.
- `fact_sales`: incremental model, `unique_key = row_id`, grain is one order line. Holds surrogate keys to the dimensions, order and ship dates, and the measures sales, quantity, discount and profit.

### Marts (`marts` schema)

- `mart_monthly_sales`: sales, profit and order count per month.
- `mart_category_region_profit`: profit and margin by category, sub-category and region.
- `mart_customer_rfm`: recency, frequency, monetary value and a segment label per customer.
- `mart_shipping_performance`: average days to ship by ship mode and region.

dbt docs are generated as part of the project.

### Ops (`ops` schema)

`ops.pipeline_run_log`: one row per DAG run with `batch_id`, `status`, `rows_loaded`, `rows_rejected`, `duration_seconds`, `failed_task`, `finished_at`.

## Quality gate

- Source tests on bronze: `row_id` not null and unique, key columns not null.
- Silver tests run before gold is built. Any failure stops the DAG.
- Reject-rate check: if rejected rows exceed a threshold (default 2% of loaded rows, configurable through an Airflow Variable), the run fails.
- Gold tests: unique and not-null keys, relationships from `fact_sales` to every dimension, and reconciliation (fact row count equals valid silver row count, and total sales matches within a small tolerance).

## DAG `superstore_pipeline`

Airflow 3 TaskFlow API, daily schedule, `catchup=False`, 2 retries with a short delay.

`check_source_file` -> `dbt_debug` -> `load_bronze` -> `test_bronze` -> `build_silver` -> `snapshot` -> `test_silver` (gate) -> `build_gold` -> `test_gold` -> `log_run`

- An `on_failure_callback` writes a failed row to `ops.pipeline_run_log` with the failing task, so failures are recorded even when `log_run` never runs.
- No email or chat alerts, since those need credentials. The callback is one function that can be extended later.
- dbt tasks use selectors by layer so each task builds and tests only its own layer.

## Docker setup

- One custom Airflow image: official Airflow base, plus dbt-duckdb, pandas and openpyxl from `requirements.txt`.
- Compose mounts `dags/`, `scripts/`, `dbt_project/`, `data/` and a `warehouse/` volume that holds the DuckDB file.
- `profiles.yml` points to the DuckDB file path inside the container.
- Only one task writes to DuckDB at a time (DuckDB allows a single writer), so tasks run sequentially.

## Testing

- pytest for the loader: first load, re-run adds zero rows, extra rows append, bad file raises.
- dbt tests as listed in the quality gate.
- Manual end-to-end check: trigger the DAG, expect `fact_sales` = 9,994 rows and a `success` row in `ops.pipeline_run_log`.

## Code style

Plain, readable code in a human voice: short functions, natural names, comments only where the reason is not obvious, no boilerplate docstrings. README in original wording.

## Out of scope

Dashboard/BI layer, email or chat alerting, CI/CD, cloud deployment.

## Prerequisite

Install Docker Desktop before the end-to-end run. Python 3.14 is on the host, but the pipeline runs in containers, so host Python packages are only needed for running the loader tests locally.
