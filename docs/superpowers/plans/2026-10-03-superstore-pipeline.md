# Superstore Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dockerised Airflow + dbt + DuckDB ELT pipeline that loads `Superstore.xlsx` into bronze, cleans and validates it into silver, and models it into a star schema plus analytics marts, with a quality gate and a run log.

**Architecture:** A pandas loader appends new rows to `bronze.raw_superstore`. dbt builds `silver` (typed rows, rejects, SCD2 snapshots), `gold` (dimensions + incremental `fact_sales`) and `marts`. An Airflow 3 DAG runs everything as shell tasks inside one image that holds a dedicated venv with dbt-duckdb, pandas and openpyxl. A small ops script checks the reject rate and writes `ops.pipeline_run_log`.

**Tech Stack:** Python 3.12 (in the container), Apache Airflow 3.1 (LocalExecutor, Postgres metadata DB), dbt Core >= 1.10 with dbt-duckdb, DuckDB, pandas, openpyxl, pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-03-superstore-pipeline-design.md`

## Global Constraints

- Orchestration: Apache Airflow 3.x run with Docker Compose. Docker Desktop must be installed.
- dbt Core with the `dbt-duckdb` adapter, installed inside the Airflow image and run through bash tasks.
- Warehouse: one DuckDB file on a mounted volume. DuckDB allows a single writer, so tasks run sequentially and `max_active_runs=1`.
- Layers are named by schema: `bronze`, `silver`, `gold`, `marts`, `ops`.
- Source: sheet `Sample - Superstore`, 9,994 rows, 21 columns. `Postal Code` is text.
- Reject rules: `sales <= 0`, `ship_date < order_date`, null `row_id`/`order_id`/`customer_id`/`product_id`, `discount` outside 0 to 1.
- Reject-rate gate: default 2%, configurable through the Airflow Variable `reject_rate_threshold`.
- DAG `superstore_pipeline`: daily, `catchup=False`, 2 retries, TaskFlow API.
- No email or chat alerts. Failures are recorded by an `on_failure_callback`.
- Code style: short functions, natural names, comments only where the reason is not obvious, no boilerplate docstrings.
- Out of scope: dashboard, alerting integrations, CI/CD, cloud deployment.

## Review Focus

1. Postal codes with a leading zero (`01001`) must stay `01001`, not become `1001`. Pinned in Task 2.
2. Running the loader twice on the same file must add zero rows. Pinned in Task 2.
3. A source file with duplicate `row_id` values must fail loudly instead of loading silently. Pinned in Task 2.
4. A file with a header row and no data must fail with a clear message, not create an empty table. Pinned in Task 2.
5. A run that fails before bronze or silver exist (for example a missing file) must still write a `failed` row to `ops.pipeline_run_log` with zero counts. Pinned in Task 3.

---

## File Structure

| File | Responsibility |
|---|---|
| `scripts/load_bronze.py` | Read Excel, validate, append new rows to `bronze.raw_superstore` |
| `scripts/pipeline_ops.py` | Reject-rate check and run logging (`ops.pipeline_run_log`) |
| `scripts/query.py` | Print the result of one SQL query against the warehouse |
| `scripts/in_container.sh` | Run a shell command inside the Airflow image with the project mounted |
| `tests/test_load_bronze.py`, `tests/test_pipeline_ops.py` | pytest for the scripts |
| `airflow/Dockerfile`, `airflow/requirements.txt`, `airflow/docker-compose.yaml` | Image and services |
| `airflow/dags/superstore_pipeline.py` | The DAG |
| `dbt_project/...` | dbt models, snapshots, tests, macros, profile |
| `README.md` | How to run and how it works |

Run every command from `C:\Users\bahr\Desktop\Claude\superstore_project` in Git Bash.

---

### Task 1: Project scaffold

**Files:**
- Create: `.gitignore`, `pytest.ini`, `warehouse/.gitkeep`, `data/raw/Superstore.xlsx`

**Interfaces:**
- Produces: folder layout, a host `.venv` with pytest, pandas, openpyxl, duckdb.

- [ ] **Step 1: Create the folders, init git, copy the data**

```bash
mkdir -p airflow/dags scripts tests data/raw warehouse dbt_project/models/bronze dbt_project/models/silver dbt_project/models/gold dbt_project/models/marts dbt_project/snapshots dbt_project/tests dbt_project/macros
cp /c/Users/bahr/Downloads/Superstore.xlsx data/raw/Superstore.xlsx
touch warehouse/.gitkeep
git init
```

- [ ] **Step 2: Write `.gitignore`**

```
.venv/
__pycache__/
.pytest_cache/
warehouse/*.duckdb
warehouse/*.duckdb.wal
dbt_project/target/
dbt_project/logs/
dbt_project/dbt_packages/
```

- [ ] **Step 3: Write `pytest.ini`**

```ini
[pytest]
pythonpath = scripts
testpaths = tests
```

- [ ] **Step 4: Create the host venv and install test dependencies**

```bash
python -m venv .venv
.venv/Scripts/python -m pip install pytest pandas openpyxl duckdb
```
Expected: installs without errors. If a package has no wheel for Python 3.14, stop and tell the user. The fallback is to run the tests inside the container after Task 4.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: project scaffold and source data"
```

---

### Task 2: Bronze loader

**Files:**
- Create: `scripts/load_bronze.py`
- Test: `tests/test_load_bronze.py`

**Interfaces:**
- Produces: `load_bronze(xlsx_path, db_path, batch_id) -> int` (rows newly inserted). CLI: `python load_bronze.py --file F --db D --batch-id B` prints `rows_loaded=N`.
- Produces: table `bronze.raw_superstore` with the 21 source columns in snake_case (`row_id`, `order_id`, `order_date`, `ship_date`, `ship_mode`, `customer_id`, `customer_name`, `segment`, `country`, `city`, `state`, `postal_code`, `region`, `product_id`, `category`, `sub_category`, `product_name`, `sales`, `quantity`, `discount`, `profit`) plus `_batch_id`, `_loaded_at`, `_source_file`.

- [ ] **Step 1: Write the failing tests** in `tests/test_load_bronze.py`

```python
import datetime as dt

import duckdb
import pandas as pd
import pytest

import load_bronze as lb


def make_rows(ids):
    rows = []
    for i in ids:
        rows.append({
            "Row ID": i,
            "Order ID": f"CA-2016-{i:06d}",
            "Order Date": dt.datetime(2016, 11, 8),
            "Ship Date": dt.datetime(2016, 11, 11),
            "Ship Mode": "Second Class",
            "Customer ID": "CG-12520",
            "Customer Name": "Claire Gute",
            "Segment": "Consumer",
            "Country": "United States",
            "City": "Henderson",
            "State": "Kentucky",
            "Postal Code": "42420",
            "Region": "South",
            "Product ID": "FUR-BO-10001798",
            "Category": "Furniture",
            "Sub-Category": "Bookcases",
            "Product Name": "Bush Somerset Collection Bookcase",
            "Sales": 261.96,
            "Quantity": 2,
            "Discount": 0.0,
            "Profit": 41.91,
        })
    return pd.DataFrame(rows)


def write_xlsx(tmp_path, df, sheet="Sample - Superstore", name="Superstore.xlsx"):
    path = tmp_path / name
    df.to_excel(path, sheet_name=sheet, index=False)
    return path


@pytest.fixture
def db(tmp_path):
    return tmp_path / "wh.duckdb"


def query(db, sql):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_first_load_creates_table_with_metadata(tmp_path, db):
    src = write_xlsx(tmp_path, make_rows([1, 2, 3]))
    assert lb.load_bronze(src, db, "batch-1") == 3
    rows = query(db, "select row_id, _batch_id, _source_file from bronze.raw_superstore order by row_id")
    assert rows == [(1, "batch-1", "Superstore.xlsx"), (2, "batch-1", "Superstore.xlsx"), (3, "batch-1", "Superstore.xlsx")]
    cols = [r[0] for r in query(db, "select column_name from information_schema.columns where table_name = 'raw_superstore'")]
    assert "sub_category" in cols and "_loaded_at" in cols


def test_rerun_with_same_file_adds_nothing(tmp_path, db):
    src = write_xlsx(tmp_path, make_rows([1, 2]))
    lb.load_bronze(src, db, "b1")
    assert lb.load_bronze(src, db, "b2") == 0
    assert query(db, "select count(*) from bronze.raw_superstore") == [(2,)]


def test_extra_rows_are_appended(tmp_path, db):
    lb.load_bronze(write_xlsx(tmp_path, make_rows([1, 2])), db, "b1")
    bigger = write_xlsx(tmp_path, make_rows([1, 2, 3, 4]), name="Superstore2.xlsx")
    assert lb.load_bronze(bigger, db, "b2") == 2
    assert query(db, "select count(*) from bronze.raw_superstore where _batch_id = 'b2'") == [(2,)]


def test_leading_zero_postal_code_is_kept(tmp_path, db):
    df = make_rows([1])
    df["Postal Code"] = "01001"
    lb.load_bronze(write_xlsx(tmp_path, df), db, "b1")
    assert query(db, "select postal_code from bronze.raw_superstore") == [("01001",)]


def test_excel_serial_dates_become_dates(tmp_path, db):
    df = make_rows([1])
    df["Order Date"] = 42682
    df["Ship Date"] = 42685
    lb.load_bronze(write_xlsx(tmp_path, df), db, "b1")
    assert query(db, "select cast(order_date as date), cast(ship_date as date) from bronze.raw_superstore") == [
        (dt.date(2016, 11, 8), dt.date(2016, 11, 11))
    ]


def test_missing_file_raises(tmp_path, db):
    with pytest.raises(FileNotFoundError):
        lb.load_bronze(tmp_path / "nope.xlsx", db, "b1")


def test_wrong_sheet_name_raises(tmp_path, db):
    src = write_xlsx(tmp_path, make_rows([1]), sheet="Other")
    with pytest.raises(ValueError, match="sheet"):
        lb.load_bronze(src, db, "b1")


def test_missing_column_raises(tmp_path, db):
    src = write_xlsx(tmp_path, make_rows([1]).drop(columns=["Discount"]))
    with pytest.raises(ValueError, match="missing columns: Discount"):
        lb.load_bronze(src, db, "b1")


def test_duplicate_row_ids_raise(tmp_path, db):
    src = write_xlsx(tmp_path, make_rows([1, 1]))
    with pytest.raises(ValueError, match="duplicate row_id"):
        lb.load_bronze(src, db, "b1")


def test_header_only_file_raises(tmp_path, db):
    src = write_xlsx(tmp_path, make_rows([1]).iloc[0:0])
    with pytest.raises(ValueError, match="no rows"):
        lb.load_bronze(src, db, "b1")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_load_bronze.py -v`
Expected: collection error, `ModuleNotFoundError: No module named 'load_bronze'`.

- [ ] **Step 3: Write `scripts/load_bronze.py`**

```python
import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

SHEET_NAME = "Sample - Superstore"
TABLE = "bronze.raw_superstore"

EXPECTED_COLUMNS = [
    "Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
    "Customer ID", "Customer Name", "Segment", "Country", "City", "State",
    "Postal Code", "Region", "Product ID", "Category", "Sub-Category",
    "Product Name", "Sales", "Quantity", "Discount", "Profit",
]
DATE_COLUMNS = ["order_date", "ship_date"]


def snake(name):
    return name.lower().replace(" ", "_").replace("-", "_")


def read_source(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"source file not found: {path}")

    book = pd.ExcelFile(path)
    if SHEET_NAME not in book.sheet_names:
        raise ValueError(f"sheet '{SHEET_NAME}' not found in {path.name}")
    df = book.parse(SHEET_NAME, dtype={"Postal Code": str})

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {', '.join(missing)}")

    df = df[EXPECTED_COLUMNS]
    df.columns = [snake(c) for c in df.columns]
    return df


def fix_dates(df):
    # some exports keep dates as plain Excel serial numbers
    for col in DATE_COLUMNS:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], unit="D", origin="1899-12-30")
    return df


def check_rows(df):
    if df.empty:
        raise ValueError("source file has no rows")
    if df["row_id"].isna().any():
        raise ValueError("source file has rows without a row_id")
    dupes = df.loc[df["row_id"].duplicated(), "row_id"].unique().tolist()
    if dupes:
        raise ValueError(f"duplicate row_id in source file: {dupes[:5]}")


def table_exists(con):
    schema, table = TABLE.split(".")
    found = con.execute(
        "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
        [schema, table],
    ).fetchone()[0]
    return found > 0


def count_rows(con):
    return con.execute(f"select count(*) from {TABLE}").fetchone()[0]


def load_bronze(xlsx_path, db_path, batch_id):
    df = fix_dates(read_source(xlsx_path))
    check_rows(df)

    df["_batch_id"] = batch_id
    df["_loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df["_source_file"] = Path(xlsx_path).name

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("create schema if not exists bronze")
        con.register("incoming", df)

        if not table_exists(con):
            con.execute(f"create table {TABLE} as select * from incoming")
            return len(df)

        before = count_rows(con)
        con.execute(
            f"""
            insert into {TABLE} by name
            select * from incoming i
            where not exists (select 1 from {TABLE} r where r.row_id = i.row_id)
            """
        )
        return count_rows(con) - before
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description="Load the Superstore Excel file into the bronze layer")
    parser.add_argument("--file", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    loaded = load_bronze(args.file, args.db, args.batch_id)
    print(f"rows_loaded={loaded}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_load_bronze.py -v`
Expected: 10 passed.

- [ ] **Step 5: Smoke test against the real file**

Run: `.venv/Scripts/python scripts/load_bronze.py --file data/raw/Superstore.xlsx --db warehouse/smoke.duckdb --batch-id smoke`
Expected: `rows_loaded=9994`. Then delete the scratch db: `rm warehouse/smoke.duckdb`.

- [ ] **Step 6: Commit**

```bash
git add scripts/load_bronze.py tests/test_load_bronze.py
git commit -m "feat: incremental bronze loader"
```

---

### Task 3: Ops script (reject-rate check and run log)

**Files:**
- Create: `scripts/pipeline_ops.py`, `scripts/query.py`
- Test: `tests/test_pipeline_ops.py`

**Interfaces:**
- Produces: `reject_rate_exceeded(rejected, total, threshold) -> bool`; `reject_stats(db_path) -> (total, rejected)`; `record_run(db_path, batch_id, status, started_at, failed_task=None)`; `main(argv=None) -> int`.
- CLI: `pipeline_ops.py check-rejects --db D --threshold 0.02` (exit 1 when over the threshold); `pipeline_ops.py log --db D --batch-id B --status success|failed --started-at ISO [--failed-task T]`.
- Produces: table `ops.pipeline_run_log(batch_id, status, rows_loaded, rows_rejected, duration_seconds, failed_task, finished_at)`.
- Consumes: `bronze.raw_superstore(_batch_id)` and `silver.silver_rejects` when they exist; both are optional.

- [ ] **Step 1: Write the failing tests** in `tests/test_pipeline_ops.py`

```python
import duckdb
import pytest

import pipeline_ops as ops


@pytest.fixture
def db(tmp_path):
    return tmp_path / "wh.duckdb"


def seed(db, bronze_rows=100, rejects=0):
    con = duckdb.connect(str(db))
    con.execute("create schema bronze")
    con.execute(
        "create table bronze.raw_superstore as "
        "select i as row_id, case when i <= 40 then 'old' else 'new' end as _batch_id "
        f"from range(1, {bronze_rows} + 1) t(i)"
    )
    con.execute("create schema silver")
    con.execute(
        "create table silver.silver_rejects as "
        f"select i as row_id, 'bad' as reject_reason from range(1, {rejects} + 1) t(i)"
    )
    con.close()


def log_rows(db):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(
            "select batch_id, status, rows_loaded, rows_rejected, failed_task from ops.pipeline_run_log"
        ).fetchall()
    finally:
        con.close()


@pytest.mark.parametrize(
    "rejected,total,threshold,expected",
    [(0, 100, 0.02, False), (2, 100, 0.02, False), (3, 100, 0.02, True), (5, 0, 0.02, False)],
)
def test_reject_rate_exceeded(rejected, total, threshold, expected):
    assert ops.reject_rate_exceeded(rejected, total, threshold) is expected


def test_check_rejects_passes_when_clean(db):
    seed(db, rejects=0)
    assert ops.main(["check-rejects", "--db", str(db), "--threshold", "0.02"]) == 0


def test_check_rejects_fails_when_over_threshold(db):
    seed(db, rejects=10)
    assert ops.main(["check-rejects", "--db", str(db), "--threshold", "0.02"]) == 1


def test_log_success_counts_current_batch(db):
    seed(db, rejects=1)
    args = ["log", "--db", str(db), "--batch-id", "new", "--status", "success",
            "--started-at", "2026-10-03T00:00:00+00:00"]
    assert ops.main(args) == 0
    assert log_rows(db) == [("new", "success", 60, 1, None)]


def test_log_failure_before_any_table_exists(db):
    args = ["log", "--db", str(db), "--batch-id", "run-1", "--status", "failed",
            "--started-at", "2026-10-03T00:00:00+00:00", "--failed-task", "check_source_file"]
    assert ops.main(args) == 0
    assert log_rows(db) == [("run-1", "failed", 0, 0, "check_source_file")]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_pipeline_ops.py -v`
Expected: `ModuleNotFoundError: No module named 'pipeline_ops'`.

- [ ] **Step 3: Write `scripts/pipeline_ops.py`**

```python
import argparse
from datetime import datetime, timezone

import duckdb


def count_rows(con, schema, table, where="", params=None):
    exists = con.execute(
        "select count(*) from information_schema.tables where table_schema = ? and table_name = ?",
        [schema, table],
    ).fetchone()[0]
    if not exists:
        return 0
    sql = f"select count(*) from {schema}.{table}"
    if where:
        sql += f" where {where}"
    return con.execute(sql, params or []).fetchone()[0]


def reject_rate_exceeded(rejected, total, threshold):
    if total == 0:
        return False
    return rejected / total > threshold


def reject_stats(db_path):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        total = count_rows(con, "bronze", "raw_superstore")
        rejected = count_rows(con, "silver", "silver_rejects")
        return total, rejected
    finally:
        con.close()


def parse_time(text):
    when = datetime.fromisoformat(text)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when.astimezone(timezone.utc).replace(tzinfo=None)


def record_run(db_path, batch_id, status, started_at, failed_task=None):
    con = duckdb.connect(str(db_path))
    try:
        con.execute("create schema if not exists ops")
        con.execute(
            """
            create table if not exists ops.pipeline_run_log (
                batch_id varchar,
                status varchar,
                rows_loaded integer,
                rows_rejected integer,
                duration_seconds double,
                failed_task varchar,
                finished_at timestamp
            )
            """
        )
        loaded = count_rows(con, "bronze", "raw_superstore", "_batch_id = ?", [batch_id])
        rejected = count_rows(con, "silver", "silver_rejects")
        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        seconds = (finished - parse_time(started_at)).total_seconds()
        con.execute(
            "insert into ops.pipeline_run_log values (?, ?, ?, ?, ?, ?, ?)",
            [batch_id, status, loaded, rejected, seconds, failed_task, finished],
        )
    finally:
        con.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline checks and run logging")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-rejects", help="fail when too many rows were rejected")
    check.add_argument("--db", required=True)
    check.add_argument("--threshold", type=float, default=0.02)

    log = sub.add_parser("log", help="write one row to ops.pipeline_run_log")
    log.add_argument("--db", required=True)
    log.add_argument("--batch-id", required=True)
    log.add_argument("--status", required=True, choices=["success", "failed"])
    log.add_argument("--started-at", required=True)
    log.add_argument("--failed-task")

    args = parser.parse_args(argv)

    if args.command == "check-rejects":
        total, rejected = reject_stats(args.db)
        rate = rejected / total if total else 0
        print(f"rejected {rejected} of {total} rows ({rate:.2%}), limit {args.threshold:.2%}")
        return 1 if reject_rate_exceeded(rejected, total, args.threshold) else 0

    record_run(args.db, args.batch_id, args.status, args.started_at, args.failed_task)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Write `scripts/query.py`** (handy for checking tables by hand)

```python
import os
import sys

import duckdb

con = duckdb.connect(os.environ["SUPERSTORE_DB"], read_only=True)
con.sql(sys.argv[1]).show()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest -v`
Expected: all tests pass (loader + ops).

- [ ] **Step 6: Commit**

```bash
git add scripts/pipeline_ops.py scripts/query.py tests/test_pipeline_ops.py
git commit -m "feat: reject-rate check and run log"
```

---

### Task 4: Docker image, Compose stack and container helper

**Files:**
- Create: `airflow/Dockerfile`, `airflow/requirements.txt`, `airflow/docker-compose.yaml`, `scripts/in_container.sh`

**Interfaces:**
- Produces: image `superstore-airflow:latest`. In the container, env vars `PIPELINE_BIN=/opt/airflow/pipeline_venv/bin`, `PROJECT_DIR=/opt/airflow/project`, `SUPERSTORE_DB`, `SUPERSTORE_FILE`, `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`.
- Produces: `bash scripts/in_container.sh '<command>'` runs `<command>` in a throwaway container with the project mounted. Later tasks use it as the way to run dbt and the scripts.

- [ ] **Step 1: Install Docker Desktop**

Check: `docker --version`. If it is missing, ask the user to install Docker Desktop (WSL2 backend), start it, and confirm. Do not continue until `docker compose version` prints a version.

- [ ] **Step 2: Write `airflow/requirements.txt`**

```
dbt-core>=1.10,<2.0
dbt-duckdb>=1.10,<2.0
duckdb
pandas
openpyxl
```

- [ ] **Step 3: Write `airflow/Dockerfile`**

```dockerfile
FROM apache/airflow:3.1.0

# dbt and the loader live in their own venv so their dependencies
# cannot clash with Airflow's pinned ones
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/airflow/pipeline_venv \
    && /opt/airflow/pipeline_venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

ENV PIPELINE_BIN=/opt/airflow/pipeline_venv/bin
```

- [ ] **Step 4: Write `airflow/docker-compose.yaml`**

```yaml
x-airflow-common: &airflow-common
  build: .
  image: superstore-airflow:latest
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "true"
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    # local-only secrets, change them if this ever leaves your machine
    AIRFLOW__API_AUTH__JWT_SECRET: superstore-local-jwt-secret
    AIRFLOW__API__SECRET_KEY: superstore-local-api-secret
    PROJECT_DIR: /opt/airflow/project
    SUPERSTORE_DB: /opt/airflow/project/warehouse/superstore.duckdb
    SUPERSTORE_FILE: /opt/airflow/project/data/raw/Superstore.xlsx
    DBT_PROJECT_DIR: /opt/airflow/project/dbt_project
    DBT_PROFILES_DIR: /opt/airflow/project/dbt_project
  volumes:
    - ./dags:/opt/airflow/dags
    - ../scripts:/opt/airflow/project/scripts
    - ../dbt_project:/opt/airflow/project/dbt_project
    - ../data:/opt/airflow/project/data
    - ../warehouse:/opt/airflow/project/warehouse

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 10

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: -c "airflow db migrate"
    depends_on:
      postgres:
        condition: service_healthy

  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-data:
```

- [ ] **Step 5: Write `scripts/in_container.sh`**

```bash
#!/usr/bin/env bash
# Run a command inside the airflow image with the project mounted.
# Quote the command in single quotes so $PIPELINE_BIN expands in the container, not here.
export MSYS_NO_PATHCONV=1
docker compose -f "$(dirname "$0")/../airflow/docker-compose.yaml" run --rm --no-deps --entrypoint bash airflow-scheduler -c "$*"
```

- [ ] **Step 6: Build the image and check the venv**

```bash
docker compose -f airflow/docker-compose.yaml build
bash scripts/in_container.sh '$PIPELINE_BIN/dbt --version && $PIPELINE_BIN/python -c "import duckdb, pandas, openpyxl; print(duckdb.__version__)"'
```
Expected: dbt version block listing `duckdb` under plugins, then a DuckDB version number. If dbt-core and dbt-duckdb versions conflict, loosen the pins in `requirements.txt` and rebuild.

- [ ] **Step 7: Start the stack and check it**

```bash
docker compose -f airflow/docker-compose.yaml up -d
docker compose -f airflow/docker-compose.yaml ps
```
Expected: postgres healthy, `airflow-init` exited 0, apiserver / scheduler / dag-processor running. Open http://localhost:8080 and confirm the UI loads without a login prompt. Leave the stack running.

- [ ] **Step 8: Commit**

```bash
git add airflow scripts/in_container.sh
git commit -m "feat: airflow image and compose stack"
```

---

### Task 5: dbt project skeleton and bronze source tests

**Files:**
- Create: `dbt_project/dbt_project.yml`, `dbt_project/profiles.yml`, `dbt_project/macros/generate_schema_name.sql`, `dbt_project/models/bronze/sources.yml`

**Interfaces:**
- Consumes: `bronze.raw_superstore` from Task 2.
- Produces: source `bronze.raw_superstore` (`source('bronze', 'raw_superstore')`); schema names are used exactly as configured (`silver`, `gold`, `marts`), without the `main_` prefix dbt normally adds.

- [ ] **Step 1: Write `dbt_project/dbt_project.yml`**

```yaml
name: superstore
version: "1.0.0"
config-version: 2
profile: superstore

model-paths: ["models"]
test-paths: ["tests"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]

models:
  superstore:
    silver:
      +schema: silver
      +materialized: table
    gold:
      +schema: gold
      +materialized: table
    marts:
      +schema: marts
      +materialized: table
```

- [ ] **Step 2: Write `dbt_project/profiles.yml`**

```yaml
superstore:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('SUPERSTORE_DB') }}"
      schema: main
      threads: 1
```

- [ ] **Step 3: Write `dbt_project/macros/generate_schema_name.sql`**

```sql
-- use the custom schema as-is (silver, gold, marts) instead of main_silver etc.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 4: Write `dbt_project/models/bronze/sources.yml`**

```yaml
version: 2

sources:
  - name: bronze
    schema: bronze
    tables:
      - name: raw_superstore
        columns:
          - name: row_id
            data_tests: [not_null, unique]
          - name: order_id
            data_tests: [not_null]
          - name: customer_id
            data_tests: [not_null]
          - name: product_id
            data_tests: [not_null]
          - name: _batch_id
            data_tests: [not_null]
```

- [ ] **Step 5: Load bronze through the container, then run dbt debug and the source tests**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/load_bronze.py --file $SUPERSTORE_FILE --db $SUPERSTORE_DB --batch-id manual-1'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt debug'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt test --select source:bronze'
```
Expected: `rows_loaded=9994`; `All checks passed!`; 5 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add dbt_project
git commit -m "feat: dbt project, profile and bronze source tests"
```

---

### Task 6: Silver models with reject rules

**Files:**
- Create: `dbt_project/models/silver/silver_checked.sql`, `silver_superstore.sql`, `silver_rejects.sql`, `silver_customers.sql`, `silver_products.sql`, `schema.yml`, `unit_tests.yml`

**Interfaces:**
- Consumes: `source('bronze', 'raw_superstore')`.
- Produces: `silver_superstore` (valid rows, typed: `row_id integer, order_id, order_date date, ship_date date, ship_mode, customer_id, customer_name, segment, country, city, state, postal_code varchar, region, product_id, category, sub_category, product_name, sales double, quantity integer, discount double, profit double, _batch_id, _loaded_at`); `silver_rejects` (same columns plus `reject_reason varchar`); `silver_customers(customer_id, customer_name, segment)`; `silver_products(product_id, product_name, category, sub_category)`.

- [ ] **Step 1: Write the failing unit test** `dbt_project/models/silver/unit_tests.yml`

```yaml
unit_tests:
  - name: silver_checked_flags_bad_rows
    model: silver_checked
    given:
      - input: source('bronze', 'raw_superstore')
        rows:
          - {row_id: 1, order_id: A1, order_date: '2016-11-08', ship_date: '2016-11-11', customer_id: C1, product_id: P1, sales: 10, discount: 0.1}
          - {row_id: 2, order_id: A2, order_date: '2016-11-08', ship_date: '2016-11-11', customer_id: C1, product_id: P1, sales: 0, discount: 0.1}
          - {row_id: 3, order_id: A3, order_date: '2016-11-08', ship_date: '2016-11-01', customer_id: C1, product_id: P1, sales: 10, discount: 0.1}
          - {row_id: 4, order_id: A4, order_date: '2016-11-08', ship_date: '2016-11-11', customer_id: C1, product_id: P1, sales: 10, discount: 1.5}
          - {row_id: 5, order_id: A5, order_date: '2016-11-08', ship_date: '2016-11-11', customer_id: null, product_id: P1, sales: 10, discount: 0.1}
    expect:
      rows:
        - {row_id: 1, reject_reason: null}
        - {row_id: 2, reject_reason: non-positive sales}
        - {row_id: 3, reject_reason: ship date before order date}
        - {row_id: 4, reject_reason: discount out of range}
        - {row_id: 5, reject_reason: missing key}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bash scripts/in_container.sh '$PIPELINE_BIN/dbt test --select test_type:unit'`
Expected: error, model `silver_checked` not found.

- [ ] **Step 3: Write `silver_checked.sql`**

```sql
{{ config(materialized='view') }}

with typed as (

    select
        cast(row_id as integer)            as row_id,
        trim(order_id)                     as order_id,
        cast(order_date as date)           as order_date,
        cast(ship_date as date)            as ship_date,
        trim(ship_mode)                    as ship_mode,
        trim(customer_id)                  as customer_id,
        trim(customer_name)                as customer_name,
        trim(segment)                      as segment,
        trim(country)                      as country,
        trim(city)                         as city,
        trim(state)                        as state,
        trim(postal_code)                  as postal_code,
        trim(region)                       as region,
        trim(product_id)                   as product_id,
        trim(category)                     as category,
        trim(sub_category)                 as sub_category,
        trim(product_name)                 as product_name,
        cast(sales as double)              as sales,
        cast(quantity as integer)          as quantity,
        cast(discount as double)           as discount,
        cast(profit as double)             as profit,
        _batch_id,
        _loaded_at
    from {{ source('bronze', 'raw_superstore') }}

)

select
    *,
    case
        when row_id is null or order_id is null
            or customer_id is null or product_id is null          then 'missing key'
        when order_date is null or ship_date is null              then 'missing date'
        when sales is null or sales <= 0                          then 'non-positive sales'
        when ship_date < order_date                               then 'ship date before order date'
        when discount < 0 or discount > 1                         then 'discount out of range'
    end as reject_reason
from typed
```

- [ ] **Step 4: Write `silver_superstore.sql`**

```sql
select * exclude (reject_reason)
from {{ ref('silver_checked') }}
where reject_reason is null
```

- [ ] **Step 5: Write `silver_rejects.sql`**

```sql
select *
from {{ ref('silver_checked') }}
where reject_reason is not null
```

- [ ] **Step 6: Write `silver_customers.sql`**

```sql
-- one row per customer, taking the most recent name and segment
select
    customer_id,
    arg_max(customer_name, order_date) as customer_name,
    arg_max(segment, order_date)       as segment
from {{ ref('silver_superstore') }}
group by customer_id
```

- [ ] **Step 7: Write `silver_products.sql`**

```sql
-- a few product ids carry more than one name in the source, so keep the latest
select
    product_id,
    arg_max(product_name, order_date) as product_name,
    arg_max(category, order_date)     as category,
    arg_max(sub_category, order_date) as sub_category
from {{ ref('silver_superstore') }}
group by product_id
```

- [ ] **Step 8: Write `schema.yml`**

```yaml
version: 2

models:
  - name: silver_superstore
    columns:
      - name: row_id
        data_tests: [unique, not_null]
      - name: order_id
        data_tests: [not_null]
      - name: customer_id
        data_tests: [not_null]
      - name: product_id
        data_tests: [not_null]
      - name: sales
        data_tests: [not_null]
      - name: ship_mode
        data_tests:
          - accepted_values:
              values: ['Same Day', 'First Class', 'Second Class', 'Standard Class']

  - name: silver_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]

  - name: silver_products
    columns:
      - name: product_id
        data_tests: [unique, not_null]
```

- [ ] **Step 9: Add the bronze-vs-silver reconciliation test** `dbt_project/tests/reconcile_bronze_silver.sql`

```sql
{{ config(tags=['silver_reconciliation']) }}

-- every bronze row must end up either in silver or in the rejects table
select b.n as bronze_rows, s.n as silver_rows, r.n as reject_rows
from (select count(*) as n from {{ source('bronze', 'raw_superstore') }}) b
cross join (select count(*) as n from {{ ref('silver_superstore') }}) s
cross join (select count(*) as n from {{ ref('silver_rejects') }}) r
where b.n <> s.n + r.n
```

- [ ] **Step 10: Run the unit test and build silver**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/dbt test --select test_type:unit'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt run --select silver'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt test --select silver tag:silver_reconciliation --indirect-selection cautious'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select (select count(*) from silver.silver_superstore) as valid, (select count(*) from silver.silver_rejects) as rejected"'
```
Expected: unit test PASS; 4 models built (`silver_checked` is a view); all tests PASS; counts `valid=9994`, `rejected=0`. If rejected is above 0, inspect `silver.silver_rejects` before continuing.

- [ ] **Step 11: Check the reject gate script against the real data**

Run: `bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/pipeline_ops.py check-rejects --db $SUPERSTORE_DB --threshold 0.02'`
Expected: `rejected 0 of 9994 rows (0.00%), limit 2.00%`, exit 0.

- [ ] **Step 12: Commit**

```bash
git add dbt_project
git commit -m "feat: silver models, reject rules and tests"
```

---

### Task 7: Snapshots

**Files:**
- Create: `dbt_project/snapshots/snapshots.yml`

**Interfaces:**
- Consumes: `silver_customers`, `silver_products`.
- Produces: `silver.snap_customer`, `silver.snap_product` (dbt snapshot columns `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`).

- [ ] **Step 1: Write `dbt_project/snapshots/snapshots.yml`**

```yaml
snapshots:
  - name: snap_customer
    relation: ref('silver_customers')
    config:
      schema: silver
      unique_key: customer_id
      strategy: check
      check_cols: [customer_name, segment]

  - name: snap_product
    relation: ref('silver_products')
    config:
      schema: silver
      unique_key: product_id
      strategy: check
      check_cols: [product_name, category, sub_category]
```

- [ ] **Step 2: Run the snapshots twice**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/dbt snapshot'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt snapshot'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select (select count(*) from silver.snap_customer) as customers, (select count(*) from silver.snap_customer where dbt_valid_to is null) as current_customers, (select count(*) from silver.snap_product) as products"'
```
Expected: the data is static, so the second run adds no new versions: `customers` equals `current_customers` (793), `products` is 1,862.

- [ ] **Step 3: Commit**

```bash
git add dbt_project/snapshots
git commit -m "feat: SCD2 snapshots for customers and products"
```

---

### Task 8: Gold dimensions and fact table

**Files:**
- Create: `dbt_project/macros/keys.sql`, `dbt_project/models/gold/dim_customer.sql`, `dim_product.sql`, `dim_location.sql`, `dim_date.sql`, `fact_sales.sql`, `schema.yml`, `dbt_project/tests/reconcile_fact_row_count.sql`, `dbt_project/tests/reconcile_fact_sales_total.sql`

**Interfaces:**
- Consumes: `silver_superstore`, `silver_customers`, `silver_products`.
- Produces: `dim_customer(customer_key, customer_id, customer_name, segment)`; `dim_product(product_key, product_id, product_name, category, sub_category)`; `dim_location(location_key, country, state, city, postal_code, region)`; `dim_date(date_key, date_day, calendar_year, calendar_quarter, calendar_month, month_name, day_of_month, day_of_week, day_name, is_weekend)`; `fact_sales(row_id, order_id, customer_key, product_key, location_key, order_date_key, ship_date_key, ship_mode, sales, quantity, discount, profit, _batch_id, _loaded_at)`. Keys are `md5` strings, except date keys, which are integers like `20161108`.
- Produces: macro `location_key(alias)` returning the SQL expression for a location key from a relation alias that has `country, state, city, postal_code`.

- [ ] **Step 1: Write `macros/keys.sql`**

```sql
-- same expression on both sides, so the fact table can find the dimension row
{% macro location_key(alias) -%}
    md5(concat_ws('|', {{ alias }}.country, {{ alias }}.state, {{ alias }}.city, {{ alias }}.postal_code))
{%- endmacro %}
```

- [ ] **Step 2: Write the dimensions**

`dim_customer.sql`:
```sql
select
    md5(customer_id) as customer_key,
    customer_id,
    customer_name,
    segment
from {{ ref('silver_customers') }}
```

`dim_product.sql`:
```sql
select
    md5(product_id) as product_key,
    product_id,
    product_name,
    category,
    sub_category
from {{ ref('silver_products') }}
```

`dim_location.sql`:
```sql
with places as (

    select
        country,
        state,
        city,
        postal_code,
        arg_max(region, order_date) as region
    from {{ ref('silver_superstore') }}
    group by country, state, city, postal_code

)

select
    {{ location_key('places') }} as location_key,
    country,
    state,
    city,
    postal_code,
    region
from places
```

`dim_date.sql`:
```sql
with bounds as (

    select min(order_date) as first_day, max(ship_date) as last_day
    from {{ ref('silver_superstore') }}

),

days as (

    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as date_day
    from bounds

)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day)                                as calendar_year,
    quarter(date_day)                             as calendar_quarter,
    month(date_day)                               as calendar_month,
    strftime(date_day, '%B')                      as month_name,
    day(date_day)                                 as day_of_month,
    dayofweek(date_day)                           as day_of_week,
    strftime(date_day, '%A')                      as day_name,
    dayofweek(date_day) in (0, 6)                 as is_weekend
from days
```

- [ ] **Step 3: Write `fact_sales.sql`**

```sql
{{ config(materialized='incremental', unique_key='row_id') }}

select
    s.row_id,
    s.order_id,
    c.customer_key,
    p.product_key,
    l.location_key,
    cast(strftime(s.order_date, '%Y%m%d') as integer) as order_date_key,
    cast(strftime(s.ship_date, '%Y%m%d') as integer)  as ship_date_key,
    s.ship_mode,
    s.sales,
    s.quantity,
    s.discount,
    s.profit,
    s._batch_id,
    s._loaded_at
from {{ ref('silver_superstore') }} s
join {{ ref('dim_customer') }} c on c.customer_id = s.customer_id
join {{ ref('dim_product') }} p on p.product_id = s.product_id
join {{ ref('dim_location') }} l on l.location_key = {{ location_key('s') }}

{% if is_incremental() %}
where s._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
```

- [ ] **Step 4: Write `models/gold/schema.yml`**

```yaml
version: 2

models:
  - name: dim_customer
    columns:
      - name: customer_key
        data_tests: [unique, not_null]
  - name: dim_product
    columns:
      - name: product_key
        data_tests: [unique, not_null]
  - name: dim_location
    columns:
      - name: location_key
        data_tests: [unique, not_null]
  - name: dim_date
    columns:
      - name: date_key
        data_tests: [unique, not_null]

  - name: fact_sales
    columns:
      - name: row_id
        data_tests: [unique, not_null]
      - name: customer_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: product_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_product')
              field: product_key
      - name: location_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_location')
              field: location_key
      - name: order_date_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      - name: ship_date_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
```

- [ ] **Step 5: Write the reconciliation tests**

`tests/reconcile_fact_row_count.sql`:
```sql
{{ config(tags=['reconciliation']) }}

select f.n as fact_rows, s.n as silver_rows
from (select count(*) as n from {{ ref('fact_sales') }}) f
cross join (select count(*) as n from {{ ref('silver_superstore') }}) s
where f.n <> s.n
```

`tests/reconcile_fact_sales_total.sql`:
```sql
{{ config(tags=['reconciliation']) }}

select f.total as fact_total, s.total as silver_total
from (select sum(sales) as total from {{ ref('fact_sales') }}) f
cross join (select sum(sales) as total from {{ ref('silver_superstore') }}) s
where abs(f.total - s.total) > 0.01
```

- [ ] **Step 6: Build gold and run its tests**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/dbt run --select gold'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt test --select gold tag:reconciliation --indirect-selection cautious'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select count(*) as fact_rows, round(sum(sales), 2) as total_sales from gold.fact_sales"'
```
Expected: 5 models built, all tests PASS, `fact_rows=9994`.

- [ ] **Step 7: Prove the incremental fact table does not duplicate on a second run**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/dbt run --select fact_sales'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select count(*) as fact_rows from gold.fact_sales"'
```
Expected: `fact_rows=9994` still.

- [ ] **Step 8: Commit**

```bash
git add dbt_project
git commit -m "feat: gold dimensions and incremental fact table"
```

---

### Task 9: Analytics marts

**Files:**
- Create: `dbt_project/models/marts/mart_monthly_sales.sql`, `mart_category_region_profit.sql`, `mart_customer_rfm.sql`, `mart_shipping_performance.sql`, `schema.yml`

**Interfaces:**
- Consumes: `fact_sales`, `dim_customer`, `dim_product`, `dim_location`, `dim_date` from Task 8.
- Produces: `mart_monthly_sales(month_start, total_sales, total_profit, order_count, units_sold)`; `mart_category_region_profit(category, sub_category, region, total_sales, total_profit, profit_margin)`; `mart_customer_rfm(customer_key, customer_name, last_order_date, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_segment)`; `mart_shipping_performance(ship_mode, region, order_lines, avg_days_to_ship, max_days_to_ship)`.

- [ ] **Step 1: Write `mart_monthly_sales.sql`**

```sql
select
    date_trunc('month', d.date_day)  as month_start,
    sum(f.sales)                     as total_sales,
    sum(f.profit)                    as total_profit,
    count(distinct f.order_id)       as order_count,
    sum(f.quantity)                  as units_sold
from {{ ref('fact_sales') }} f
join {{ ref('dim_date') }} d on d.date_key = f.order_date_key
group by 1
order by 1
```

- [ ] **Step 2: Write `mart_category_region_profit.sql`**

```sql
select
    p.category,
    p.sub_category,
    l.region,
    sum(f.sales)                                  as total_sales,
    sum(f.profit)                                 as total_profit,
    sum(f.profit) / nullif(sum(f.sales), 0)       as profit_margin
from {{ ref('fact_sales') }} f
join {{ ref('dim_product') }} p on p.product_key = f.product_key
join {{ ref('dim_location') }} l on l.location_key = f.location_key
group by 1, 2, 3
```

- [ ] **Step 3: Write `mart_customer_rfm.sql`**

```sql
with per_customer as (

    select
        f.customer_key,
        max(d.date_day)             as last_order_date,
        count(distinct f.order_id)  as frequency,
        sum(f.sales)                as monetary
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_date') }} d on d.date_key = f.order_date_key
    group by 1

),

as_of as (

    -- measure recency from the newest order in the data, not from today
    select max(last_order_date) as as_of_date from per_customer

),

scored as (

    select
        c.*,
        date_diff('day', c.last_order_date, a.as_of_date)   as recency_days,
        ntile(4) over (order by date_diff('day', c.last_order_date, a.as_of_date) desc) as r_score,
        ntile(4) over (order by c.frequency)                as f_score,
        ntile(4) over (order by c.monetary)                 as m_score
    from per_customer c
    cross join as_of a

)

select
    s.customer_key,
    dc.customer_name,
    s.last_order_date,
    s.recency_days,
    s.frequency,
    round(s.monetary, 2) as monetary,
    s.r_score,
    s.f_score,
    s.m_score,
    case
        when s.r_score >= 3 and s.f_score >= 3 and s.m_score >= 3 then 'Champion'
        when s.r_score >= 3 and s.f_score >= 2                    then 'Loyal'
        when s.r_score <= 2 and s.f_score >= 3                    then 'At risk'
        when s.r_score = 1                                        then 'Lost'
        else 'Regular'
    end as rfm_segment
from scored s
join {{ ref('dim_customer') }} dc on dc.customer_key = s.customer_key
```

- [ ] **Step 4: Write `mart_shipping_performance.sql`**

```sql
select
    f.ship_mode,
    l.region,
    count(*)                                          as order_lines,
    avg(date_diff('day', od.date_day, sd.date_day))   as avg_days_to_ship,
    max(date_diff('day', od.date_day, sd.date_day))   as max_days_to_ship
from {{ ref('fact_sales') }} f
join {{ ref('dim_location') }} l on l.location_key = f.location_key
join {{ ref('dim_date') }} od on od.date_key = f.order_date_key
join {{ ref('dim_date') }} sd on sd.date_key = f.ship_date_key
group by 1, 2
```

- [ ] **Step 5: Write `models/marts/schema.yml`**

```yaml
version: 2

models:
  - name: mart_monthly_sales
    columns:
      - name: month_start
        data_tests: [unique, not_null]
  - name: mart_customer_rfm
    columns:
      - name: customer_key
        data_tests: [unique, not_null]
      - name: rfm_segment
        data_tests:
          - accepted_values:
              values: ['Champion', 'Loyal', 'At risk', 'Lost', 'Regular']
  - name: mart_category_region_profit
    columns:
      - name: category
        data_tests: [not_null]
  - name: mart_shipping_performance
    columns:
      - name: ship_mode
        data_tests: [not_null]
```

- [ ] **Step 6: Build and test the marts, then sanity-check numbers**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/dbt run --select marts'
bash scripts/in_container.sh '$PIPELINE_BIN/dbt test --select marts --indirect-selection cautious'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select rfm_segment, count(*) as customers from marts.mart_customer_rfm group by 1 order by 2 desc"'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select round(sum(total_sales), 2) as sales_from_marts, (select round(sum(sales), 2) from gold.fact_sales) as sales_from_fact from marts.mart_monthly_sales"'
```
Expected: 4 models built, tests PASS, the five segments sum to 793 customers, and the two sales totals are equal.

- [ ] **Step 7: Commit**

```bash
git add dbt_project/models/marts
git commit -m "feat: analytics marts"
```

---

### Task 10: Airflow DAG

**Files:**
- Create: `airflow/dags/superstore_pipeline.py`

**Interfaces:**
- Consumes: env vars from compose (`PROJECT_DIR`, `PIPELINE_BIN`, `SUPERSTORE_DB`, `SUPERSTORE_FILE`), scripts from Tasks 2 and 3, dbt selectors from Tasks 5-9, optional Airflow Variable `reject_rate_threshold`.
- Produces: DAG `superstore_pipeline` with tasks in order `check_source_file`, `dbt_debug`, `load_bronze`, `test_bronze`, `build_silver`, `snapshot`, `test_silver`, `build_gold`, `test_gold`, `log_run`.

- [ ] **Step 1: Write `airflow/dags/superstore_pipeline.py`**

```python
import os
import subprocess
from datetime import datetime, timedelta

from airflow.sdk import dag, task

# values rendered by Airflow before the shell command runs
TASK_ENV = {
    "BATCH_ID": "{{ run_id }}",
    "STARTED_AT": "{{ dag_run.start_date.isoformat() }}",
    "REJECT_THRESHOLD": "{{ var.value.get('reject_rate_threshold', '0.02') }}",
}

shell = task.bash(env=TASK_ENV, append_env=True)


def log_failure(context):
    # runs for the task that failed, even if bronze or silver were never built
    subprocess.run(
        [
            os.path.join(os.environ["PIPELINE_BIN"], "python"),
            os.path.join(os.environ["PROJECT_DIR"], "scripts", "pipeline_ops.py"),
            "log",
            "--db", os.environ["SUPERSTORE_DB"],
            "--batch-id", context["run_id"],
            "--status", "failed",
            "--started-at", context["dag_run"].start_date.isoformat(),
            "--failed-task", context["ti"].task_id,
        ],
        check=False,
    )


@dag(
    dag_id="superstore_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": log_failure,
    },
    tags=["superstore", "dbt"],
)
def superstore_pipeline():

    @shell
    def check_source_file():
        return 'test -f "$SUPERSTORE_FILE" || { echo "source file missing: $SUPERSTORE_FILE"; exit 1; }'

    @shell
    def dbt_debug():
        return "$PIPELINE_BIN/dbt debug"

    @shell
    def load_bronze():
        return (
            "$PIPELINE_BIN/python $PROJECT_DIR/scripts/load_bronze.py "
            '--file "$SUPERSTORE_FILE" --db "$SUPERSTORE_DB" --batch-id "$BATCH_ID"'
        )

    @shell
    def test_bronze():
        return "$PIPELINE_BIN/dbt test --select source:bronze"

    @shell
    def build_silver():
        return "$PIPELINE_BIN/dbt run --select silver"

    @shell
    def snapshot():
        return "$PIPELINE_BIN/dbt snapshot"

    @shell
    def test_silver():
        # gate: dbt tests first, then the reject-rate limit
        return (
            "$PIPELINE_BIN/dbt test --select silver tag:silver_reconciliation --indirect-selection cautious"
            " && $PIPELINE_BIN/python $PROJECT_DIR/scripts/pipeline_ops.py check-rejects"
            ' --db "$SUPERSTORE_DB" --threshold "$REJECT_THRESHOLD"'
        )

    @shell
    def build_gold():
        return "$PIPELINE_BIN/dbt run --select gold marts"

    @shell
    def test_gold():
        return (
            "$PIPELINE_BIN/dbt test --select gold marts tag:reconciliation --indirect-selection cautious"
            " && $PIPELINE_BIN/dbt docs generate"
        )

    @shell
    def log_run():
        return (
            "$PIPELINE_BIN/python $PROJECT_DIR/scripts/pipeline_ops.py log "
            '--db "$SUPERSTORE_DB" --batch-id "$BATCH_ID" --status success --started-at "$STARTED_AT"'
        )

    (
        check_source_file()
        >> dbt_debug()
        >> load_bronze()
        >> test_bronze()
        >> build_silver()
        >> snapshot()
        >> test_silver()
        >> build_gold()
        >> test_gold()
        >> log_run()
    )


superstore_pipeline()
```

- [ ] **Step 2: Check the DAG parses**

```bash
docker compose -f airflow/docker-compose.yaml restart airflow-scheduler airflow-dag-processor
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags list-import-errors
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags list
```
Expected: no import errors; `superstore_pipeline` is listed. If `task.bash` rejects `env=` or `append_env=`, switch the tasks to `BashOperator` from `airflow.providers.standard.operators.bash` with the same arguments, keeping the same task ids.

- [ ] **Step 3: Commit**

```bash
git add airflow/dags
git commit -m "feat: superstore_pipeline DAG"
```

---

### Task 11: End-to-end run, failure path and README

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Start from a clean warehouse**

```bash
docker compose -f airflow/docker-compose.yaml stop airflow-scheduler airflow-dag-processor
rm -f warehouse/superstore.duckdb warehouse/superstore.duckdb.wal
docker compose -f airflow/docker-compose.yaml start airflow-scheduler airflow-dag-processor
```

- [ ] **Step 2: Trigger a successful run**

```bash
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags unpause superstore_pipeline
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags trigger superstore_pipeline
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags list-runs superstore_pipeline
```
Repeat the last command until the state is `success` (a few minutes). If it ends in `failed`, open the failed task log in the UI at http://localhost:8080 and fix the cause before continuing.

- [ ] **Step 3: Verify results**

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select count(*) as fact_rows from gold.fact_sales"'
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select batch_id, status, rows_loaded, rows_rejected, round(duration_seconds) as seconds from ops.pipeline_run_log"'
```
Expected: `fact_rows=9994`; one `success` row with `rows_loaded=9994`, `rows_rejected=0`.

- [ ] **Step 4: Verify a second run is a no-op load**

Trigger the DAG again as in Step 2. Expected: success, and the new log row has `rows_loaded=0`; `fact_sales` still has 9,994 rows.

- [ ] **Step 5: Verify the failure path is logged**

```bash
mv data/raw/Superstore.xlsx data/raw/Superstore.xlsx.bak
docker compose -f airflow/docker-compose.yaml exec airflow-scheduler airflow dags trigger superstore_pipeline
```
Wait until the run is `failed` (retries add a few minutes). Then:
```bash
mv data/raw/Superstore.xlsx.bak data/raw/Superstore.xlsx
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select batch_id, status, failed_task from ops.pipeline_run_log order by finished_at desc limit 1"'
```
Expected: latest row has `status=failed`, `failed_task=check_source_file`. Restoring the file first matters: the project must not be left broken.

- [ ] **Step 6: Write `README.md`**

````markdown
# Superstore pipeline

A small ELT project: Excel in, tested star schema and a few reporting tables out.
Airflow runs it daily, dbt does the SQL, DuckDB holds the data.

## How the data moves

```
Superstore.xlsx -> bronze -> silver -> gold -> marts
                      |         |
                      |         +-- silver_rejects (rows that broke a rule)
                      +-- loaded incrementally, new Row IDs only
```

- **bronze.raw_superstore**: the file as it came, plus batch id, load time and file name.
- **silver**: typed rows. Bad rows (zero or negative sales, ship date before order date, missing keys, discount outside 0 to 1) go to `silver_rejects` with a reason. Customer and product history is kept in snapshots.
- **gold**: customer, product, location and date dimensions and an incremental `fact_sales` (one row per order line).
- **marts**: monthly sales, profit by category and region, customer RFM segments, shipping speed.
- **ops.pipeline_run_log**: one row per run with counts, duration and the task that failed.

## The DAG

`check_source_file -> dbt_debug -> load_bronze -> test_bronze -> build_silver -> snapshot -> test_silver -> build_gold -> test_gold -> log_run`

`test_silver` is the gate. If a silver test fails, or more than 2% of rows are rejected, gold is not built.
Change the limit with the Airflow Variable `reject_rate_threshold` (for example `0.05`).

## Run it

You need Docker Desktop.

```bash
docker compose -f airflow/docker-compose.yaml up -d --build
```

Open http://localhost:8080, unpause `superstore_pipeline` and trigger it.
The warehouse is `warehouse/superstore.duckdb`.

Look at a table without opening the UI:

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select * from marts.mart_monthly_sales limit 5"'
```

Run dbt by hand:

```bash
bash scripts/in_container.sh '$PIPELINE_BIN/dbt build'
```

## Tests

- Loader and ops scripts: `python -m pytest` (needs pandas, openpyxl, duckdb, pytest).
- dbt: source tests, silver and gold tests, a unit test for the reject rules, and reconciliation checks (bronze = silver + rejects, fact rows and sales = silver).

## Notes

- DuckDB has one writer, so the tasks run one after another and only one DAG run is active at a time.
- The passwords and secrets in `docker-compose.yaml` are for local use only.
- The data is static, so the snapshots show one version per customer and product until the source changes.
````

- [ ] **Step 7: Final full check and commit**

```bash
.venv/Scripts/python -m pytest -v
bash scripts/in_container.sh '$PIPELINE_BIN/dbt build'
git add README.md
git commit -m "docs: README and end-to-end verification"
```
Expected: pytest all pass; `dbt build` completes with no errors.

---

## Self-review

- **Spec coverage:** loader and incremental bronze (Task 2), reject-rate check and run log (3), Docker (4), source tests (5), silver, rejects and unit test (6), snapshots (7), dimensions and incremental fact plus reconciliation (8), four marts (9), DAG with gate, callback, retries, variable threshold (10), docs generate, e2e, failure path and README (10, 11). Out-of-scope items are not planned.
- **Placeholders:** none; every code step holds full code.
- **Consistency:** `load_bronze(xlsx_path, db_path, batch_id)`, `record_run(db_path, batch_id, status, started_at, failed_task)`, source/table names, tags (`silver_reconciliation`, `reconciliation`) and env var names match across tasks.
- **Known risks to watch during execution:** the `task.bash` keyword arguments (fallback given in Task 10), dbt and dbt-duckdb version compatibility (loosen pins), and the host Python 3.14 wheels (fallback given in Task 1).
