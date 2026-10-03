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
- DAG structure (task ids, order, dbt selectors): `tests/test_dag.py` needs Airflow, so it only runs inside the image. It is skipped on the host.
- dbt: source tests, silver and gold tests, a unit test for the reject rules, and reconciliation checks (bronze = silver + rejects, fact rows and sales = silver).

## Notes

- DuckDB has one writer, so the tasks run one after another and only one DAG run is active at a time.
- The passwords and secrets in `docker-compose.yaml` are for local use only.
- The data is static, so the snapshots show one version per customer and product until the source changes.
