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


def shell():
    # a fresh decorator per task, so every task keeps its own task_id
    return task.bash(env=TASK_ENV, append_env=True)


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

    @shell()
    def check_source_file():
        return 'test -f "$SUPERSTORE_FILE" || { echo "source file missing: $SUPERSTORE_FILE"; exit 1; }'

    @shell()
    def dbt_debug():
        return "$PIPELINE_BIN/dbt debug"

    @shell()
    def load_bronze():
        return (
            "$PIPELINE_BIN/python $PROJECT_DIR/scripts/load_bronze.py "
            '--file "$SUPERSTORE_FILE" --db "$SUPERSTORE_DB" --batch-id "$BATCH_ID"'
        )

    @shell()
    def test_bronze():
        return "$PIPELINE_BIN/dbt test --select source:bronze --indirect-selection cautious"

    @shell()
    def build_silver():
        return "$PIPELINE_BIN/dbt run --select silver"

    @shell()
    def snapshot():
        return "$PIPELINE_BIN/dbt snapshot"

    @shell()
    def test_silver():
        # gate: dbt tests first, then the reject-rate limit
        return (
            "$PIPELINE_BIN/dbt test --select silver tag:silver_reconciliation --indirect-selection cautious"
            " && $PIPELINE_BIN/python $PROJECT_DIR/scripts/pipeline_ops.py check-rejects"
            ' --db "$SUPERSTORE_DB" --threshold "$REJECT_THRESHOLD"'
        )

    @shell()
    def build_gold():
        return "$PIPELINE_BIN/dbt run --select gold marts"

    @shell()
    def test_gold():
        return (
            "$PIPELINE_BIN/dbt test --select gold marts tag:reconciliation --indirect-selection cautious"
            " && $PIPELINE_BIN/dbt docs generate"
        )

    @shell()
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
