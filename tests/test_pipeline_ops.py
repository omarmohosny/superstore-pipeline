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
