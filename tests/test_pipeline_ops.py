import duckdb
import pytest

import pipeline_ops as ops


@pytest.fixture
def db(tmp_path):
    return tmp_path / "wh.duckdb"


def seed(db, rejects_new=0, rejects_old=0):
    # bronze: 40 rows from batch "old", 60 from batch "new"
    con = duckdb.connect(str(db))
    con.execute("create schema bronze")
    con.execute(
        "create table bronze.raw_superstore as "
        "select i as row_id, case when i <= 40 then 'old' else 'new' end as _batch_id "
        "from range(1, 101) t(i)"
    )
    con.execute("create schema silver")
    con.execute(
        "create table silver.silver_rejects as "
        f"select i as row_id, 'old' as _batch_id, 'bad' as reject_reason from range(1, {rejects_old} + 1) t(i) "
        "union all "
        f"select 1000 + i, 'new', 'bad' from range(1, {rejects_new} + 1) t(i)"
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


def check(db, batch="new", threshold="0.02"):
    return ops.main(["check-rejects", "--db", str(db), "--batch-id", batch, "--threshold", threshold])


@pytest.mark.parametrize(
    "rejected,total,threshold,expected",
    [(0, 100, 0.02, False), (2, 100, 0.02, False), (3, 100, 0.02, True), (5, 0, 0.02, False)],
)
def test_reject_rate_exceeded(rejected, total, threshold, expected):
    assert ops.reject_rate_exceeded(rejected, total, threshold) is expected


def test_check_rejects_passes_when_batch_is_clean(db):
    seed(db)
    assert check(db) == 0


def test_check_rejects_fails_when_batch_is_over_threshold(db):
    seed(db, rejects_new=3)
    assert check(db) == 1


def test_old_rejects_do_not_count_against_a_new_batch(db):
    seed(db, rejects_old=30)
    assert check(db) == 0


def test_batch_that_loaded_nothing_passes(db):
    seed(db, rejects_old=30)
    assert check(db, batch="run-with-no-new-rows") == 0


def test_log_success_counts_only_the_current_batch(db):
    seed(db, rejects_new=1, rejects_old=5)
    args = ["log", "--db", str(db), "--batch-id", "new", "--status", "success",
            "--started-at", "2026-10-03T00:00:00+00:00"]
    assert ops.main(args) == 0
    assert log_rows(db) == [("new", "success", 60, 1, None)]


def test_log_failure_before_any_table_exists(db):
    args = ["log", "--db", str(db), "--batch-id", "run-1", "--status", "failed",
            "--started-at", "2026-10-03T00:00:00+00:00", "--failed-task", "check_source_file"]
    assert ops.main(args) == 0
    assert log_rows(db) == [("run-1", "failed", 0, 0, "check_source_file")]
