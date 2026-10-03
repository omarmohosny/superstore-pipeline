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


def batch_counts(con, batch_id):
    loaded = count_rows(con, "bronze", "raw_superstore", "_batch_id = ?", [batch_id])
    rejected = count_rows(con, "silver", "silver_rejects", "_batch_id = ?", [batch_id])
    return loaded, rejected


def reject_stats(db_path, batch_id):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return batch_counts(con, batch_id)
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
        loaded, rejected = batch_counts(con, batch_id)
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
    check.add_argument("--batch-id", required=True)
    check.add_argument("--threshold", type=float, default=0.02)

    log = sub.add_parser("log", help="write one row to ops.pipeline_run_log")
    log.add_argument("--db", required=True)
    log.add_argument("--batch-id", required=True)
    log.add_argument("--status", required=True, choices=["success", "failed"])
    log.add_argument("--started-at", required=True)
    log.add_argument("--failed-task")

    args = parser.parse_args(argv)

    if args.command == "check-rejects":
        total, rejected = reject_stats(args.db, args.batch_id)
        rate = rejected / total if total else 0
        print(f"batch {args.batch_id}: rejected {rejected} of {total} loaded rows ({rate:.2%}), limit {args.threshold:.2%}")
        return 1 if reject_rate_exceeded(rejected, total, args.threshold) else 0

    record_run(args.db, args.batch_id, args.status, args.started_at, args.failed_task)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
