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


def to_date(col):
    if pd.api.types.is_datetime64_any_dtype(col):
        return col
    # Excel serial numbers where the cell is a number, plain text parsing otherwise;
    # anything unreadable becomes null and gets rejected in silver
    serial = pd.to_numeric(col, errors="coerce")
    from_serial = pd.to_datetime(serial, unit="D", origin="1899-12-30")
    from_text = pd.to_datetime(col.where(serial.isna()), errors="coerce")
    return from_serial.fillna(from_text)


def fix_dates(df):
    for col in DATE_COLUMNS:
        df[col] = to_date(df[col])
    return df


def fix_postal_codes(df):
    # Excel stores them as numbers, so 06824 arrives as 6824
    codes = df["postal_code"].str.strip()
    df["postal_code"] = codes.mask(codes.str.fullmatch(r"\d{1,4}").fillna(False), codes.str.zfill(5))
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
    df = fix_postal_codes(fix_dates(read_source(xlsx_path)))
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
