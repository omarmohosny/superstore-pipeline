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


def test_numeric_postal_code_gets_its_leading_zero_back(tmp_path, db):
    df = make_rows([1])
    df["Postal Code"] = 6824  # Excel stores it as a number, so the zero is already gone
    lb.load_bronze(write_xlsx(tmp_path, df), db, "b1")
    assert query(db, "select postal_code from bronze.raw_superstore") == [("06824",)]


def test_text_in_a_date_column_becomes_null_and_later_loads_still_work(tmp_path, db):
    df = make_rows([1, 2]).astype({"Order Date": object})
    df.loc[0, "Order Date"] = "TBD"
    df.loc[1, "Order Date"] = 42682
    lb.load_bronze(write_xlsx(tmp_path, df), db, "b1")
    rows = query(db, "select row_id, cast(order_date as date) from bronze.raw_superstore order by row_id")
    assert rows == [(1, None), (2, dt.date(2016, 11, 8))]

    bigger = write_xlsx(tmp_path, make_rows([1, 2, 3]), name="Superstore2.xlsx")
    assert lb.load_bronze(bigger, db, "b2") == 1
