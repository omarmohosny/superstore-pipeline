"""Builds docs/Superstore_Pipeline_Technical_Documentation.pdf from the project and the warehouse."""
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape

import duckdb
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Frame, Image, KeepTogether, NextPageTemplate, PageBreak,
                                PageTemplate, Paragraph, Preformatted, Spacer, Table, TableStyle)
from reportlab.platypus.tableofcontents import TableOfContents

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
FIG = DOCS / "figures"
SHOTS = DOCS / "screenshots"
OUT = DOCS / "Superstore_Pipeline_Technical_Documentation.pdf"

NAVY = colors.HexColor("#1F3A5F")
BLUE = colors.HexColor("#2E6FBA")
GRAY = colors.HexColor("#6B7280")
LIGHT = colors.HexColor("#F3F6FA")
LINE = colors.HexColor("#CBD5E1")

AUTHOR = "Omar Mohamed Hosny"
SCHOOL = "NTI - National Telecommunication Institute"
INSTRUCTOR = "Kyrellos El Naggar"
REPO = "https://github.com/omarmohosny/superstore-pipeline"
TITLE = "Superstore Data Pipeline"

base = getSampleStyleSheet()
S = {
    "body": ParagraphStyle("body", parent=base["BodyText"], fontName="Helvetica", fontSize=10, leading=14.5,
                           alignment=TA_JUSTIFY, spaceAfter=7, textColor=colors.HexColor("#1F2937")),
    "h1": ParagraphStyle("Heading1", parent=base["Heading1"], fontName="Helvetica-Bold", fontSize=18, leading=22,
                         textColor=NAVY, spaceBefore=4, spaceAfter=10),
    "h2": ParagraphStyle("Heading2", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=13, leading=17,
                         textColor=BLUE, spaceBefore=12, spaceAfter=6),
    "h3": ParagraphStyle("h3", parent=base["Heading3"], fontName="Helvetica-Bold", fontSize=10.5, leading=14,
                         textColor=NAVY, spaceBefore=8, spaceAfter=4),
    "bullet": ParagraphStyle("bullet", parent=base["BodyText"], fontName="Helvetica", fontSize=10, leading=14,
                             leftIndent=16, bulletIndent=4, spaceAfter=3, textColor=colors.HexColor("#1F2937")),
    "figcap": ParagraphStyle("FigCaption", parent=base["BodyText"], fontName="Helvetica-Oblique", fontSize=9,
                             leading=12, alignment=TA_CENTER, textColor=GRAY, spaceBefore=4, spaceAfter=12),
    "tabcap": ParagraphStyle("TableCaption", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=9,
                             leading=12, textColor=NAVY, spaceBefore=8, spaceAfter=4, keepWithNext=1),
    "cell": ParagraphStyle("cell", parent=base["BodyText"], fontName="Helvetica", fontSize=8.4, leading=10.8),
    "cellh": ParagraphStyle("cellh", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=8.4, leading=10.8,
                            textColor=colors.white),
    "code": ParagraphStyle("code", fontName="Courier", fontSize=6.9, leading=8.4, backColor=LIGHT,
                           borderColor=LINE, borderWidth=0.5, borderPadding=5, spaceBefore=4, spaceAfter=10),
    "title": ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=30, leading=36, textColor=NAVY),
    "subtitle": ParagraphStyle("subtitle", fontName="Helvetica", fontSize=14, leading=19, textColor=BLUE),
    "meta": ParagraphStyle("meta", fontName="Helvetica", fontSize=11, leading=17, textColor=colors.HexColor("#1F2937")),
    "metah": ParagraphStyle("metah", fontName="Helvetica-Bold", fontSize=9, leading=12, textColor=GRAY),
    "tochead": ParagraphStyle("tochead", fontName="Helvetica-Bold", fontSize=18, leading=22, textColor=NAVY, spaceAfter=10),
}


def toc_levels(size_first=10.5):
    return [
        ParagraphStyle("t0", fontName="Helvetica-Bold", fontSize=size_first, leading=13, leftIndent=0, spaceBefore=3),
        ParagraphStyle("t1", fontName="Helvetica", fontSize=9, leading=11, leftIndent=16),
    ]


class KindTOC(TableOfContents):
    """A table of contents that only collects the entries of one kind (headings, figures or tables)."""

    def __init__(self, kind, **kw):
        super().__init__(**kw)
        self.kind = kind

    def notify(self, kind, stuff):
        if kind == self.kind:
            self.addEntry(*stuff)


class Doc(BaseDocTemplate):
    def __init__(self, path):
        super().__init__(str(path), pagesize=A4, leftMargin=2.2 * cm, rightMargin=2.2 * cm,
                         topMargin=2.2 * cm, bottomMargin=2.2 * cm, title=f"{TITLE} - Technical Documentation",
                         author=AUTHOR, subject="ELT pipeline with Airflow, dbt and DuckDB")
        frame = Frame(self.leftMargin, self.bottomMargin, self.width, self.height, id="f")
        self.addPageTemplates([PageTemplate("Title", [frame], onPage=self.title_page),
                               PageTemplate("Body", [frame], onPage=self.body_page)])

    def title_page(self, canv, doc):
        canv.saveState()
        canv.setFillColor(NAVY)
        canv.rect(0, A4[1] - 1.1 * cm, A4[0], 1.1 * cm, stroke=0, fill=1)
        canv.setFillColor(BLUE)
        canv.rect(0, 0, A4[0], 0.6 * cm, stroke=0, fill=1)
        canv.restoreState()

    def body_page(self, canv, doc):
        canv.saveState()
        canv.setStrokeColor(LINE)
        canv.line(self.leftMargin, A4[1] - 1.6 * cm, A4[0] - self.rightMargin, A4[1] - 1.6 * cm)
        canv.setFont("Helvetica", 8)
        canv.setFillColor(GRAY)
        canv.drawString(self.leftMargin, A4[1] - 1.35 * cm, f"{TITLE} - Technical Documentation")
        canv.drawRightString(A4[0] - self.rightMargin, A4[1] - 1.35 * cm, AUTHOR)
        canv.line(self.leftMargin, 1.7 * cm, A4[0] - self.rightMargin, 1.7 * cm)
        canv.drawString(self.leftMargin, 1.25 * cm, SCHOOL)
        canv.drawRightString(A4[0] - self.rightMargin, 1.25 * cm, f"Page {doc.page}")
        canv.restoreState()

    def afterFlowable(self, flowable):
        if not isinstance(flowable, Paragraph):
            return
        name = flowable.style.name
        text = flowable.getPlainText()
        if name == "Heading1":
            self.notify("TOCEntry", (0, text, self.page))
        elif name == "Heading2":
            self.notify("TOCEntry", (1, text, self.page))
        elif name == "FigCaption":
            self.notify("LOFEntry", (0, text, self.page))
        elif name == "TableCaption":
            self.notify("LOTEntry", (0, text, self.page))


counters = {"fig": 0, "tab": 0}


def P(text, style="body"):
    return Paragraph(text, S[style])


def H1(text):
    return Paragraph(escape(text), S["h1"])


def H2(text):
    return Paragraph(escape(text), S["h2"])


def H3(text):
    return Paragraph(escape(text), S["h3"])


def bullets(items):
    return [Paragraph(i, S["bullet"], bulletText="•") for i in items]


def code(text):
    return Preformatted(text.strip("\n"), S["code"])


def figure(name, caption, width=15.4 * cm):
    counters["fig"] += 1
    path = FIG / name
    img = Image(str(path))
    ratio = img.imageHeight / img.imageWidth
    img.drawWidth, img.drawHeight = width, width * ratio
    img.hAlign = "CENTER"
    cap = Paragraph(f"Figure {counters['fig']}: {escape(caption)}", S["figcap"])
    return KeepTogether([img, cap])


def screenshot(name, caption, hint, width=15.4 * cm):
    path = SHOTS / name
    if path.exists():
        return figure_from(path, caption, width)
    counters["fig"] += 1
    box = Table([[Paragraph(f"<b>Screenshot not yet added</b><br/>{escape(hint)}<br/><font color='#6B7280'>"
                            f"Expected file: docs/screenshots/{escape(name)}</font>", S["cell"])]],
                colWidths=[width], rowHeights=[4.2 * cm])
    box.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#E9A23B")),
                             ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFF8E1")),
                             ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("ALIGN", (0, 0), (-1, -1), "CENTER")]))
    return KeepTogether([box, Paragraph(f"Figure {counters['fig']}: {escape(caption)}", S["figcap"])])


def figure_from(path, caption, width):
    counters["fig"] += 1
    img = Image(str(path))
    ratio = img.imageHeight / img.imageWidth
    img.drawWidth, img.drawHeight = width, min(width * ratio, 11.5 * cm)
    img.drawWidth = img.drawHeight / ratio
    img.hAlign = "CENTER"
    return KeepTogether([img, Paragraph(f"Figure {counters['fig']}: {escape(caption)}", S["figcap"])])


def table(caption, header, rows, widths, mono_cols=()):
    counters["tab"] += 1
    data = [[Paragraph(escape(h), S["cellh"]) for h in header]]
    for r in rows:
        line = []
        for i, c in enumerate(r):
            txt = escape(str(c))
            line.append(Paragraph(f"<font face='Courier' size='7.8'>{txt}</font>" if i in mono_cols else txt, S["cell"]))
        data.append(line)
    t = Table(data, colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.4, LINE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
    ]))
    return [Paragraph(f"Table {counters['tab']}: {escape(caption)}", S["tabcap"]), t, Spacer(1, 8)]


def source(path, start=None, end=None):
    text = (ROOT / path).read_text(encoding="utf-8")
    if start:
        text = text[text.index(start):]
    if end:
        text = text[:text.index(end)]
    return text.replace("\t", "    ").rstrip()


def facts():
    con = duckdb.connect(str(ROOT / "warehouse" / "superstore.duckdb"), read_only=True)
    one = lambda sql: con.execute(sql).fetchone()
    f = {}
    f["rows"] = one("select count(*) from gold.fact_sales")[0]
    f["sales"], f["profit"] = one("select sum(sales), sum(profit) from gold.fact_sales")
    f["orders"] = one("select count(distinct order_id) from gold.fact_sales")[0]
    f["customers"] = one("select count(*) from gold.dim_customer")[0]
    f["products"] = one("select count(*) from gold.dim_product")[0]
    f["locations"] = one("select count(*) from gold.dim_location")[0]
    f["dates"] = one("select count(*) from gold.dim_date")[0]
    f["first"], f["last"] = one("select min(order_date), max(order_date) from silver.silver_superstore")
    f["rejects"] = one("select count(*) from silver.silver_rejects")[0]
    f["run"] = one("select status, rows_loaded, rows_rejected, round(duration_seconds) from ops.pipeline_run_log order by finished_at desc limit 1")
    f["segments"] = con.execute("select rfm_segment, count(*) from marts.mart_customer_rfm group by 1 order by 2 desc").fetchall()
    f["top_sub"] = con.execute("select sub_category, round(sum(total_profit)) p from marts.mart_category_region_profit group by 1 order by 2 desc limit 3").fetchall()
    f["bad_sub"] = con.execute("select sub_category, round(sum(total_profit)) p from marts.mart_category_region_profit group by 1 order by 2 limit 3").fetchall()
    return f


def build():
    F = facts()
    money = lambda v: f"-${-v:,.0f}" if v < 0 else f"${v:,.0f}"
    W = 16.6 * cm
    s = []

    # ---------------- title page ----------------
    s += [Spacer(1, 3.2 * cm), P(TITLE, "title"), Spacer(1, 6), P("Technical Documentation", "subtitle"),
          P("An ELT pipeline with Apache Airflow, dbt and DuckDB", "subtitle"), Spacer(1, 2.2 * cm)]
    meta = [("STUDENT", AUTHOR), ("INSTITUTION", SCHOOL), ("INSTRUCTOR", INSTRUCTOR),
            ("DATE", f"{date(2026, 10, 3).day} {date(2026, 10, 3):%B %Y}"), ("SOURCE CODE", REPO)]
    for k, v in meta:
        s += [P(k, "metah"), P(escape(v), "meta"), Spacer(1, 5)]
    s += [NextPageTemplate("Body"), PageBreak()]

    # ---------------- contents ----------------
    toc = KindTOC("TOCEntry")
    toc.levelStyles = toc_levels()
    lof = KindTOC("LOFEntry")
    lof.levelStyles = [ParagraphStyle("lf", fontName="Helvetica", fontSize=9, leading=12.5)]
    lot = KindTOC("LOTEntry")
    lot.levelStyles = [ParagraphStyle("lt", fontName="Helvetica", fontSize=9, leading=12.5)]
    s += [P("Table of Contents", "tochead"), toc, PageBreak(),
          P("List of Figures", "tochead"), lof, Spacer(1, 18),
          P("List of Tables", "tochead"), lot, PageBreak()]

    # ---------------- 1 introduction ----------------
    s += [H1("1. Introduction"), H2("1.1 Purpose")]
    s += [P("This document describes the design, implementation, testing and operation of the <b>Superstore Data Pipeline</b>, "
            "an end-to-end ELT (extract, load, transform) project. The pipeline takes a spreadsheet of retail sales orders, "
            "loads it into an analytical database, validates and cleans it, models it as a star schema, and publishes "
            "ready-to-query reporting tables. Apache Airflow runs the whole process on a schedule inside Docker containers.")]
    s += [H2("1.2 The dataset")]
    s += [P(f"The source is <i>Superstore.xlsx</i>, a sample retail dataset with a single sheet named <i>Sample - Superstore</i>. "
            f"It contains {F['rows']:,} order lines and 21 columns that cover orders, shipping, customers, geography, products "
            f"and the measures sales, quantity, discount and profit. Orders run from {F['first']:%d %B %Y} to {F['last']:%d %B %Y}, "
            f"across {F['orders']:,} orders, {F['customers']:,} customers and {F['products']:,} products.")]
    s += [H2("1.3 Goals")]
    s += bullets([
        "Load the Excel file into a database and keep the raw data untouched, with load metadata for traceability.",
        "Clean and type the data, and quarantine invalid rows with a reason instead of silently dropping them.",
        "Model the clean data as a star schema and build analytics tables for monthly sales, profit, customer segments and shipping speed.",
        "Run everything on a schedule, stop automatically when data quality is poor, and record every run.",
        "Make the project reproducible: one command starts the whole stack, and automated tests protect its behaviour.",
    ])
    s += [H2("1.4 Scope and out-of-scope items")]
    s += [P("The project covers ingestion, transformation, orchestration, testing and local deployment. A dashboard layer, e-mail or chat "
            "alerting, CI/CD and cloud deployment are intentionally out of scope.")]

    # ---------------- 2 technology ----------------
    s += [CondPageBreak(8 * cm), H1("2. Technology Stack")]
    s += [P("Each tool was chosen for a clear job and kept as simple as the requirements allow.")]
    s += table("Technologies used", ["Tool", "Version", "Role in the project"], [
        ["Apache Airflow", "3.1.0", "Schedules and monitors the pipeline; one DAG with ten tasks, retries and failure callbacks."],
        ["dbt Core + dbt-duckdb", "1.12.5 / 1.11.0", "SQL transformations, tests, snapshots, documentation."],
        ["DuckDB", "1.5.6", "Embedded analytical database stored in a single file."],
        ["Python, pandas, openpyxl", "3.12 (container)", "Reads the Excel file and loads the bronze table; run logging and quality checks."],
        ["PostgreSQL", "16", "Airflow metadata database (not used for business data)."],
        ["Docker Compose", "v5", "Runs Airflow, PostgreSQL and the tools as one reproducible stack."],
        ["pytest", "-", "Unit tests for the loader, the ops script and the DAG structure."],
        ["Git and GitHub", "-", "Version control and publication of the source code."],
    ], [4.2 * cm, 3.0 * cm, 9.4 * cm])
    s += [P("dbt and the loader are installed in a dedicated Python virtual environment inside the Airflow image. This keeps their "
            "dependencies separate from Airflow's pinned dependencies, so neither side can break the other.")]

    # ---------------- 3 architecture ----------------
    s += [H1("3. System Architecture"), H2("3.1 Overview")]
    s += [P("The system has two parts that share one DuckDB file. The <b>control plane</b> is Airflow, which decides what runs and when. "
            "The <b>data plane</b> is a set of scripts and dbt commands that run as bash tasks and read or write the database. "
            "Figure 1 shows the components and how they connect.")]
    s += [figure("fig_architecture.png", "System architecture")]
    s += [H2("3.2 Containers")]
    s += [P("Docker Compose starts five services. The init service migrates the Airflow metadata database once and exits; the other "
            "Airflow services wait for it. Every Airflow container mounts the same project folders, so a change to a model or script "
            "is visible immediately without rebuilding the image.")]
    s += [figure("fig_docker.png", "Docker Compose services and bind mounts")]
    s += [H2("3.3 Repository layout")]
    s += [code("""
superstore_project/
  airflow/            Dockerfile, requirements.txt, docker-compose.yaml, dags/superstore_pipeline.py
  data/raw/           Superstore.xlsx (source file)
  scripts/            load_bronze.py, pipeline_ops.py, query.py, in_container.sh
  dbt_project/        dbt_project.yml, profiles.yml, macros/, snapshots/, tests/
    models/           bronze (sources), silver, gold, marts
  tests/              pytest: test_load_bronze.py, test_pipeline_ops.py, test_dag.py
  warehouse/          superstore.duckdb (created at run time, not in Git)
  docs/               specification, implementation plan, this document
""")]

    # ---------------- 4 data layers ----------------
    s += [CondPageBreak(8 * cm), H1("4. Data Layers"), H2("4.1 Medallion design")]
    s += [P("The data moves through four layers, each in its own database schema. A layer only reads from the one before it, "
            "which keeps the flow easy to follow and to test.")]
    s += [figure("fig_data_flow.png", "Data flow from the Excel file to the marts")]
    s += table("Layers and their objects", ["Schema", "Objects", "Materialization", "Purpose"], [
        ["bronze", "raw_superstore", "table (Python)", "Raw copy of the file with batch metadata. Append-only."],
        ["silver", "silver_checked, silver_superstore, silver_rejects, silver_customers, silver_products, snap_customer, snap_product",
         "view / table / snapshot", "Typed, trimmed and validated rows; rejected rows with a reason; slowly changing history."],
        ["gold", "dim_customer, dim_product, dim_location, dim_date, fact_sales", "table / incremental",
         "Star schema for analytics."],
        ["marts", "mart_monthly_sales, mart_category_region_profit, mart_customer_rfm, mart_shipping_performance",
         "table", "Ready-to-use reporting tables."],
        ["ops", "pipeline_run_log", "table (Python)", "One row per pipeline run."],
    ], [1.8 * cm, 6.2 * cm, 3.0 * cm, 5.6 * cm], mono_cols=(0,))

    s += [H2("4.2 Bronze: incremental loading")]
    s += [P("<font face='Courier'>scripts/load_bronze.py</font> reads the sheet with pandas and writes "
            "<font face='Courier'>bronze.raw_superstore</font>. Column names are only converted to snake_case; values are not cleaned, "
            "so bronze always shows what the source contained. Three metadata columns are added to every row: "
            "<font face='Courier'>_batch_id</font>, <font face='Courier'>_loaded_at</font> and <font face='Courier'>_source_file</font>.")]
    s += bullets([
        "<b>Incremental:</b> only rows whose <font face='Courier'>row_id</font> is not already loaded are inserted. Running the loader twice on the same file adds zero rows, and a file with extra rows loads only the new ones.",
        "<b>Dates:</b> Excel often stores dates as serial numbers (for example 42682 is 8 November 2016). Numeric cells are converted from serial numbers; text is parsed; anything unreadable becomes null and is rejected later in silver instead of crashing the load.",
        "<b>Postal codes:</b> they are read as text, and numeric codes that lost their leading zero (449 rows, such as 6824) are padded back to five digits (06824).",
        "<b>Fail early:</b> a missing file, a missing sheet, missing columns, a header-only sheet, empty or duplicate <font face='Courier'>row_id</font> values all stop the load with a clear message.",
    ])

    s += [H2("4.3 Silver: validation and typing")]
    s += [P("The model <font face='Courier'>silver_checked</font> casts every column to its real type, trims text, and computes a "
            "<font face='Courier'>reject_reason</font> with five rules (Table 3). Two models then split the result: "
            "<font face='Courier'>silver_superstore</font> keeps the valid rows and <font face='Courier'>silver_rejects</font> keeps the "
            "others together with the reason. Nothing is deleted, so every bronze row ends up in exactly one of the two tables; a dbt "
            "reconciliation test enforces this.")]
    s += table("Reject rules (evaluated in this order)", ["Reason", "Condition"], [
        ["missing key", "row_id, order_id, customer_id or product_id is null"],
        ["missing date", "order_date or ship_date is null (or unreadable)"],
        ["non-positive sales", "sales is null or sales <= 0"],
        ["ship date before order date", "ship_date < order_date"],
        ["discount out of range", "discount < 0 or discount > 1"],
    ], [5.0 * cm, 11.6 * cm], mono_cols=(0,))
    s += [P(f"On the real dataset all {F['rows']:,} rows pass, so {F['rejects']} rows are rejected. The rules matter for future files.")]
    s += [P("<font face='Courier'>silver_customers</font> and <font face='Courier'>silver_products</font> hold one row per customer and "
            "product, using the most recent attributes. A few product IDs appear with more than one name in the source, so the latest "
            "name is kept. Two dbt snapshots, <font face='Courier'>snap_customer</font> and <font face='Courier'>snap_product</font>, "
            "keep the history of those attributes using the <i>check</i> strategy (slowly changing dimension, type 2). Because the "
            "dataset is static, each key has a single version until the source changes.")]

    s += [H2("4.4 Gold: star schema")]
    s += [P("The gold layer builds four dimensions and one fact table. Surrogate keys are the MD5 hash of the natural key, so they stay "
            "identical every time a dimension is rebuilt and the fact table never loses its links. "
            "<font face='Courier'>dim_date</font> is generated as a calendar that covers every order and ship date. "
            "<font face='Courier'>fact_sales</font> is an <i>incremental</i> model with <font face='Courier'>row_id</font> as unique key: "
            "on later runs it only processes rows loaded after the newest row already in the table.")]
    s += [H2("4.5 Marts")]
    s += table("Analytics marts", ["Mart", "Grain", "What it answers"], [
        ["mart_monthly_sales", "month", "Sales, profit, orders and units per month."],
        ["mart_category_region_profit", "category, sub-category, region", "Where is the business profitable?"],
        ["mart_customer_rfm", "customer", "Recency, frequency, monetary value, quartile scores and a segment label."],
        ["mart_shipping_performance", "ship mode, region", "Average and maximum days from order to shipment."],
    ], [5.8 * cm, 4.4 * cm, 6.4 * cm], mono_cols=(0,))
    s += [P("RFM scores use quartiles (1 to 4) of recency, frequency and monetary value. Recency is measured from the newest order in the data, "
            "not from today's date, so the result does not change as time passes. Segments are assigned in this order: "
            "<b>Champion</b> (R, F and M all 3 or higher), <b>Loyal</b> (R 3 or higher and F 2 or higher), <b>At risk</b> "
            "(R 2 or lower and F 3 or higher), <b>Lost</b> (R equal to 1), and <b>Regular</b> for the rest.")]

    # ---------------- 5 orchestration ----------------
    s += [CondPageBreak(8 * cm), H1("5. Orchestration with Airflow"), H2("5.1 The DAG")]
    s += [P("The DAG <font face='Courier'>superstore_pipeline</font> runs daily and has ten tasks in a straight line. Every task is a bash "
            "command, written with Airflow's TaskFlow <font face='Courier'>@task.bash</font> decorator. DuckDB allows only one writer at a time, "
            "so the tasks run one after another and only one DAG run is active at once.")]
    s += [figure("fig_dag.png", "The superstore_pipeline DAG")]
    s += table("DAG tasks", ["Task", "Command", "Purpose"], [
        ["check_source_file", "test -f $SUPERSTORE_FILE", "Fail fast if the Excel file is missing."],
        ["dbt_debug", "dbt debug", "Check the dbt project and database connection."],
        ["load_bronze", "load_bronze.py", "Append new rows to bronze."],
        ["test_bronze", "dbt test --select source:bronze", "Not-null and unique checks on the raw table."],
        ["build_silver", "dbt run --select silver", "Type, validate and split the data."],
        ["snapshot", "dbt snapshot", "Record customer and product history."],
        ["test_silver", "dbt test (silver) + check-rejects", "Quality gate: dbt tests, then the reject-rate limit."],
        ["build_gold", "dbt run --select gold marts", "Build dimensions, fact table and marts."],
        ["test_gold", "dbt test (gold, marts) + docs generate", "Keys, relationships, reconciliation; refresh dbt docs."],
        ["log_run", "pipeline_ops.py log", "Write the success row to ops.pipeline_run_log."],
    ], [3.4 * cm, 6.0 * cm, 7.2 * cm], mono_cols=(0,))

    s += [H2("5.2 Quality gate")]
    s += [P("The gold layer is built only if <font face='Courier'>test_silver</font> succeeds. It runs the silver dbt tests and then "
            "<font face='Courier'>pipeline_ops.py check-rejects</font>, which compares the rejected rows of the <i>current batch</i> with "
            "the rows that batch loaded. If the rate is above the limit (2% by default), the task fails and the later tasks do not run. "
            "The limit can be changed with the Airflow Variable <font face='Courier'>reject_rate_threshold</font>, with no code change. "
            "A run that loads no new rows always passes this check.")]
    s += [H2("5.3 Failure handling and run log")]
    s += [P("Each task retries twice, one minute apart. When a task finally fails, the <font face='Courier'>on_failure_callback</font> "
            "writes a row with status <i>failed</i> and the name of the failing task to <font face='Courier'>ops.pipeline_run_log</font>. "
            "The callback works even if no bronze or silver tables exist yet, for example when the source file is missing. "
            "Successful runs are logged by the last task. Each row records the batch id, status, rows loaded, rows rejected, "
            "duration, failed task and finish time.")]
    s += table("Configuration", ["Setting", "Where", "Meaning"], [
        ["SUPERSTORE_FILE", "docker-compose.yaml", "Path of the source Excel file inside the containers."],
        ["SUPERSTORE_DB", "docker-compose.yaml", "Path of the DuckDB file; also used by dbt through profiles.yml."],
        ["PIPELINE_BIN", "Dockerfile", "Folder of the dedicated virtual environment (dbt and python)."],
        ["reject_rate_threshold", "Airflow Variable", "Maximum share of rejected rows per batch (default 0.02)."],
        ["schedule / retries", "DAG file", "@daily, 2 retries with a 1-minute delay, max_active_runs = 1."],
    ], [4.2 * cm, 3.8 * cm, 8.6 * cm], mono_cols=(0,))

    # ---------------- 6 data model ----------------
    s += [CondPageBreak(8 * cm), H1("6. Data Model"), H2("6.1 Star schema")]
    s += [P(f"Figure 5 shows the gold layer. One fact table at order-line grain ({F['rows']:,} rows) references four dimensions "
            f"({F['customers']:,} customers, {F['products']:,} products, {F['locations']:,} locations and {F['dates']:,} calendar days). "
            "All relationships are many-to-one and are verified by dbt tests.")]
    s += [figure("fig_star_schema.png", "Star schema of the gold layer")]
    s += [H2("6.2 Data dictionary")]
    cw = [3.7 * cm, 2.4 * cm, 10.5 * cm]
    hdr = ["Column", "Type", "Description"]
    s += table("bronze.raw_superstore (metadata columns; the other 21 columns mirror the source file in snake_case)", hdr, [
        ["_batch_id", "VARCHAR", "Airflow run id of the load that inserted the row."],
        ["_loaded_at", "TIMESTAMP", "UTC time the row was loaded; drives the incremental fact table."],
        ["_source_file", "VARCHAR", "Name of the Excel file."],
    ], cw, mono_cols=(0, 1))
    s += table("gold.dim_customer", hdr, [
        ["customer_key", "VARCHAR", "Surrogate key, md5(customer_id). Primary key."],
        ["customer_id", "VARCHAR", "Natural key from the source, for example CG-12520."],
        ["customer_name", "VARCHAR", "Customer name."],
        ["segment", "VARCHAR", "Consumer, Corporate or Home Office."],
    ], cw, mono_cols=(0, 1))
    s += table("gold.dim_product", hdr, [
        ["product_key", "VARCHAR", "Surrogate key, md5(product_id). Primary key."],
        ["product_id", "VARCHAR", "Natural key, for example FUR-BO-10001798."],
        ["product_name", "VARCHAR", "Latest product name for the id."],
        ["category", "VARCHAR", "Furniture, Office Supplies or Technology."],
        ["sub_category", "VARCHAR", "Sub-category such as Bookcases or Phones."],
    ], cw, mono_cols=(0, 1))
    s += table("gold.dim_location", hdr, [
        ["location_key", "VARCHAR", "Surrogate key, md5 of country, state, city and postal code. Primary key."],
        ["country, state, city", "VARCHAR", "Address levels."],
        ["postal_code", "VARCHAR", "Five-digit postal code kept as text."],
        ["region", "VARCHAR", "Central, East, South or West."],
    ], cw, mono_cols=(0, 1))
    s += table("gold.dim_date", hdr, [
        ["date_key", "INTEGER", "Date as yyyymmdd. Primary key."],
        ["date_day", "DATE", "Calendar date."],
        ["calendar_year / quarter / month", "BIGINT", "Date parts."],
        ["month_name, day_name", "VARCHAR", "Names for reporting."],
        ["day_of_month, day_of_week", "BIGINT", "Day of month; day of week (0 = Sunday)."],
        ["is_weekend", "BOOLEAN", "True for Saturday and Sunday."],
    ], cw, mono_cols=(0, 1))
    s += table("gold.fact_sales (grain: one order line)", hdr, [
        ["row_id", "INTEGER", "Source Row ID. Primary key and incremental unique key."],
        ["order_id", "VARCHAR", "Order number; several lines can share it."],
        ["customer_key, product_key, location_key", "VARCHAR", "Foreign keys to the dimensions."],
        ["order_date_key, ship_date_key", "INTEGER", "Foreign keys to dim_date."],
        ["ship_mode", "VARCHAR", "Same Day, First Class, Second Class or Standard Class."],
        ["sales", "DOUBLE", "Revenue of the line in USD."],
        ["quantity", "INTEGER", "Units sold."],
        ["discount", "DOUBLE", "Discount rate between 0 and 1."],
        ["profit", "DOUBLE", "Profit in USD; negative when the line lost money."],
        ["_batch_id, _loaded_at", "VARCHAR / TIMESTAMP", "Lineage columns copied from silver."],
    ], cw, mono_cols=(0, 1))
    s += table("marts.* (columns of the four marts)", ["Mart", "Columns", "Notes"], [
        ["mart_monthly_sales", "month_start, total_sales, total_profit, order_count, units_sold", "One row per month (48)."],
        ["mart_category_region_profit", "category, sub_category, region, total_sales, total_profit, profit_margin", "Margin = profit / sales."],
        ["mart_customer_rfm", "customer_key, customer_name, last_order_date, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_segment",
         "One row per customer."],
        ["mart_shipping_performance", "ship_mode, region, order_lines, avg_days_to_ship, max_days_to_ship", "16 rows."],
    ], [4.4 * cm, 8.4 * cm, 3.8 * cm], mono_cols=(0,))
    s += table("ops.pipeline_run_log", hdr, [
        ["batch_id", "VARCHAR", "Airflow run id."],
        ["status", "VARCHAR", "success or failed."],
        ["rows_loaded, rows_rejected", "INTEGER", "Counts for this batch only."],
        ["duration_seconds", "DOUBLE", "Time from the start of the run to the log entry."],
        ["failed_task", "VARCHAR", "Task that failed (null on success)."],
        ["finished_at", "TIMESTAMP", "UTC time of the log entry."],
    ], cw, mono_cols=(0, 1))

    # ---------------- 7 testing ----------------
    s += [CondPageBreak(8 * cm), H1("7. Testing and Data Quality"), H2("7.1 Approach")]
    s += [P("Code was written test-first wherever it is Python: a failing test was written, run to see it fail for the right reason, "
            "and only then was the code written to make it pass. The same discipline was used for the silver reject rules through a dbt "
            "unit test. Bugs that appeared later were first captured by a failing test and then fixed. The checks fall into three groups.")]
    s += table("Automated checks", ["Group", "What is checked", "Where"], [
        ["Python unit tests (pytest)",
         "Loader: first load, re-run adds nothing, extra rows append, leading-zero postal codes, Excel serial dates, text in a date column, "
         "missing file, wrong sheet, missing column, duplicate row_id, header-only file. "
         "Ops script: reject-rate logic, per-batch gate, run logging, failure logging before any table exists.",
         "tests/test_load_bronze.py, tests/test_pipeline_ops.py"],
        ["DAG tests (inside the Airflow image)",
         "Every task keeps its own id, the tasks run in the documented order, bronze tests do not pull in later-layer checks, "
         "the gate judges the current batch.", "tests/test_dag.py"],
        ["dbt data tests",
         "Not-null and unique keys, accepted values for ship mode and RFM segment, relationships from the fact table to all four dimensions, "
         "and reconciliation tests (bronze = silver + rejects; fact rows and total sales equal silver).",
         "models/*/schema.yml, dbt_project/tests/"],
        ["dbt unit tests",
         "Silver: five artificial rows must get the exact reject reason. Marts: customers with tied RFM values must get stable, key-ordered scores.",
         "models/silver/unit_tests.yml, models/marts/unit_tests.yml"],
    ], [3.4 * cm, 8.0 * cm, 5.2 * cm])
    s += [H2("7.2 Results")]
    s += table("Test results at the end of the project", ["Suite", "Result"], [
        ["pytest on the host (loader and ops script)", "22 passed, 1 skipped (the DAG tests need Airflow)"],
        ["pytest inside the Airflow image (DAG tests)", "4 passed"],
        ["dbt build (models, snapshots, tests)", "65 of 65 nodes passed"],
        ["End-to-end Airflow run on an empty warehouse", f"success, {F['rows']:,} rows loaded, {F['rejects']} rejected"],
        ["Second run with the same file", "success, 0 rows loaded, fact table unchanged"],
        ["Run with the source file removed", "failed after retries; logged with failed_task = check_source_file"],
    ], [8.0 * cm, 8.6 * cm])
    s += [H2("7.3 Why reconciliation tests matter")]
    s += [P("Unique and not-null tests check individual tables, but they cannot show that rows went missing between layers. Two singular "
            "tests close that gap: bronze rows must equal silver rows plus rejected rows, and the fact table must have the same number of "
            "rows and the same total sales as silver. A join that silently drops rows, for example a missing dimension key, makes these "
            "tests fail.")]

    # ---------------- 8 operation ----------------
    s += [H1("8. Deployment and Operation"), H2("8.1 Prerequisites")]
    s += bullets(["Docker Desktop with the WSL2 backend (Windows) or Docker Engine with the Compose plugin.",
                  "The file data/raw/Superstore.xlsx (included in the repository)."])
    s += [H2("8.2 Start the stack")]
    s += [code("""
git clone https://github.com/omarmohosny/superstore-pipeline.git
cd superstore-pipeline
docker compose -f airflow/docker-compose.yaml up -d --build
# Airflow UI: http://localhost:8080  (no login in this local setup)
""")]
    s += [P("In the UI, unpause <font face='Courier'>superstore_pipeline</font> and trigger a run. A full run takes about one minute.")]
    s += [H2("8.3 Useful commands")]
    s += [code("""
# look at any table
bash scripts/in_container.sh '$PIPELINE_BIN/python $PROJECT_DIR/scripts/query.py "select count(*) from gold.fact_sales"'

# run every dbt model and test by hand
bash scripts/in_container.sh '$PIPELINE_BIN/dbt build'

# host tests (loader and ops script)
python -m pytest
""")]
    s += [H2("8.4 Loading corrected data")]
    s += [P("Bronze is append-only by <font face='Courier'>row_id</font>. If a row that is already loaded is corrected in the Excel file, "
            "delete it from bronze first so the next run loads it again: "
            "<font face='Courier'>delete from bronze.raw_superstore where row_id in (...)</font>.")]

    # ---------------- 9 results ----------------
    s += [CondPageBreak(8 * cm), H1("9. Results"), H2("9.1 Business results from the marts")]
    s += [P(f"The pipeline produced {money(F['sales'])} in total sales and {money(F['profit'])} in total profit "
            f"({F['profit'] / F['sales']:.1%} margin) across {F['orders']:,} orders. Figures 6 to 8 are drawn directly from the mart tables.")]
    s += [figure("chart_monthly.png", "Monthly sales and profit, 2014 to 2017 (mart_monthly_sales)")]
    s += [figure("chart_category_rfm.png", "Sales and profit by category, and customers per RFM segment")]
    top = ", ".join(f"{n} ({money(p)})" for n, p in F["top_sub"])
    bad = ", ".join(f"{n} ({money(p)})" for n, p in F["bad_sub"])
    s += [P(f"Technology is the most profitable category. The most profitable sub-categories are {top}. "
            f"The weakest are {bad}; negative values mean those sub-categories lose money overall, which is a useful finding for discount policy. "
            "Sales follow a clear seasonal pattern with peaks in September, November and December, and sales grow from 2015 to 2017.")]
    s += [figure("chart_shipping.png", "Average days from order to shipment by ship mode", width=11.5 * cm)]
    s += [H2("9.2 Pipeline evidence")]
    st, loaded, rejected, secs = F["run"]
    s += [P(f"The most recent logged run has status <b>{st}</b>, loaded {loaded:,} rows, rejected {rejected} and took about {secs:.0f} seconds. "
            "The following screenshots come from the running system.")]
    s += [screenshot("airflow_graph.png", "Airflow graph view of the DAG with all tasks successful",
                     "Airflow UI, DAGs, superstore_pipeline, Graph view of a successful run.")]
    s += [screenshot("airflow_runs.png", "Airflow run history with successful and failed runs",
                     "Airflow UI, superstore_pipeline, Runs tab.")]
    s += [screenshot("airflow_gate_log.png", "Log of the test_silver quality gate",
                     "Airflow UI, open task test_silver, Logs tab (shows the reject-rate line).")]
    s += [screenshot("docker_containers.png", "The five containers of the stack in Docker Desktop",
                     "Docker Desktop, Containers.")]
    s += [screenshot("dbt_lineage.png", "dbt lineage graph from the generated documentation",
                     "dbt docs, lineage graph (optional).")]

    # ---------------- 10 issues ----------------
    s += [CondPageBreak(8 * cm), H1("10. Issues Found and Fixed")]
    s += [P("Running the system end to end, reviewing the code and preparing this document found several defects. Each one was captured by a test that failed before "
            "the fix and passes after it.")]
    s += table("Defects and their fixes", ["#", "Defect", "How it was found", "Fix and regression test"], [
        ["1", "Task ids became check_source_file__1, __2, ... because one shared decorator object was reused for every task.",
         "First end-to-end run in Airflow.", "Create a fresh decorator for each task. test_every_task_keeps_its_own_id."],
        ["2", "test_bronze also ran a reconciliation test that depends on silver, which did not exist yet on an empty warehouse.",
         "First end-to-end run (task failed).", "Add --indirect-selection cautious. test_bronze_tests_do_not_pull_in_silver_checks."],
        ["3", "Host pytest tried to import the project's own airflow/ folder as the Airflow package.",
         "Running the suite on the host.", "Skip on airflow.models instead of airflow."],
        ["4", "449 postal codes lost their leading zero (06824 became 6824) because Excel stores them as numbers.",
         "Code review, confirmed on the real file.", "Pad numeric codes to five digits. test_numeric_postal_code_gets_its_leading_zero_back."],
        ["5", "One text value in a date column would have broken the silver build and later loads.",
         "Code review.", "Convert dates cell by cell; unreadable values become null and are rejected. test_text_in_a_date_column_becomes_null_and_later_loads_still_work."],
        ["6", "The reject gate used lifetime totals, so old rejects counted forever and new bad rows could hide in a large history.",
         "Code review.", "Judge only the current batch. test_old_rejects_do_not_count_against_a_new_batch, test_reject_gate_judges_the_current_batch."],
        ["7", "A test for text dates passed too early because pandas reads the string N/A as an empty value.",
         "Watching the test fail before the fix.", "Use a different text value (TBD) so the test fails for the right reason."],
        ["8", "RFM segment counts changed between two runs on identical data, because quartile scoring (ntile) gave tied customers an arbitrary order.",
         "Comparing two runs while preparing this document.", "Add customer_key as a tie-breaker to all three scores. Unit test rfm_scores_break_ties_by_customer_key."],
    ], [0.7 * cm, 5.4 * cm, 3.6 * cm, 6.9 * cm])
    s += [H2("Known limitations")]
    s += bullets([
        "Gold dimensions read the current silver tables, not the snapshots, so the SCD2 history is stored but not yet used by reports.",
        "The snapshot task runs before the quality gate, so history from a batch that the gate later rejects is kept.",
        "<font face='Courier'>fact_sales</font> never deletes rows. If a reject rule becomes stricter, run dbt with <font face='Courier'>--full-refresh</font>.",
        "DuckDB has one writer: reading the database while a DAG run is writing can fail that task; the retries normally cover it.",
        "The Compose file has no user mapping, so the mounted warehouse folder may need permissions on Linux hosts.",
    ])

    # ---------------- 11 future / conclusion ----------------
    s += [H1("11. Future Work"), ]
    s += bullets(["Add a dashboard (for example Streamlit or Metabase) on top of the marts.",
                  "Send failure alerts by e-mail or chat from the existing callback function.",
                  "Use the snapshots in the dimensions to support point-in-time reporting.",
                  "Add CI that runs the pytest suite and dbt build on every push.",
                  "Move the warehouse to a server database if several writers are needed."])
    s += [H1("12. Conclusion")]
    s += [P("The project delivers a complete, tested ELT pipeline: an Excel file becomes a validated star schema and four analytics tables, "
            "built on a schedule, with a quality gate that stops bad data and a log that records every run. Writing the Python parts "
            "test-first, adding reconciliation tests between layers, and reviewing the finished system caught real defects early. "
            "The Docker setup lets anyone reproduce the result with a single command.")]

    # ---------------- appendix ----------------
    s += [PageBreak(), H1("Appendix A: Key Source Code")]
    s += [H2("A.1 Reject rules (dbt model silver_checked)")]
    s += [code(source("dbt_project/models/silver/silver_checked.sql", start="select\n    *,"))]
    s += [H2("A.2 Incremental fact table (dbt model fact_sales)")]
    s += [code(source("dbt_project/models/gold/fact_sales.sql"))]
    s += [H2("A.3 Bronze loader core (scripts/load_bronze.py)")]
    s += [code(source("scripts/load_bronze.py", start="def to_date(col):", end="def main():"))]
    s += [H2("A.4 DAG definition (airflow/dags/superstore_pipeline.py)")]
    s += [code(source("airflow/dags/superstore_pipeline.py", start="TASK_ENV"))]

    doc = Doc(OUT)
    doc.multiBuild(s)
    print("written", OUT, "pages:", doc.page)


if __name__ == "__main__":
    build()
