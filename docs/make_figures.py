"""Builds the figures used in the technical documentation (docs/figures/*.png)."""
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "figures"
OUT.mkdir(parents=True, exist_ok=True)

NAVY, BLUE, TEAL, AMBER, GRAY, RED = "#1F3A5F", "#2E6FBA", "#2A9D8F", "#E9A23B", "#6B7280", "#C0392B"
BRONZE, SILVER, GOLD = "#B87333", "#8A94A6", "#D4A017"
LIGHT = "#F3F6FA"


def canvas(w, h):
    fig, ax = plt.subplots(figsize=(w, h))
    ax.set_xlim(0, w * 10)
    ax.set_ylim(0, h * 10)
    ax.axis("off")
    return fig, ax


def box(ax, x, y, w, h, text, fill=LIGHT, edge=NAVY, color=NAVY, size=9, bold=False, style="round,pad=0.4,rounding_size=1.5"):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=style, facecolor=fill, edgecolor=edge, linewidth=1.3))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=size, color=color,
            fontweight="bold" if bold else "normal", linespacing=1.35)


def arrow(ax, x1, y1, x2, y2, color=NAVY, style="-|>", ls="-", rad=0.0):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle=style, mutation_scale=12, color=color,
                                 linewidth=1.4, linestyle=ls, connectionstyle=f"arc3,rad={rad}"))


def save(fig, name):
    fig.savefig(OUT / name, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def architecture():
    fig, ax = canvas(11, 6.4)
    ax.add_patch(FancyBboxPatch((18, 4), 90, 56, boxstyle="round,pad=0.4,rounding_size=2",
                                facecolor="#FAFBFD", edgecolor=GRAY, linewidth=1.2, linestyle="--"))
    ax.text(21, 58, "Docker Compose stack", fontsize=9, color=GRAY, fontweight="bold", va="top")

    box(ax, 1, 26, 14, 12, "Superstore\n.xlsx\n(9,994 rows)", fill="#FFF4E0", edge=AMBER, size=9, bold=True)

    box(ax, 24, 40, 38, 14, "Apache Airflow 3.1\napi-server  |  scheduler  |  dag-processor\nDAG: superstore_pipeline", fill="#E8F0FB", edge=BLUE, size=9, bold=True)
    box(ax, 70, 44, 30, 10, "PostgreSQL 16\n(Airflow metadata)", fill=LIGHT, edge=GRAY, size=9)

    box(ax, 24, 8, 38, 24, "pipeline_venv\n\nscripts/load_bronze.py (pandas)\ndbt Core 1.12 + dbt-duckdb\nscripts/pipeline_ops.py", fill="#E6F5F3", edge=TEAL, size=9)

    box(ax, 70, 8, 34, 24, "DuckDB file\nsuperstore.duckdb\n\nbronze | silver | gold\nmarts | ops", fill="#FFF8E1", edge=GOLD, size=9, bold=True)

    arrow(ax, 43, 40, 43, 32.8, color=BLUE)
    ax.text(44.5, 36.3, "bash tasks", fontsize=8, color=BLUE)
    arrow(ax, 62.8, 20, 69.2, 20, color=TEAL)
    ax.text(66, 23, "read /\nwrite", fontsize=7.5, color=TEAL, ha="center")
    arrow(ax, 62.8, 47, 69.2, 47, color=GRAY, style="<|-|>")
    arrow(ax, 15.2, 32, 23.2, 24, color=AMBER, rad=-0.15)
    ax.text(5, 20, "mounted\n./data", fontsize=8, color=AMBER, ha="center")
    save(fig, "fig_architecture.png")


def data_flow():
    fig, ax = canvas(11, 6.4)
    box(ax, 1, 24, 12, 12, "Excel\nsource", fill="#FFF4E0", edge=AMBER, bold=True)
    layers = [
        (18, "BRONZE", "raw_superstore\nappend-only\n+ batch metadata", BRONZE),
        (46, "SILVER", "silver_superstore\nsilver_rejects\nsnapshots", SILVER),
        (74, "GOLD", "dim_customer / product\ndim_location / date\nfact_sales", GOLD),
    ]
    for x, title, body, c in layers:
        box(ax, x, 18, 22, 24, "", fill="white", edge=c)
        ax.add_patch(FancyBboxPatch((x, 33), 22, 9, boxstyle="round,pad=0.4,rounding_size=1.5", facecolor=c, edgecolor=c))
        ax.text(x + 11, 37.5, title, ha="center", va="center", fontsize=10, color="white", fontweight="bold")
        ax.text(x + 11, 25, body, ha="center", va="center", fontsize=8, color=NAVY, linespacing=1.4)
    box(ax, 74, 2, 22, 12, "MARTS\nmonthly sales\ncategory / region\nRFM | shipping", fill="#E6F5F3", edge=TEAL, size=8, bold=True)
    box(ax, 46, 2, 22, 10, "ops.pipeline_run_log\n(one row per run)", fill=LIGHT, edge=GRAY, size=8)
    box(ax, 46, 49, 22, 9, "Reject rules\n(5 checks)", fill="#FDECEA", edge=RED, color=RED, size=8.5, bold=True)

    arrow(ax, 13.8, 30, 17.2, 30)
    arrow(ax, 40.8, 30, 45.2, 30)
    arrow(ax, 68.8, 30, 73.2, 30)
    ax.text(15.5, 32, "load", fontsize=7.5, ha="center", color=NAVY)
    ax.text(43, 27, "clean", fontsize=7.5, ha="center", color=NAVY)
    ax.text(71, 27, "model", fontsize=7.5, ha="center", color=NAVY)
    arrow(ax, 85, 17.2, 85, 14.8, color=TEAL)
    arrow(ax, 57, 48.4, 57, 43.0, color=RED, ls="--")
    arrow(ax, 57, 17.2, 57, 12.8, color=GRAY, ls="--")
    ax.text(58.5, 45.5, "bad rows", fontsize=8, color=RED)
    save(fig, "fig_data_flow.png")


def dag():
    fig, ax = canvas(11, 5.2)
    row1 = ["check_source\n_file", "dbt_debug", "load_bronze", "test_bronze", "build_silver"]
    row2 = ["snapshot", "test_silver\n(GATE)", "build_gold", "test_gold", "log_run"]
    w, h = 17, 9
    xs = [2 + i * 21 for i in range(5)]
    for i, name in enumerate(row1):
        box(ax, xs[i], 34, w, h, name, fill="#E8F0FB", edge=BLUE, size=8.5, bold=True)
        if i:
            arrow(ax, xs[i - 1] + w + 0.4, 38.5, xs[i] - 0.4, 38.5)
    for i, name in enumerate(row2):
        gate = "GATE" in name
        box(ax, xs[i], 14, w, h, name, fill="#FDECEA" if gate else "#E8F0FB", edge=RED if gate else BLUE,
            color=RED if gate else NAVY, size=8.5, bold=True)
        if i:
            arrow(ax, xs[i - 1] + w + 0.4, 18.5, xs[i] - 0.4, 18.5)
    # connector from the end of row 1 to the start of row 2
    cx = xs[4] + w / 2
    ax.plot([cx, cx], [33.4, 28.5], color=NAVY, linewidth=1.4)
    ax.plot([cx, xs[0] + w / 2], [28.5, 28.5], color=NAVY, linewidth=1.4)
    arrow(ax, xs[0] + w / 2, 28.5, xs[0] + w / 2, 23.6)
    box(ax, 20, 1, 62, 7, "on_failure_callback  ->  ops.pipeline_run_log (status = failed, failed_task)", fill="#FFF8E1", edge=GOLD, size=8, color=NAVY)
    arrow(ax, 51, 13.4, 51, 8.9, color=RED, ls="--")
    ax.text(2, 49, "Retries: 2 (1 minute apart)   |   max_active_runs = 1   |   schedule: @daily", fontsize=8.5, color=GRAY)
    save(fig, "fig_dag.png")


def star_schema():
    fig, ax = canvas(11, 6.6)
    box(ax, 36, 24, 38, 24,
        "fact_sales\n\nrow_id (PK)  |  order_id\ncustomer_key (FK)  product_key (FK)\nlocation_key (FK)\norder_date_key (FK)  ship_date_key (FK)\nship_mode  sales  quantity\ndiscount  profit",
        fill="#FFF8E1", edge=GOLD, size=8.5, bold=True)
    box(ax, 2, 46, 28, 16, "dim_customer\n\ncustomer_key (PK)\ncustomer_id\ncustomer_name  segment", fill="#E8F0FB", edge=BLUE, size=8.5)
    box(ax, 78, 46, 30, 16, "dim_product\n\nproduct_key (PK)\nproduct_id  product_name\ncategory  sub_category", fill="#E8F0FB", edge=BLUE, size=8.5)
    box(ax, 2, 4, 28, 16, "dim_location\n\nlocation_key (PK)\ncountry  state  city\npostal_code  region", fill="#E8F0FB", edge=BLUE, size=8.5)
    box(ax, 78, 4, 30, 16, "dim_date\n\ndate_key (PK, yyyymmdd)\ndate_day  calendar_year\ncalendar_quarter  calendar_month\nday_name  is_weekend", fill="#E8F0FB", edge=BLUE, size=8.2)
    arrow(ax, 36, 42, 30.8, 52, style="-", color=GRAY)
    arrow(ax, 74, 42, 77.2, 52, style="-", color=GRAY)
    arrow(ax, 36, 30, 30.8, 16, style="-", color=GRAY)
    arrow(ax, 74, 30, 77.2, 16, style="-", color=GRAY)
    ax.text(55, 54, "N : 1 relationships\n(enforced by dbt tests)", ha="center", fontsize=8.5, color=GRAY, style="italic")
    save(fig, "fig_star_schema.png")


def docker_services():
    fig, ax = canvas(11, 4.2)
    names = [("postgres", "postgres:16\nhealthcheck"), ("airflow-init", "airflow db migrate\n(runs once)"),
             ("airflow-apiserver", "UI + API\nport 8080"), ("airflow-scheduler", "schedules and\nruns tasks"),
             ("airflow-dag-processor", "parses DAG files\nruns callbacks")]
    w = 18
    for i, (n, d) in enumerate(names):
        x = 1 + i * 21
        box(ax, x, 22, w, 16, f"{n}\n\n{d}", fill="#E8F0FB" if i else LIGHT, edge=BLUE if i else GRAY, size=7.6)
    arrow(ax, 19.4, 30, 21.6, 30, color=GRAY)
    ax.text(2, 15, "Start order: postgres (healthy)  ->  airflow-init (migrates the metadata DB, exits 0)  ->  apiserver, scheduler, dag-processor", fontsize=8, color=NAVY)
    mounts = ["./airflow/dags", "./scripts", "./dbt_project", "./data", "./warehouse"]
    ax.text(2, 8, "Bind mounts in every Airflow container:  " + "   ".join(mounts), fontsize=8, color=TEAL)
    save(fig, "fig_docker.png")


def charts():
    con = duckdb.connect(str(ROOT / "warehouse" / "superstore.duckdb"), read_only=True)
    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})

    monthly = con.execute("select month_start, total_sales, total_profit from marts.mart_monthly_sales order by 1").fetchall()
    fig, ax = plt.subplots(figsize=(10, 3.8))
    x = [r[0] for r in monthly]
    ax.plot(x, [r[1] / 1000 for r in monthly], color=BLUE, linewidth=2, label="Sales")
    ax.plot(x, [r[2] / 1000 for r in monthly], color=TEAL, linewidth=2, label="Profit")
    ax.set_ylabel("Thousand USD")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(frameon=False)
    save(fig, "chart_monthly.png")

    cat = con.execute("select category, sum(total_sales), sum(total_profit) from marts.mart_category_region_profit group by 1 order by 2 desc").fetchall()
    seg = con.execute("select rfm_segment, count(*) from marts.mart_customer_rfm group by 1 order by 2 desc").fetchall()
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    pos = range(len(cat))
    a1.bar([p - 0.2 for p in pos], [c[1] / 1000 for c in cat], 0.4, color=BLUE, label="Sales")
    a1.bar([p + 0.2 for p in pos], [c[2] / 1000 for c in cat], 0.4, color=TEAL, label="Profit")
    a1.set_xticks(list(pos))
    a1.set_xticklabels([c[0] for c in cat])
    a1.set_ylabel("Thousand USD")
    a1.set_title("Sales and profit by category", fontsize=10)
    a1.legend(frameon=False)
    a2.bar([s[0] for s in seg], [s[1] for s in seg], color=[NAVY, BLUE, TEAL, AMBER, GRAY][:len(seg)])
    a2.set_ylabel("Customers")
    a2.set_title("Customers per RFM segment", fontsize=10)
    for i, s in enumerate(seg):
        a2.text(i, s[1] + 2, str(s[1]), ha="center", fontsize=8)
    save(fig, "chart_category_rfm.png")

    ship = con.execute("select ship_mode, sum(avg_days_to_ship*order_lines)/sum(order_lines) from marts.mart_shipping_performance group by 1 order by 2").fetchall()
    fig, ax = plt.subplots(figsize=(6.5, 3.2))
    ax.barh([s[0] for s in ship], [s[1] for s in ship], color=BLUE)
    for i, s in enumerate(ship):
        ax.text(s[1] + 0.05, i, f"{s[1]:.2f}", va="center", fontsize=8)
    ax.set_xlabel("Average days from order to shipment")
    save(fig, "chart_shipping.png")


if __name__ == "__main__":
    architecture()
    data_flow()
    dag()
    star_schema()
    docker_services()
    charts()
    print("figures written to", OUT)
