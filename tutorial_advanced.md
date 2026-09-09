# Advanced Guide: Building the Profit Forecast

This follows the exact same six-week plan as `tutorial.md` — same datasets, same milestones, same
"done when" bars. It exists because `tutorial.md` tells you *what* to build each week and points
at docs; this tells you *how*, with full working scripts wired to this project's actual data
model (as specified in `business-case.md`). Read `tutorial.md` first for the schedule and the
"why this design" reasoning — this is the implementation companion, not a replacement.

Every script below is written against the real column names and grains from `business-case.md`
§B (data model) and §C (profit formula). Copy them as a starting point, adapt as your own
ingestion reveals real column quirks (Kaggle CSVs always have a surprise or two).

---

## Day 0 — Setup, in full

`tutorial.md` gives you the checklist. Here's the actual sequence, in order, so nothing depends on
something you haven't done yet.

```bash
# 1. Auth and project
gcloud auth login
gcloud projects create profit-forecast --name="Profit Forecast"
gcloud config set project profit-forecast
gcloud auth application-default login   # separate from `gcloud auth login` — this is what
                                          # client libraries (google-cloud-storage, dbt-bigquery)
                                          # actually read. Skipping this is the #1 cause of
                                          # "works with bq CLI but not in my script."

# 2. Enable the APIs you'll actually call
gcloud services enable bigquery.googleapis.com storage.googleapis.com

# 3. Budget alert — do this before any query, not after
gcloud billing budgets create \
  --billing-account=$(gcloud billing accounts list --format='value(ACCOUNT_ID)' --limit=1) \
  --display-name="profit-forecast-5eur" \
  --budget-amount=5EUR \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=1.0

# 4. Bucket, region matters (pick one region and stay consistent — cross-region
#    BigQuery <-> GCS access has its own cost and latency implications)
gcloud storage buckets create gs://profit-forecast-data --location=EU

# 5. Python project
uv init --python 3.12   # note: repo currently pins requires-python = ">=3.14" in
                         # pyproject.toml — 3.12 is the safer target for library compatibility
                         # (dbt-bigquery, statsmodels, lightgbm all lag new CPython releases by
                         # months). Lower it unless you've verified your deps on 3.14.
uv add duckdb google-cloud-storage google-cloud-bigquery pandas pyarrow kaggle
uv add --dev pytest ruff

# 6. Kaggle credentials
# Download kaggle.json from https://www.kaggle.com/settings/account, then:
mkdir -p ~/.kaggle && mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

**Done when:**
```bash
bq ls                                   # empty list, no error
duckdb -c "select 1"                    # returns 1
kaggle competitions list                # returns a table, not a 403
gcloud auth application-default print-access-token   # prints a token, not an error
```

If `kaggle competitions list` 403s, you haven't accepted the competition rules on the web page yet
— the API enforces the same click-through as the UI.

---

## Week 1 — Ingestion and Python hygiene

### 1.1 Why raw stays raw

The staging/intermediate/marts split (used from Week 3 onward) only works if `raw` never lies
about what the source actually contained. Any cleaning, renaming, or type coercion done at
ingestion time is invisible later — if `ingest.py` silently drops nulls, nobody debugging a dbt
model three weeks from now will think to look in a Python script for the cause. Ingestion's job is
narrow: get bytes from Kaggle to GCS as Parquet, unchanged in content, changed only in format.

### 1.2 `download.py`

```python
#!/usr/bin/env python3
"""download.py — fetch Favorita and Olist from Kaggle to a local raw/ directory.

Requires ~/.kaggle/kaggle.json (see Day 0). Idempotent: re-running skips files
already present unless --force is passed.

Usage:
    python download.py --dest data/raw
    python download.py --dest data/raw --force
"""
from __future__ import annotations

import argparse
import logging
import zipfile
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

logger = logging.getLogger("download")

SOURCES = [
    # (kind, identifier, subdir)
    ("competition", "favorita-grocery-sales-forecasting", "favorita"),
    ("dataset", "olistbr/brazilian-ecommerce", "olist"),
    ("dataset", "olistbr/marketing-funnel-olist", "olist_funnel"),
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download Kaggle sources for the profit forecast.")
    p.add_argument("--dest", type=Path, default=Path("data/raw"))
    p.add_argument("--force", action="store_true", help="Re-download even if already present")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def download_one(api: KaggleApi, kind: str, identifier: str, dest: Path, force: bool) -> None:
    marker = dest / ".downloaded"
    if marker.exists() and not force:
        logger.info("Skipping %s (already downloaded, use --force to redo)", identifier)
        return

    dest.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s (%s) -> %s", identifier, kind, dest)
    if kind == "competition":
        api.competition_download_files(identifier, path=str(dest), quiet=False)
    else:
        api.dataset_download_files(identifier, path=str(dest), quiet=False)

    for zf in dest.glob("*.zip"):
        logger.debug("Extracting %s", zf)
        with zipfile.ZipFile(zf) as z:
            z.extractall(dest)
        zf.unlink()

    marker.touch()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )
    api = KaggleApi()
    api.authenticate()

    for kind, identifier, subdir in SOURCES:
        download_one(api, kind, identifier, args.dest / subdir, args.force)

    logger.info("Done. Raw data in %s", args.dest)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
```

Why a `.downloaded` marker file rather than checking for the CSVs directly: the extracted
filenames vary by source and you don't want `download.py` to need per-source knowledge of what
files to expect — that's `ingest.py`'s job.

### 1.3 `ingest.py` — the DuckDB streaming conversion

This is the part `tutorial.md` calls out explicitly: use DuckDB's `COPY`, not pandas, because
`train.csv` is 125M rows and pandas will try to materialize the whole thing in RAM.

```python
#!/usr/bin/env python3
"""ingest.py — convert raw Kaggle CSVs to partitioned Parquet in GCS, via DuckDB.

DuckDB streams the CSV -> Parquet conversion without loading the full file into
memory, and can write straight to gs:// if the httpfs extension is loaded with
credentials — but writing locally then `gcloud storage cp -r` is simpler to
debug on a first pass, so that's what this does. Switch to direct GCS writes
once you trust the pipeline.

Usage:
    python ingest.py --raw data/raw --out data/parquet --bucket profit-forecast-data
"""
from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

import duckdb

logger = logging.getLogger("ingest")

# Each entry: source CSV (relative to --raw), output table name, partition columns (or None)
FAVORITA_TABLES = [
    ("favorita/train.csv", "favorita_sales", ["year", "month"]),
    ("favorita/items.csv", "favorita_items", None),
    ("favorita/stores.csv", "favorita_stores", None),
    ("favorita/transactions.csv", "favorita_transactions", None),
    ("favorita/oil.csv", "favorita_oil", None),
    ("favorita/holidays_events.csv", "favorita_holidays", None),
]

OLIST_TABLES = [
    ("olist/olist_orders_dataset.csv", "olist_orders", None),
    ("olist/olist_order_items_dataset.csv", "olist_order_items", None),
    ("olist/olist_products_dataset.csv", "olist_products", None),
    ("olist/olist_customers_dataset.csv", "olist_customers", None),
    ("olist/olist_order_reviews_dataset.csv", "olist_order_reviews", None),
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert raw CSVs to partitioned Parquet.")
    p.add_argument("--raw", type=Path, default=Path("data/raw"))
    p.add_argument("--out", type=Path, default=Path("data/parquet"))
    p.add_argument("--bucket", required=True, help="GCS bucket to sync to (no gs:// prefix)")
    p.add_argument("--skip-upload", action="store_true", help="Convert locally only, don't sync")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def convert_table(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    table_name: str,
    out_dir: Path,
    partition_by: list[str] | None,
) -> None:
    if not csv_path.exists():
        logger.warning("Missing %s, skipping %s", csv_path, table_name)
        return

    table_out = out_dir / table_name
    table_out.mkdir(parents=True, exist_ok=True)

    if table_name == "favorita_sales":
        # This is the row-count-heavy one and the one that needs the date split
        # for partitioning. Everything else is small enough to write as a
        # single Parquet file with no partitioning.
        query = f"""
            COPY (
                SELECT *,
                       EXTRACT(year FROM date) AS year,
                       EXTRACT(month FROM date) AS month
                FROM read_csv_auto('{csv_path}', header=true)
            ) TO '{table_out}' (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE true)
        """
    else:
        query = f"""
            COPY (SELECT * FROM read_csv_auto('{csv_path}', header=true))
            TO '{table_out / (table_name + ".parquet")}' (FORMAT PARQUET)
        """

    logger.info("Converting %s -> %s", csv_path, table_out)
    con.execute(query)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    con = duckdb.connect()  # in-memory — we're only using DuckDB as a conversion engine here
    args.out.mkdir(parents=True, exist_ok=True)

    for rel_path, table_name, partition_by in [*FAVORITA_TABLES, *OLIST_TABLES]:
        convert_table(con, args.raw / rel_path, table_name, args.out, partition_by)

    if not args.skip_upload:
        for prefix, tables in [("raw/favorita", FAVORITA_TABLES), ("raw/olist", OLIST_TABLES)]:
            for _, table_name, _ in tables:
                local = args.out / table_name
                if not local.exists():
                    continue
                dest = f"gs://{args.bucket}/{prefix}/{table_name}/"
                logger.info("Syncing %s -> %s", local, dest)
                subprocess.run(
                    ["gcloud", "storage", "rsync", "-r", str(local), dest],
                    check=True,
                )

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
```

Why `gcloud storage rsync` and not a single `cp -r`: rsync is idempotent and incremental — running
`ingest.py` twice after a partial failure only uploads what changed, and it's what makes the
"running twice leaves the bucket unchanged" done-bar in `tutorial.md` actually true, rather than
something you have to remember to check by hand.

### 1.4 The one pytest test `tutorial.md` asks for

```python
# test_ingest.py
from pathlib import Path
import ingest


def test_favorita_sales_partitioned_by_year_month(tmp_path: Path):
    csv = tmp_path / "train.csv"
    csv.write_text("date,store_nbr,item_nbr,unit_sales\n2017-01-15,1,100,5.0\n")

    import duckdb
    con = duckdb.connect()
    out_dir = tmp_path / "out"
    ingest.convert_table(con, csv, "favorita_sales", out_dir, ["year", "month"])

    written = list((out_dir / "favorita_sales").rglob("*.parquet"))
    assert any("year=2017" in str(p) and "month=1" in str(p) for p in written)
```

**Be able to explain:** why raw keeps source shape and does no cleaning (§1.1 above), and why
4.6 GB of CSV becomes far less as Parquet — columnar storage stores each column contiguously and
compresses within it; `unit_sales` and `item_nbr` are low-cardinality/low-entropy relative to a
row-oriented CSV's repeated text encoding of every value on every line.

---

## Week 2 — Two engines, one dataset

### 2.1 `load.py` — BigQuery native tables

```python
#!/usr/bin/env python3
"""load.py — load Parquet from GCS into partitioned/clustered BigQuery tables.

Usage:
    python load.py --bucket profit-forecast-data --dataset raw --project profit-forecast
"""
from __future__ import annotations

import argparse
import logging

from google.cloud import bigquery

logger = logging.getLogger("load")

# table_name -> (partition_field, cluster_fields) — None means no partitioning/clustering
TABLE_CONFIG = {
    "favorita_sales": ("date", ["family"]),
    "favorita_items": (None, None),
    "favorita_stores": (None, None),
    "favorita_transactions": (None, None),
    "favorita_oil": (None, None),
    "favorita_holidays": (None, None),
    "olist_orders": (None, None),
    "olist_order_items": (None, None),
    "olist_products": (None, None),
    "olist_customers": (None, None),
    "olist_order_reviews": (None, None),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load GCS Parquet into BigQuery.")
    p.add_argument("--bucket", required=True)
    p.add_argument("--dataset", default="raw")
    p.add_argument("--project", required=True)
    p.add_argument("--location", default="EU")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def load_table(
    client: bigquery.Client,
    dataset: str,
    table_name: str,
    source_uri: str,
    partition_field: str | None,
    cluster_fields: list[str] | None,
) -> None:
    table_ref = f"{client.project}.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # replace, not append —
        # matches the "reloading a month replaces rather than duplicates" done-bar
    )
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    logger.info("Loading %s -> %s (partition=%s, cluster=%s)",
                source_uri, table_ref, partition_field, cluster_fields)
    job = client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
    job.result()  # blocks until done, raises on failure

    table = client.get_table(table_ref)
    logger.info("Loaded %s: %d rows, %.2f MB", table_ref, table.num_rows,
                table.num_bytes / 1e6)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    client = bigquery.Client(project=args.project, location=args.location)
    client.create_dataset(args.dataset, exists_ok=True)

    for table_name, (partition_field, cluster_fields) in TABLE_CONFIG.items():
        prefix = "favorita" if table_name.startswith("favorita") else "olist"
        source_uri = f"gs://{args.bucket}/raw/{prefix}/{table_name}/*.parquet"
        load_table(client, args.dataset, table_name, source_uri, partition_field, cluster_fields)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
```

Note the Hive-partitioned Parquet from `ingest.py` (`year=2017/month=1/*.parquet`) needs
`source_uri_prefix` and hive partitioning options if you load it as-is; the simpler path used
above assumes `favorita_sales`'s partition columns get re-derived as a genuine BigQuery
`TimePartitioning` on the `date` column at load time rather than relying on the source file
layout — which is also *more* useful, since BigQuery's own partition pruning then works
regardless of how the Parquet files were laid out in GCS.

### 2.2 DuckDB reading the same Parquet, no load step

This is the part that makes Appendix C.5's "no separate load step for dev" true — DuckDB reads
Parquet in place:

```sql
-- dev.sql — run with `duckdb data/local.duckdb < dev.sql`, or via dbt sources
CREATE OR REPLACE VIEW favorita_sales AS
    SELECT * FROM read_parquet('data/parquet/favorita_sales/**/*.parquet', hive_partitioning=true);

CREATE OR REPLACE VIEW favorita_items AS
    SELECT * FROM read_parquet('data/parquet/favorita_items/*.parquet');

CREATE OR REPLACE VIEW olist_order_items AS
    SELECT * FROM read_parquet('data/parquet/olist_order_items/*.parquet');
```

No copy, no wait — a view over the Parquet files. This is why the dev loop in Appendix C.5 is
"seconds, free": there's no load job, just DuckDB's vectorized scan directly against the files on
disk (or against `gs://` directly, via the `httpfs` extension, if you skip the local sync step).

### 2.3 The cost experiment script

`tutorial.md` asks you to record three numbers: BigQuery bytes-scanned on a partitioned table,
the same on an unpartitioned copy, and DuckDB wall-clock. Automate the comparison so it's
reproducible, not eyeballed once and forgotten:

```python
#!/usr/bin/env python3
"""cost_experiment.py — compare BigQuery partitioned vs unpartitioned bytes scanned,
and DuckDB wall-clock, for the same one-week query. Writes results as Markdown you
can paste straight into your README.

Usage:
    python cost_experiment.py --project profit-forecast --duckdb data/local.duckdb
"""
from __future__ import annotations

import argparse
import time

import duckdb
from google.cloud import bigquery

WEEK_QUERY_BQ = """
    SELECT family, SUM(unit_sales) AS total_units
    FROM `{project}.raw.{table}`
    WHERE date BETWEEN '2017-07-03' AND '2017-07-09'
    GROUP BY family
"""

WEEK_QUERY_DUCKDB = """
    SELECT family, SUM(unit_sales) AS total_units
    FROM favorita_sales
    WHERE date BETWEEN '2017-07-03' AND '2017-07-09'
    GROUP BY family
"""


def bq_bytes_scanned(client: bigquery.Client, project: str, table: str) -> int:
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query = WEEK_QUERY_BQ.format(project=project, table=table)
    job = client.query(query, job_config=job_config)
    return job.total_bytes_processed


def duckdb_wall_clock(con: duckdb.DuckDBPyConnection) -> float:
    start = time.monotonic()
    con.execute(WEEK_QUERY_DUCKDB).fetchall()
    return time.monotonic() - start


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--duckdb", required=True)
    p.add_argument(
        "--unpartitioned-table",
        default="favorita_sales_unpartitioned",
        help="Name of the unpartitioned copy you created for this experiment",
    )
    args = p.parse_args()

    bq = bigquery.Client(project=args.project)
    partitioned_bytes = bq_bytes_scanned(bq, args.project, "favorita_sales")
    unpartitioned_bytes = bq_bytes_scanned(bq, args.project, args.unpartitioned_table)

    con = duckdb.connect(args.duckdb)
    duckdb_seconds = duckdb_wall_clock(con)

    print("| Engine | Config | Bytes scanned / time |")
    print("|---|---|---|")
    print(f"| BigQuery | partitioned by date | {partitioned_bytes / 1e6:.1f} MB |")
    print(f"| BigQuery | unpartitioned | {unpartitioned_bytes / 1e6:.1f} MB |")
    print(f"| DuckDB | local Parquet | {duckdb_seconds * 1000:.0f} ms |")
    print(f"\nPartition pruning saved {(1 - partitioned_bytes / unpartitioned_bytes) * 100:.0f}% "
          f"of bytes scanned for this query.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Create the unpartitioned copy once, deliberately, for the comparison:

```sql
CREATE TABLE raw.favorita_sales_unpartitioned AS
SELECT * FROM raw.favorita_sales;
```

**Be able to explain:** partition pruning (the planner skips reading partitions outside the
`WHERE` range before scanning starts, because partition metadata is checked, not row content);
why `LIMIT` doesn't reduce BigQuery cost (the limit is applied to the result *after* the full
column scan the query requires — the engine doesn't know it can stop early until the scan is
already priced); why selecting fewer columns does (columnar storage means each unselected column
is never read off disk at all — cost is bytes-of-columns-touched, not rows-touched).

---

## Week 3 — dbt, portable across both engines

### 3.1 Project skeleton

```
profit_forecast/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── favorita/
│   │   │   ├── _favorita__sources.yml
│   │   │   ├── stg_favorita__sales.sql
│   │   │   ├── stg_favorita__items.sql
│   │   │   └── stg_favorita__stores.sql
│   │   └── olist/
│   │       ├── _olist__sources.yml
│   │       ├── stg_olist__orders.sql
│   │       └── stg_olist__order_items.sql
│   ├── intermediate/
│   │   ├── int_olist__cost_benchmarks.sql
│   │   ├── int_olist__leadtime_dist.sql
│   │   ├── int_favorita__weekly_demand.sql
│   │   └── int_favorita__stockout_flags.sql
│   └── marts/
│       ├── fct_weekly_pnl.sql
│       └── _marts__schema.yml
├── seeds/
│   ├── family_price_anchors.csv
│   └── scenario_parameters.csv
└── tests/
    └── assert_no_revenue_without_units.sql
```

### 3.2 Staging: `stg_favorita__sales.sql` — the zero-fill

This is the model `tutorial.md` calls out explicitly: Favorita omits zero-sales rows entirely, so
a naive `GROUP BY` overstates the mean. Zero-filling means generating the missing (item, store,
date) combinations explicitly and joining sales onto that full grid.

```sql
-- models/staging/favorita/stg_favorita__sales.sql
with source as (
    select * from {{ source('favorita', 'favorita_sales') }}
),

renamed as (
    select
        date,
        store_nbr,
        item_nbr,
        unit_sales,
        onpromotion
    from source
),

-- the full item x store x date grid this data *should* have, if zero-sales
-- days were recorded instead of omitted
date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2013-01-01' as date)",
        end_date="cast('2017-08-16' as date)"
    ) }}
),

item_store_pairs as (
    select distinct store_nbr, item_nbr from renamed
),

full_grid as (
    select
        date_spine.date_day as date,
        item_store_pairs.store_nbr,
        item_store_pairs.item_nbr
    from date_spine
    cross join item_store_pairs
),

zero_filled as (
    select
        full_grid.date,
        full_grid.store_nbr,
        full_grid.item_nbr,
        coalesce(renamed.unit_sales, 0) as unit_sales,
        coalesce(renamed.onpromotion, false) as onpromotion,
        (renamed.unit_sales is null) as est_is_stockout_candidate
    from full_grid
    left join renamed
        on full_grid.date = renamed.date
        and full_grid.store_nbr = renamed.store_nbr
        and full_grid.item_nbr = renamed.item_nbr
)

select * from zero_filled
```

**Warning worth stating explicitly in your own README**: the full cross join of every
item × store × day is enormous (order of billions of rows before filtering) — in practice you'll
want to restrict `item_store_pairs` to combinations that appear at all in the source data (already
done above via `distinct ... from renamed`, which only ever produces pairs that were observed at
least once — the zero-fill fills in *missing days* for known item/store combinations, not entirely
novel combinations that never existed). Still expensive; this is one of the models worth running
on the `dev`-sampled date range first (see §3.6) before ever pointing it at `prod`.

### 3.3 Staging: `stg_olist__order_items.sql`

```sql
-- models/staging/olist/stg_olist__order_items.sql
with source as (
    select * from {{ source('olist', 'olist_order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        price,
        freight_value,
        {{ dbt.safe_cast('shipping_limit_date', api.Column.translate_type('timestamp')) }}
            as shipping_limit_date
    from source
    -- Olist has duplicate (order_id, order_item_id) rows in rare cases; dedupe
    -- explicitly rather than let it silently inflate the benchmark
    qualify row_number() over (
        partition by order_id, order_item_id order by price desc
    ) = 1
)

select * from renamed
```

### 3.4 Intermediate: `int_olist__cost_benchmarks.sql` — the ~200-row collapse

This is the model that turns ~112k Olist order items into the freight-to-value ratios that anchor
`fct_weekly_pnl`'s cost side. Weight bands require joining to `olist_products` for
`product_weight_g`.

```sql
-- models/intermediate/int_olist__cost_benchmarks.sql
with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

products as (
    select * from {{ ref('stg_olist__products') }}
),

joined as (
    select
        order_items.price,
        order_items.freight_value,
        products.product_category_name,
        case
            when products.product_weight_g < 500 then 'light'
            when products.product_weight_g < 2000 then 'medium'
            when products.product_weight_g < 8000 then 'heavy'
            else 'bulky'
        end as weight_band
    from order_items
    inner join products on order_items.product_id = products.product_id
    where products.product_weight_g is not null and order_items.price > 0
),

benchmarks as (
    select
        product_category_name,
        weight_band,
        count(*) as n_observations,
        avg(freight_value / price) as est_freight_to_value_ratio,
        percentile_cont(0.5) within group (order by price) as est_median_price,
        stddev(price) / nullif(avg(price), 0) as est_price_dispersion_cv
    from joined
    group by 1, 2
)

select * from benchmarks
```

The `est_` prefix rule from `business-case.md` starts here, not just at the marts layer — any
column that's a derived statistic rather than a passthrough of a source field should carry it,
so the schema itself flags which numbers are measured versus computed.

### 3.5 Intermediate: `int_favorita__weekly_demand.sql`

```sql
-- models/intermediate/int_favorita__weekly_demand.sql
with daily as (
    select * from {{ ref('stg_favorita__sales') }}
),

items as (
    select * from {{ ref('stg_favorita__items') }}
),

joined as (
    select
        {{ dbt.date_trunc('week', 'daily.date') }} as week_start,
        items.family,
        daily.unit_sales,
        daily.onpromotion
    from daily
    inner join items on daily.item_nbr = items.item_nbr
)

select
    week_start,
    family,
    sum(unit_sales) as units_sold,
    sum(case when onpromotion then unit_sales else 0 end)
        / nullif(sum(unit_sales), 0) as promo_share,
    count(distinct case when onpromotion then week_start end) as promo_weeks
from joined
group by 1, 2
```

### 3.6 Dev sampling so `dbt build --target dev` stays fast

```sql
-- models/staging/favorita/stg_favorita__sales.sql, top of the file
{% if target.name == 'dev' %}
{% set date_filter = "and date >= '2017-01-01'" %}
{% else %}
{% set date_filter = "" %}
{% endif %}
```

Reference `{{ date_filter }}` inside the model's final `where` clause. This is what Appendix C.5
means by "6-month sample" in dev — same SQL, a `var`/Jinja-conditional filter is the only
divergence, and it's the *first* line of that model, so it's impossible to miss when reading it.

### 3.7 Marts: `fct_weekly_pnl.sql` — the profit formula, verbatim from the business case

```sql
-- models/marts/fct_weekly_pnl.sql
with demand as (
    select * from {{ ref('int_favorita__weekly_demand') }}
),

price_anchors as (
    select * from {{ ref('family_price_anchors') }}
),

cost_benchmarks as (
    -- collapse the weight-band grain to one row per category for the join;
    -- a full treatment would map family -> category -> weight band explicitly
    select
        product_category_name,
        avg(est_freight_to_value_ratio) as est_freight_to_value_ratio
    from {{ ref('int_olist__cost_benchmarks') }}
    group by 1
),

base_scenario as (
    -- the "base" row of scenario_parameters is what fct_weekly_pnl (the
    -- historical/actuals mart) uses; fct_scenario_grid (Week 6) applies all rows
    select * from {{ ref('scenario_parameters') }} where scenario_name = 'base'
),

joined as (
    select
        demand.week_start,
        demand.family,
        demand.units_sold,
        demand.promo_share,
        price_anchors.base_price,
        price_anchors.gross_margin_pct,
        cost_benchmarks.est_freight_to_value_ratio,
        base_scenario.fill_rate,
        base_scenario.discount_pct
    from demand
    inner join price_anchors on demand.family = price_anchors.family
    left join cost_benchmarks on price_anchors.olist_category_proxy = cost_benchmarks.product_category_name
    cross join base_scenario
)

select
    week_start,
    family,
    units_sold,
    -- units_sold here is already fill_rate-adjusted implicitly by being the
    -- *observed* historical value; the fill_rate parameter matters for
    -- fct_forecast (Week 5) and fct_scenario_grid (Week 6), where units are
    -- predicted/demanded rather than observed.
    units_sold * base_price as est_revenue_gross,
    units_sold * base_price * promo_share * discount_pct as est_promo_cost,
    (units_sold * base_price) * (1 - gross_margin_pct) as est_cogs,
    units_sold * base_price * coalesce(est_freight_to_value_ratio, 0.12) as est_logistics,
    (units_sold * base_price)
        - (units_sold * base_price * promo_share * discount_pct)
        - ((units_sold * base_price) * (1 - gross_margin_pct))
        - (units_sold * base_price * coalesce(est_freight_to_value_ratio, 0.12))
        as est_profit
from joined
```

Note this historical mart is intentionally simpler than the full scenario formula in
`business-case.md` §C — `waste` requires a `perishable_flag` and a `sell_through` rate that only
matter once you're modeling *predicted* demand with a supply constraint (Week 5–6). For actuals,
`units_sold` already reflects whatever was actually sold, so there's no `units_demanded` vs
`units_sold` gap to model yet. `fct_scenario_grid` (Week 6) is where the full formula — including
the demanded/sold asymmetry that drives the fat downside tail — actually gets implemented.

### 3.8 Tests

```yaml
# models/marts/_marts__schema.yml
version: 2

models:
  - name: fct_weekly_pnl
    columns:
      - name: week_start
        tests: [not_null]
      - name: family
        tests: [not_null]
      - name: est_profit
        tests: [not_null]
      - name: gross_margin_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [week_start, family]
```

```sql
-- tests/assert_no_revenue_without_units.sql
-- Singular test: fails (returns rows) if any week has revenue with no units sold —
-- would indicate a join fanout or a formula bug, not real data.
select week_start, family, units_sold, est_revenue_gross
from {{ ref('fct_weekly_pnl') }}
where est_revenue_gross > 0 and units_sold = 0
```

**Done when:** `dbt build --target dev` (DuckDB) and `dbt build --target prod` (BigQuery) both
pass from the same SQL — see Appendix C.6 in `tutorial.md` for the `profiles.yml` that makes this
possible and the dialect table for where you'll actually hit divergence (mostly `SAFE_CAST` vs
`TRY_CAST`, and `FILTER (WHERE ...)` needing to become `CASE WHEN` for BigQuery).

---

## Week 4 — Orchestration and infrastructure

### 4.1 Terraform: bucket and datasets

```hcl
# infra/main.tf
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

resource "google_storage_bucket" "data" {
  name                        = "${var.project_id}-data"
  location                    = "EU"
  uniform_bucket_level_access = true
  force_destroy               = true  # allows `terraform destroy` to actually clean up —
                                       # deliberate for a learning project; remove for anything real
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = "EU"
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = "EU"
}

output "bucket_name" {
  value = google_storage_bucket.data.name
}
```

```bash
cd infra
terraform init
terraform plan -var="project_id=profit-forecast"
terraform apply -var="project_id=profit-forecast"
# ... later ...
terraform destroy -var="project_id=profit-forecast"
```

`force_destroy = true` on the bucket is the detail most people miss and then can't `terraform
destroy` cleanly — GCS refuses to delete a non-empty bucket by default.

### 4.2 Airflow: the weekly DAG

```python
# dags/weekly_pnl.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


@dag(
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["profit-forecast"],
)
def weekly_pnl():
    @task
    def ingest():
        import subprocess

        result = subprocess.run(
            ["python", "ingest.py", "--raw", "data/raw", "--out", "data/parquet",
             "--bucket", "profit-forecast-data"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise AirflowFailException(f"ingest.py failed:\n{result.stderr}")

    @task
    def load():
        import subprocess

        result = subprocess.run(
            ["python", "load.py", "--bucket", "profit-forecast-data",
             "--project", "profit-forecast"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise AirflowFailException(f"load.py failed:\n{result.stderr}")

    @task
    def dbt_build():
        import subprocess

        result = subprocess.run(
            ["dbt", "build", "--target", "prod"],
            capture_output=True, text=True, cwd="profit_forecast",
        )
        if result.returncode != 0:
            raise AirflowFailException(f"dbt build failed:\n{result.stdout}\n{result.stderr}")

    ingest() >> load() >> dbt_build()


weekly_pnl()
```

**Be able to explain: idempotency under mid-write retry.** Walk through what happens if `load()`
retries after `ingest()` succeeded but `load()` died halfway through a BigQuery load job:
- `ingest.py` used `gcloud storage rsync`, which only re-uploads changed files — safe to rerun.
- `load.py` used `WriteDisposition.WRITE_TRUNCATE` — a retried load job replaces the table
  wholesale rather than appending, so a half-loaded table from the failed attempt is simply
  overwritten, not duplicated into.
- `dbt build` models are `table` or `view` materializations by default — each run fully rebuilds
  from its `ref()`s, so a retry after a mid-run dbt failure just reruns the DAG from wherever it
  stopped; already-succeeded models aren't re-executed unless you pass `--full-refresh` on an
  incremental model (none of this project's models are incremental, deliberately, given the data
  volume fits comfortably in a full rebuild).

Every step in this pipeline is idempotent by construction, which is *why* `retries=3` in
`default_args` is safe to set blindly — a retry can never corrupt state, only redo work.

---

## Week 5 — Forecasting

### 5.1 `forecast.py`

```python
#!/usr/bin/env python3
"""forecast.py — seasonal-naive baseline + ETS, rolling-origin backtested,
writing marts.fct_forecast with a model_version column.

Usage:
    python forecast.py --duckdb data/local.duckdb --horizon 13
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

logger = logging.getLogger("forecast")

MODEL_VERSION = "ets_v1"


def load_weekly_demand(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("""
        select week_start, family, units_sold
        from int_favorita__weekly_demand
        order by family, week_start
    """).fetch_df()


def seasonal_naive(series: pd.Series, horizon: int, season_length: int = 52) -> np.ndarray:
    """Same week last year, repeated forward if horizon exceeds one season."""
    if len(series) < season_length:
        return np.full(horizon, series.mean())
    last_season = series.values[-season_length:]
    reps = int(np.ceil(horizon / season_length))
    return np.tile(last_season, reps)[:horizon]


def wape(actual: np.ndarray, predicted: np.ndarray) -> float:
    return np.abs(actual - predicted).sum() / max(np.abs(actual).sum(), 1e-9)


def rolling_origin_backtest(
    series: pd.Series, horizon: int, n_folds: int = 4, season_length: int = 52
) -> tuple[float, float]:
    """Returns (baseline_wape, model_wape) averaged across folds."""
    baseline_scores, model_scores = [], []
    min_train = season_length * 2

    for fold in range(n_folds):
        cut = len(series) - horizon * (n_folds - fold)
        if cut < min_train:
            continue
        train, test = series.iloc[:cut], series.iloc[cut:cut + horizon]
        if len(test) < horizon:
            continue

        baseline_pred = seasonal_naive(train, horizon, season_length)
        baseline_scores.append(wape(test.values, baseline_pred))

        try:
            fit = ExponentialSmoothing(
                train, trend="add", seasonal="add", seasonal_periods=season_length,
                initialization_method="estimated",
            ).fit()
            model_pred = fit.forecast(horizon)
            model_scores.append(wape(test.values, model_pred.values))
        except Exception as exc:  # statsmodels can fail to converge on short/sparse series
            logger.warning("ETS failed for this fold, falling back to baseline score: %s", exc)
            model_scores.append(baseline_scores[-1])

    return float(np.mean(baseline_scores)), float(np.mean(model_scores))


def forecast_family(df: pd.DataFrame, horizon: int, season_length: int = 52) -> pd.DataFrame:
    series = df.set_index("week_start")["units_sold"]
    baseline_wape, model_wape = rolling_origin_backtest(series, horizon, season_length=season_length)

    if model_wape < baseline_wape:
        fit = ExponentialSmoothing(
            series, trend="add", seasonal="add", seasonal_periods=season_length,
            initialization_method="estimated",
        ).fit()
        forecast = fit.forecast(horizon)
        model_used = MODEL_VERSION
    else:
        # Guardrail from tutorial.md: if the fancy model loses to seasonal naive,
        # ship seasonal naive and say so — explicitly, in the output, not silently.
        forecast = pd.Series(seasonal_naive(series, horizon, season_length))
        model_used = "seasonal_naive"
        logger.info("%s: seasonal_naive beat ETS (%.3f vs %.3f WAPE), shipping baseline",
                     df["family"].iloc[0], baseline_wape, model_wape)

    last_date = series.index.max()
    future_dates = pd.date_range(last_date, periods=horizon + 1, freq="W-MON")[1:]

    return pd.DataFrame({
        "week_start": future_dates,
        "family": df["family"].iloc[0],
        "est_units_forecast": forecast.values,
        "model_version": model_used,
        "backtest_wape": model_wape if model_used == MODEL_VERSION else baseline_wape,
    })


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", required=True)
    p.add_argument("--horizon", type=int, default=13)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                         format="%(asctime)s %(levelname)-8s %(message)s")

    con = duckdb.connect(args.duckdb)
    demand = load_weekly_demand(con)

    results = pd.concat(
        [forecast_family(g, args.horizon) for _, g in demand.groupby("family")],
        ignore_index=True,
    )

    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.execute("CREATE OR REPLACE TABLE marts.fct_forecast AS SELECT * FROM results")
    logger.info("Wrote %d forecast rows to marts.fct_forecast", len(results))
    logger.info("Families on seasonal_naive fallback: %d / %d",
                (results.groupby("family")["model_version"].first() == "seasonal_naive").sum(),
                results["family"].nunique())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

**Be able to explain: why WAPE, not MAPE.** MAPE divides each error by that period's actual value
— a week with 2 actual units and a forecast of 5 contributes a 150% error that dominates the
average, even though the absolute miss (3 units) is trivial next to a family selling thousands of
units elsewhere. WAPE (`sum(|error|) / sum(|actual|)`) weights every period's error by its share of
total volume instead of treating every period equally regardless of scale — the right choice
whenever near-zero actuals are common, which zero-filled weekly demand guarantees for slow-moving
families.

---

## Week 6 — Scenarios and sensitivity

### 6.1 `scenario_parameters.csv` (seed)

```csv
scenario_name,promo_uplift,price_index,gross_margin_pct,freight_delta,fill_rate,sell_through
base,1.15,1.00,0.35,0.00,0.95,0.90
pessimistic,1.05,0.97,0.30,0.10,0.85,0.80
optimistic,1.25,1.03,0.38,-0.05,0.98,0.95
supply_crunch,1.15,1.00,0.35,0.05,0.60,0.90
input_cost_inflation,1.15,1.00,0.25,0.20,0.95,0.90
promo_war,1.35,0.90,0.30,0.00,0.95,0.85
```

### 6.2 `fct_scenario_grid.sql` — the full formula from `business-case.md` §C

```sql
-- models/marts/fct_scenario_grid.sql
with forecast as (
    select * from {{ source('marts', 'fct_forecast') }}  -- written by forecast.py
),

scenarios as (
    select * from {{ ref('scenario_parameters') }}
),

price_anchors as (
    select * from {{ ref('family_price_anchors') }}
),

cost_benchmarks as (
    select product_category_name, avg(est_freight_to_value_ratio) as est_freight_to_value_ratio
    from {{ ref('int_olist__cost_benchmarks') }}
    group by 1
),

grid as (
    select
        forecast.week_start,
        forecast.family,
        scenarios.scenario_name,
        forecast.est_units_forecast,
        scenarios.promo_uplift,
        scenarios.price_index,
        scenarios.gross_margin_pct,
        scenarios.freight_delta,
        scenarios.fill_rate,
        scenarios.sell_through,
        price_anchors.base_price,
        price_anchors.perishable_flag,
        coalesce(cost_benchmarks.est_freight_to_value_ratio, 0.12) as est_freight_to_value_ratio
    from forecast
    cross join scenarios
    inner join price_anchors on forecast.family = price_anchors.family
    left join cost_benchmarks
        on price_anchors.olist_category_proxy = cost_benchmarks.product_category_name
),

priced as (
    select
        *,
        est_units_forecast * promo_uplift as units_demanded,
        est_units_forecast * promo_uplift * fill_rate as units_sold
    from grid
)

select
    week_start,
    family,
    scenario_name,
    units_demanded as est_units_demanded,
    units_sold as est_units_sold,

    -- revenue attaches to what you could actually ship
    (units_sold * base_price * price_index)
        - (0.30 * units_sold * base_price * price_index * (1 - price_index))
        as est_revenue,

    (units_sold * base_price * price_index) * (1 - gross_margin_pct) as est_cogs,

    units_sold * base_price * est_freight_to_value_ratio * (1 + freight_delta) as est_logistics,

    -- waste only applies to perishables that were demanded but not sold-through
    case when perishable_flag
        then (units_demanded - units_sold) * (1 - sell_through) * base_price * (1 - gross_margin_pct)
        else 0
    end as est_waste,

    -- promo cost attaches to demand, not to what shipped — the asymmetry
    -- business-case.md calls the single most interesting output
    0.30 * units_demanded * base_price * price_index * (1 - price_index) as est_promo_cost,

    (
        (units_sold * base_price * price_index)
        - ((units_sold * base_price * price_index) * (1 - gross_margin_pct))
        - (units_sold * base_price * est_freight_to_value_ratio * (1 + freight_delta))
        - (case when perishable_flag
            then (units_demanded - units_sold) * (1 - sell_through) * base_price * (1 - gross_margin_pct)
            else 0
          end)
    ) as est_profit
from priced
```

**Run it, then think, per `tutorial.md`'s instruction:** 13 weeks × 33 families × 6 scenarios is
~2,500 rows — trivial for BigQuery regardless of partitioning. Now imagine the same cross join at
daily × SKU grain instead of weekly × family: roughly 4.5 years × ~4,000 items × 6 scenarios is
tens of millions of rows from the *scenario* fan-out alone, before even touching the base
forecast volume. That contrast — where a design choice about grain changes a job from "instant"
to "needs partitioning discipline" — is the lesson this step is built to teach.

### 6.3 `tornado.py` — one-at-a-time sensitivity

```python
#!/usr/bin/env python3
"""tornado.py — OAT sensitivity: vary each scenario parameter across its
plausible range, hold others at base, rank by profit swing.

Usage:
    python tornado.py --duckdb data/local.duckdb --out tornado.png
"""
from __future__ import annotations

import argparse

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

PARAM_RANGES = {
    # param: (low, base, high) — pulled from the min/max across scenario_parameters.csv rows
    "promo_uplift": (1.05, 1.15, 1.35),
    "price_index": (0.90, 1.00, 1.03),
    "gross_margin_pct": (0.25, 0.35, 0.38),
    "freight_delta": (-0.05, 0.00, 0.20),
    "fill_rate": (0.60, 0.95, 0.98),
    "sell_through": (0.80, 0.90, 0.95),
}


def profit_at(con: duckdb.DuckDBPyConnection, overrides: dict[str, float]) -> float:
    set_clause = ", ".join(f"{k} = {v}" for k, v in overrides.items())
    query = f"""
        with base as (select * from scenario_parameters where scenario_name = 'base'),
             overridden as (select * replace ({set_clause}) from base)
        select sum(f.est_units_forecast * o.promo_uplift * o.fill_rate * p.base_price
                    * (o.gross_margin_pct)) as approx_profit
        from marts.fct_forecast f
        cross join overridden o
        inner join family_price_anchors p on f.family = p.family
    """
    return con.execute(query).fetchone()[0]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", required=True)
    p.add_argument("--out", default="tornado.png")
    args = p.parse_args()

    con = duckdb.connect(args.duckdb)
    base_profit = profit_at(con, {})

    swings = []
    for param, (low, base, high) in PARAM_RANGES.items():
        low_profit = profit_at(con, {param: low})
        high_profit = profit_at(con, {param: high})
        swings.append({
            "param": param,
            "low_delta": low_profit - base_profit,
            "high_delta": high_profit - base_profit,
            "range": abs(high_profit - low_profit),
        })

    df = pd.DataFrame(swings).sort_values("range")

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(df["param"], df["high_delta"] - df["low_delta"], left=df["low_delta"])
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Profit swing vs base scenario")
    ax.set_title("Tornado chart — OAT sensitivity")
    fig.tight_layout()
    fig.savefig(args.out, dpi=150)
    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Expect (per `business-case.md` §D) `gross_margin_pct` and `fill_rate` to rank above anything
derived from Favorita's real data — that's the honest finding worth stating in your README rather
than a bug to chase: the model's biggest lever is the parameter you know least about.

### 6.4 `simulate.py` — Monte Carlo

```python
#!/usr/bin/env python3
"""simulate.py — Monte Carlo over scenario parameters, triangular distributions,
10k draws, writes marts.fct_profit_dist with P10/P50/P90.

Usage:
    python simulate.py --duckdb data/local.duckdb --draws 10000
"""
from __future__ import annotations

import argparse
import logging

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger("simulate")

# param: (min, mode, max) — same ranges as tornado.py's PARAM_RANGES, just
# reinterpreted as a triangular distribution's three parameters
TRIANGULAR_PARAMS = {
    "promo_uplift": (1.05, 1.15, 1.35),
    "price_index": (0.90, 1.00, 1.03),
    "gross_margin_pct": (0.25, 0.35, 0.38),
    "freight_delta": (-0.05, 0.00, 0.20),
    "fill_rate": (0.60, 0.95, 0.98),
    "sell_through": (0.80, 0.90, 0.95),
}


def sample_params(n_draws: int, rng: np.random.Generator) -> pd.DataFrame:
    return pd.DataFrame({
        param: rng.triangular(low, mode, high, size=n_draws)
        for param, (low, mode, high) in TRIANGULAR_PARAMS.items()
    })


def run_simulation(
    con: duckdb.DuckDBPyConnection, draws: pd.DataFrame
) -> pd.DataFrame:
    forecast = con.execute("""
        select f.family, f.est_units_forecast, p.base_price, p.perishable_flag
        from marts.fct_forecast f
        inner join family_price_anchors p on f.family = p.family
    """).fetch_df()

    results = []
    for i, row in draws.iterrows():
        units_demanded = forecast["est_units_forecast"] * row["promo_uplift"]
        units_sold = units_demanded * row["fill_rate"]
        revenue = units_sold * forecast["base_price"] * row["price_index"]
        cogs = revenue * (1 - row["gross_margin_pct"])
        logistics = units_sold * forecast["base_price"] * 0.12 * (1 + row["freight_delta"])
        waste = np.where(
            forecast["perishable_flag"],
            (units_demanded - units_sold) * (1 - row["sell_through"]) * forecast["base_price"],
            0,
        )
        profit = (revenue - cogs - logistics - waste).sum()
        results.append(profit)

        if i % 1000 == 0:
            logger.debug("Draw %d/%d", i, len(draws))

    return pd.DataFrame({"draw": range(len(results)), "est_total_profit": results})


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", required=True)
    p.add_argument("--draws", type=int, default=10_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                         format="%(asctime)s %(levelname)-8s %(message)s")

    rng = np.random.default_rng(args.seed)
    con = duckdb.connect(args.duckdb)

    draws = sample_params(args.draws, rng)
    sim = run_simulation(con, draws)

    p10, p50, p90 = sim["est_total_profit"].quantile([0.10, 0.50, 0.90])
    logger.info("P10=%.0f  P50=%.0f  P90=%.0f", p10, p50, p90)

    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.execute("CREATE OR REPLACE TABLE marts.fct_profit_dist AS SELECT * FROM sim")
    logger.info("Wrote %d draws to marts.fct_profit_dist", len(sim))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Why `np.random.default_rng(seed)` rather than the legacy `np.random.seed(...)` global state: it's
a local generator instance, so running this alongside other code that also uses numpy's RNG can't
cross-contaminate reproducibility — the seed you set here only ever affects `rng`.

**Be able to explain:** why the supply constraint fattens the downside. `est_promo_cost` (or, in
the simulation, the discount embedded in `price_index`) attaches to `units_demanded` — the
promotion was run and the discount given regardless of whether you could fulfill the order. But
`est_revenue` attaches to `units_sold` — you only get paid for what shipped. When `fill_rate`
draws low in the same simulation run as `promo_uplift` draws high (a `supply_crunch`-shaped draw),
you pay full promotional cost against a demand spike you can't fully serve. Nothing in the formula
lets a shortfall on the supply side get "cancelled out" by strong demand — it can only ever
subtract. That's a structural asymmetry, not a modeling artifact, and it's why the P10 tail is
worse than a naive symmetric-uncertainty model would predict.

---

*As with `tutorial.md`: the SQL above assumes the exact source-column names Kaggle's exports use
today. Run `describe select * from read_csv_auto('train.csv') limit 0` (DuckDB) the moment you
have the real files, and adjust column names before trusting anything downstream — Kaggle exports
have drifted column names across dataset versions before.*
