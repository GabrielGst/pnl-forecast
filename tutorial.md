# Step-by-Step: Building the Profit Forecast

Six weeks at 1–2h/day, on **Corporación Favorita** (demand) + **Olist** (cost parameters) + declared synthesis.

**Realistic scheduling:** finish weeks 1–3 before you start the job. Do 4–6 after, at whatever pace the job leaves you.

---

## Ground rules

- Git from commit #1, one branch per week, merged via a PR to yourself.
- Never commit a service-account key. `gcloud auth application-default login` locally, `*.json` and `.env` in `.gitignore`.
- Never commit the data. Commit `download.py`. Both datasets have licence terms that redistribution would violate.
- **Develop against DuckDB, ship against BigQuery.** Same models, two targets. See Appendix C.
- Budget alert at €5 before your first cloud query.
- Every week ends with something that runs.

---

## Day 0 — Setup (~90 min)

| What | Link |
|---|---|
| Google Cloud account + credits | <https://cloud.google.com/free> |
| gcloud CLI | <https://cloud.google.com/sdk/docs/install> |
| Docker Desktop (give it ≥4 GB RAM) | <https://docs.docker.com/get-started/> |
| uv for Python environments | <https://docs.astral.sh/uv/getting-started/> |
| DuckDB CLI + Python | <https://duckdb.org/docs/installation/> |
| Kaggle API | <https://github.com/Kaggle/kaggle-api> |

Create project `profit-forecast`, enable BigQuery and Cloud Storage APIs, set the budget alert.

**Done when:** `bq ls` runs, `duckdb -c "select 1"` runs, and `kaggle competitions list` returns something.

---

## Week 1 — Ingestion and Python hygiene

**Learn**
- uv project layout — <https://docs.astral.sh/uv/guides/projects/>
- pytest — <https://docs.pytest.org/en/stable/getting-started.html>
- Docker concepts — <https://docs.docker.com/get-started/introduction/>
- Git branching (Pro Git ch. 3) — <https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging>
- Parquet, and why you convert immediately — <https://parquet.apache.org/docs/>

**Build**

`download.py` fetching both sources. Accept the competition rules on each Kaggle page first, or the API returns 403:

```bash
kaggle competitions download -c favorita-grocery-sales-forecasting   # ~4.6 GB, 125M rows
kaggle datasets download -d olistbr/brazilian-ecommerce             # ~120 MB, 9 tables
kaggle datasets download -d olistbr/marketing-funnel-olist          # MQLs and closed deals
```

> **Pick the right Favorita.** The *playground* competition `store-sales-time-series-forecasting` is a trimmed 3M-row subset — fine for forecasting practice, useless for the scale lesson. You want the original above.

Then `ingest.py`: CSV → Parquet → `gs://<bucket>/raw/favorita/<table>/` and `.../raw/olist/<table>/`. Partition the Favorita sales file by month on write. Use `logging`, not `print`. One pytest test on the path builder.

Do the conversion with DuckDB, not pandas — `COPY (SELECT * FROM read_csv_auto('train.csv')) TO 'out/' (FORMAT PARQUET, PARTITION_BY (year, month))` streams it without loading 125M rows into RAM.

**Done when:** running twice leaves the bucket unchanged, and it runs in a container.

**Be able to explain:** why raw keeps source shape and does no cleaning, and why 4.6 GB of CSV becomes far less as Parquet.

---

## Week 2 — Two engines, one dataset

**Learn**
- Loading Parquet from GCS — <https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet>
- Partitioned tables — <https://cloud.google.com/bigquery/docs/partitioned-tables>
- Clustered tables — <https://cloud.google.com/bigquery/docs/clustered-tables>
- Cost best practices — <https://cloud.google.com/bigquery/docs/best-practices-costs>
- External tables — <https://cloud.google.com/bigquery/docs/external-data-cloud-storage>
- DuckDB reading Parquet — <https://duckdb.org/docs/data/parquet/overview>
- DuckDB over httpfs/GCS — <https://duckdb.org/docs/extensions/httpfs/overview>

**Build**

1. `load.py` → `raw.favorita_sales` partitioned by date, clustered by `family`; `raw.olist_*` unpartitioned (too small to matter).
2. The same tables queryable locally in DuckDB straight off the Parquet.
3. **The cost experiment.** Create an unpartitioned copy of the sales table. Run the same one-week query against both and record the bytes-scanned estimate. Then run the same query in DuckDB and record wall-clock time. Write all three numbers in your README.

**Done when:** you have that three-way comparison written down, and reloading a month replaces rather than duplicates.

**Be able to explain:** partition pruning; why `LIMIT` doesn't reduce BigQuery cost; why selecting fewer columns does.

---

## Week 3 — dbt, portable across both engines

**Learn**
- Start here — <https://docs.getdbt.com/docs/get-started-dbt>
- dbt Fundamentals, free, ~5h — <https://learn.getdbt.com/catalog?category=beginner>
- dbt Core install — <https://docs.getdbt.com/docs/core/installation-overview>
- BigQuery setup — <https://docs.getdbt.com/guides/bigquery>
- dbt-duckdb adapter — <https://github.com/duckdb/dbt-duckdb>
- Project structure — <https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview>
- Cross-database macros — <https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros>
- Tests — <https://docs.getdbt.com/docs/build/data-tests> · Seeds — <https://docs.getdbt.com/docs/build/seeds>

**Build**

- Two source groups: `stg_favorita__*` and `stg_olist__*`. They never join.
- `int_olist__cost_benchmarks` — freight as a share of item value, by category and weight band. Collapses ~112k order items into ~200 rows.
- `int_olist__leadtime_dist` — lead time p50/p90 and late rate.
- `int_favorita__weekly_demand` — units, promo share, transactions by family × week. **Zero-fill the missing rows first**: Favorita omits zero-sales days entirely, so a naive `GROUP BY` silently over-states the mean.
- `int_favorita__stockout_flags` — runs of consecutive missing days as an availability proxy.
- Seeds: `family_price_anchors.csv` (base price, gross margin, source column) and `scenario_parameters.csv`.
- `fct_weekly_pnl` applying the profit model. **Prefix every synthesised column `est_`.**
- Tests: `unique`/`not_null` on keys, `relationships` to dimensions, `accepted_range` on margin, and a singular test asserting no week has `est_revenue` without `units_sold`.

**Done when:** `dbt build --target dev` (DuckDB) and `--target prod` (BigQuery) both pass from the same SQL.

**Be able to explain:** why the staging layer exists, and why Olist contributes columns rather than rows.

---

## Week 4 — Orchestration and infrastructure

**Learn**
- Airflow tutorial — <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html>
- Airflow with Docker Compose — <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>
- Terraform on GCP — <https://developer.hashicorp.com/terraform/tutorials/gcp-get-started>
- Google provider reference — <https://registry.terraform.io/providers/hashicorp/google/latest/docs>

**Build**

Weekly DAG: `ingest → load → dbt build`, with retries. Terraform for bucket and datasets, `terraform destroy` working. README with the architecture diagram.

**Done when:** you can destroy everything and rebuild in under 30 minutes from your own README.

**Be able to explain:** what happens when a task retries mid-write, and why every step is idempotent.

---

## Week 5 — Forecasting

**Learn**
- statsmodels time series — <https://www.statsmodels.org/stable/tsa.html>
- Rolling-origin backtesting — <https://otexts.com/fpp3/tscv.html> (Hyndman & Athanasopoulos, free)
- LightGBM — <https://lightgbm.readthedocs.io/en/stable/>

**Build**

1. **Seasonal naive baseline first.** Same week last year. Measure WAPE. This is the bar.
2. ETS or SARIMAX per family, 13-week horizon. Favorita gives you ~230 weekly points and four full annual cycles — enough to estimate seasonality rather than guess it.
3. Rolling-origin backtest.
4. `forecast.py` writes to `marts.fct_forecast` with a `model_version` column.

Covariates worth trying: promo share, national and regional holidays, payday effects (Ecuador pays on the 15th and month-end, and it shows), oil price.

**Done when:** you can state how much better than baseline you are, and on which families you're worse.

**Be able to explain:** why WAPE rather than MAPE when weeks contain near-zero sales.

*Guardrail:* if the fancy model loses to seasonal naive, ship seasonal naive and say so.

---

## Week 6 — Scenarios and sensitivity

**Learn**
- Jinja and variables — <https://docs.getdbt.com/docs/build/jinja-macros>
- SALib — <https://salib.readthedocs.io/en/latest/>
- Streamlit — <https://docs.streamlit.io/>

**Build**

1. Populate `scenario_parameters.csv`: `scenario_name, promo_uplift, price_index, gross_margin_pct, freight_delta, fill_rate, sell_through`. Rows for base / pessimistic / optimistic / supply_crunch / input_cost_inflation / promo_war.
2. `fct_scenario_grid`: cross join forecast × seed, apply the profit formula. Pure SQL.
3. **Run it on BigQuery, then think.** 13 weeks × 33 families × 6 scenarios is trivial. Now run the same fan-out at daily × item grain and watch what happens. That contrast is the lesson.
4. Tornado chart: OAT across each parameter's range, sorted by profit swing.
5. Monte Carlo in `simulate.py`: triangular distributions, 10k draws, P10/P50/P90 into `fct_profit_dist`.
6. Add both to the DAG. One page of output.

**Done when:** you can name the top three profit drivers by variance contribution and defend the ranking.

**Be able to explain:** why the supply constraint fattens the downside — promotional cost attaches to demand, revenue attaches to what you could actually ship.

---

# Appendix A — Datasets

## Corporación Favorita (demand backbone)

<https://www.kaggle.com/c/favorita-grocery-sales-forecasting/data>

125,497,040 training rows, Jan 2013 – Aug 2017. Ecuadorian grocery chain. Tables: `train` (date, store, item, unit_sales, onpromotion), `items` (family, class, perishable), `stores` (city, state, type, cluster), `transactions`, `oil` (daily price), `holidays_events`.

Why it's the fact source: real multi-year seasonality, a genuine promotion flag, and an external macro shock in an oil-dependent economy.

Two structural quirks that will bite you: rows with zero sales are **omitted**, not zero — so absence is ambiguous between no demand and no stock. And ~16% of `onpromotion` is null, concentrated in the early period before promotions were tracked.

Licence: competition rules, non-commercial. Don't redistribute.

## Olist (parameter source)

<https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce> · funnel: <https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist>

~100k orders, Sept 2016 – Oct 2018, nine joined tables. Used **only** to derive: `freight_value / price` by category and weight band, delivery lead-time distributions (estimated vs actual), price dispersion within category, and funnel conversion rates.

Never joined to Favorita. Contributes ~250 rows of coefficients to the whole project.

## Exogenous ranges (for defensible sensitivity bounds)

- **World Bank Pink Sheet** — monthly commodity prices: <https://www.worldbank.org/en/research/commodity-markets>
- **FRED** — producer price and freight indices: <https://fred.stlouisfed.org/>

These turn `freight_delta` and `input_cost` ranges from invented numbers into cited ones.

---

# Appendix B — The GPU evening (separate track)

Not on the critical path. Do it on a weekend.

**Stage 1 — run models on the GPU (~2h):** NVIDIA driver + CUDA <https://docs.nvidia.com/cuda/cuda-installation-guide-linux/> · PyTorch <https://pytorch.org/get-started/locally/> · Ollama <https://ollama.com/>. Success = `torch.cuda.is_available()` is True.

**Stage 2 — use it here (~1h, optional):** `XGBoost(device="cuda")` <https://xgboost.readthedocs.io/en/stable/gpu/index.html> · cuDF <https://docs.rapids.ai/api/cudf/stable/> for a 10M-draw Monte Carlo.

**Stage 3 — actually learn CUDA (months, defer):** <https://developer.nvidia.com/blog/even-easier-introduction-cuda/> · <https://docs.nvidia.com/cuda/cuda-c-programming-guide/> · Numba <https://numba.readthedocs.io/en/stable/cuda/index.html>

---

# Appendix C — Hosting, queries, and which engine runs what

The most common confusion in this project. Read it once before week 2.

## C.1 Four places data can live

| Regime | Who pays storage | Who pays compute | Applies to you? |
|---|---|---|---|
| `bigquery-public-data` | Google | You, per byte scanned | No — neither dataset is here |
| Your BigQuery native tables | You | You, per byte scanned | **Yes** — main path |
| Your GCS + BigQuery external table | You (GCS only) | You, per byte scanned | Optional — avoids double storage |
| Your local disk + DuckDB | Nobody | Your own CPU/RAM | **Yes** — dev loop |

The free public datasets you read about are regime 1: Google eats the storage bill, which is why you can query a 3 TB table for free. **Favorita and Olist are Kaggle downloads.** The moment you use them, you leave that regime and become the host. That changes the economics, though in this case not painfully.

## C.2 What it actually costs

Getting data in is free: GCS ingress isn't billed, and batch-loading from GCS into BigQuery carries no per-byte charge. You pay to *keep* and to *scan*.

- **Storage:** first 10 GiB free, then roughly $0.02/GB/month for active storage, halving after 90 days untouched.
- **Queries:** first 1 TiB scanned per month free, then about $6.25/TiB.

Rough sizing — verify against the console rather than trusting these:

| | Raw CSV | As Parquet | In BigQuery |
|---|---|---|---|
| Favorita `train` (125M rows) | ~4.6 GB | ~0.5–1 GB | ~1–2 GB |
| Olist (9 tables) | ~120 MB | ~40 MB | ~50 MB |

Both fit inside the free 10 GiB with room to spare. Columnar formats compress hard on this data because it's mostly repeated integer keys and a low-cardinality boolean.

**The honest implication:** a full scan of Favorita costs you 1–2 GB of a 1 TiB monthly allowance. You would need roughly 500 careless full scans in a month to pay anything. BigQuery will not hurt you here.

That's a problem for learning. Cost discipline is a habit built by feeling the cost, and this project won't make you feel it. So manufacture the lesson deliberately in week 2: build the unpartitioned twin, compare estimates, and run one query against `bigquery-public-data.wikipedia.pageviews_*` with and without a date filter to watch three orders of magnitude appear and disappear.

## C.3 How BigQuery bills, precisely

Billing is on **bytes of the columns your query touches**, decompressed, across the partitions it can't prune. Three consequences that trip people up:

- `LIMIT 10` costs the same as no limit. The limit applies after the scan.
- `SELECT *` on a wide table is the single most expensive thing you can write. Naming five columns instead of forty cuts the bill roughly eightfold.
- A filter on the partition column prunes. A filter on any other column doesn't — clustering makes it *faster* and *cheaper* in practice, but it's a block-skipping optimisation, not a guarantee.

Check before you run, always:

```bash
bq query --dry_run --use_legacy_sql=false 'SELECT ...'
```

The console shows the same estimate in the top-right as you type. It's free to look, and it's the habit that separates people who get a surprise invoice from people who don't.

Enforce it in code too — `maximum_bytes_billed` makes an over-budget query fail rather than charge you, and it belongs in your dbt profile:

```yaml
prod:
  type: bigquery
  maximum_bytes_billed: 5000000000   # 5 GB ceiling; the query errors instead of billing
```

## C.4 What DuckDB is doing differently

DuckDB is an in-process columnar engine — a library, not a server. It's vectorised, multi-threaded, and spills to disk when a working set exceeds RAM. It reads Parquet natively, including directly from GCS via the `httpfs` extension, so the same files back both engines.

On your 16 GB laptop, a 125M-row aggregation over Favorita takes seconds. Pandas on the same CSV would want somewhere north of 20 GB of RAM and simply die. That gap — same laptop, same file, one tool works and the other doesn't — is the most useful thing this project will teach you about data engineering, and it has nothing to do with the cloud.

What DuckDB gives you: zero cost, no network round trip, and a query that starts returning in milliseconds where BigQuery has a per-query floor of a second or two. For an inner development loop where you run `dbt build` forty times an evening, that difference dominates everything else.

What it doesn't give you: concurrent access for other people or tools, managed governance, or elastic scale-out. Those are the actual reasons a client is on BigQuery, and none of them show up in a solo project — which is worth understanding, because it means you shouldn't conclude from this project that warehouses are pointless.

## C.5 The division of labour

```
Kaggle ──► download.py ──► local disk ──► ingest.py ──► GCS (Parquet)
                                                          │
                                    ┌─────────────────────┴───────────────┐
                                    ▼                                     ▼
                          DuckDB (local file)                    BigQuery (native tables)
                          dbt --target dev                       dbt --target prod
                          6-month sample                         full 4.5 years
                          seconds, free                          the shared artifact
                          the inner loop                         scenario fan-out, Airflow runs
```

GCS is the single source of truth. The DuckDB file is disposable — rebuilt from GCS on whichever machine you're sitting at, which is exactly how you move between your desktop and your laptop without syncing gigabytes.

Sample in dev via a var, so the models are identical:

```sql
{% if target.name == 'dev' %}
  where date >= '2017-01-01'
{% endif %}
```

## C.6 Keeping one set of SQL portable

Put both targets in `profiles.yml`:

```yaml
profit_forecast:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'data/local.duckdb'
      extensions: [httpfs, parquet]
    prod:
      type: bigquery
      method: oauth
      project: profit-forecast
      dataset: analytics
      location: EU
      maximum_bytes_billed: 5000000000
```

Then keep the SQL neutral. dbt's cross-database macros exist for exactly this — `{{ dbt.date_trunc('week', 'sale_date') }}` compiles correctly on both. Reach for `{% if target.type == 'bigquery' %}` only where you genuinely can't avoid divergence.

Dialect differences you *will* hit:

| | BigQuery | DuckDB |
|---|---|---|
| Types | `INT64`, `FLOAT64`, `STRING` | `BIGINT`, `DOUBLE`, `VARCHAR` |
| Safe cast | `SAFE_CAST(x AS INT64)` | `TRY_CAST(x AS BIGINT)` |
| Week truncation | `DATE_TRUNC(d, WEEK(MONDAY))` | `date_trunc('week', d)` |
| Filtered aggregate | not supported — use `CASE WHEN` | `count(*) FILTER (WHERE ...)` |
| Identifiers | backticks, `project.dataset.table` | double quotes, `schema.table` |
| Reading Parquet | external table or load job | `read_parquet('gs://...')` inline |

Two rules keep this manageable: put all engine-specific weirdness in the staging layer so intermediate and marts stay clean, and run `dbt build --target prod` at least once a week. Portability that isn't tested isn't portability.

## C.7 The external-table alternative

If you'd rather not store the data twice, define a BigQuery external table over the Parquet already in GCS. You then pay GCS storage only, and BigQuery reads through to the files.

Worth doing once so you understand it. The trade-offs are real: no clustering, no native partition metadata (you get pruning only if the files are laid out `dt=2017-01-01/…` and you declare hive partitioning), and consistently slower scans. Native storage is the better default here — the data is small enough that the storage saving is pennies, and the query experience is meaningfully better.

---

*Tool versions and doc URLs drift. If a link 404s, go to the tool's docs root rather than trusting an old blog post. Airflow changed substantially between 2.x and 3.x — always check which version a tutorial targets.*
