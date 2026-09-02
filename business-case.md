# Business Case — Profit Forecasting Under Uncertainty

*A learning project built to look like client work.*

---

## 1. Context

A grocery retailer plans its next quarter. Two teams commit independently:

- **Commercial** commits a promotion calendar, expecting it to lift volume.
- **Supply chain** commits inventory and logistics capacity, expecting a volume profile.

Neither owns the number that matters — **profit** — and neither knows how sensitive it is to the other's assumptions. When both are optimistic, the company discounts stock it can't supply. When both are cautious, it misses the peak and still pays for the promotion.

## 2. The decision this supports

> *"How much promotional intensity and inventory should we commit for the next 13 weeks, and how wrong could we be?"*

The output is not a forecast. It is a **profit distribution plus a ranked list of which assumption to go and verify first.**

## 3. Data strategy — two datasets, one direction of flow

This is the central design decision, and it is deliberate.

| | **Corporación Favorita** | **Olist** |
|---|---|---|
| Role | Fact source | Parameter source |
| Contributes | Rows | Coefficients |
| Grain used | item × store × day → category × week | aggregated to ~200 rows of ratios |
| Country / currency | Ecuador (USD) | Brazil (BRL) |
| Period | Jan 2013 – Aug 2017 | Sept 2016 – Oct 2018 |

**The two datasets are never joined.** They are different companies, different countries, and their overlap is 11 months of coincidence. Any key connecting them would be fabricated, and a model built on a fabricated join produces numbers that cannot be defended.

Instead: Favorita supplies the demand signal — 125M rows, four and a half years of real seasonality, a genuine promotion flag, holidays, and an external shock in the form of daily oil prices in an oil-dependent economy. Olist supplies what Favorita lacks, as **ratios rather than rows**: freight cost as a share of item value by weight band, delivery lead-time distributions, and price dispersion within a category.

Ratios transfer across borders in a way absolute values do not. A freight-to-value ratio of 12% is a defensible benchmark; R$23 is not. This also disposes of the currency problem — Ecuador is dollarised, Olist is in reais, and the FX rate cancels out of every ratio.

**The third source is synthesis.** Favorita has no prices, no costs, and no stock levels. Those are constructed, anchored on Olist where possible and on published benchmarks otherwise, and every one is a declared parameter rather than a hardcoded number. This is not a compromise forced by open data — it is what happens on real engagements when the client's own tables have holes in them. The discipline being practised is *making the assumptions visible and testable*, which is precisely what the sensitivity analysis then exercises.

## 4. Scope

**In scope:** weekly grain, product-family level, 13-week horizon, deterministic scenarios plus Monte Carlo, one dashboard view.

**Out of scope:** SKU-level forecasting, causal media-mix modelling, real-time serving, price optimisation.

## 5. Success criteria

| Criterion | Target |
|---|---|
| Forecast accuracy vs seasonal-naive baseline | WAPE improvement ≥ 15% |
| Backtest coverage | P10–P90 interval contains actuals ≥ 80% of weeks |
| Reproducibility | Full rebuild from empty cloud project in < 30 min |
| Portability | Same dbt models run on DuckDB and BigQuery without edits |
| Pipeline reliability | 4 consecutive scheduled runs with no manual fix |
| Explainability | Every profit number traceable to a declared assumption |

## 6. Cost

**€0 expected.** Both datasets fit inside BigQuery's free tier: 10 GiB of storage and 1 TiB of scanned queries per month. Favorita is ~4.6 GB as raw CSV and compresses to roughly 1–2 GB in columnar storage; Olist is under 200 MB. Development runs against DuckDB locally, which costs nothing at all.

The residual risk is a careless unpartitioned scan repeated in a loop. The design prevents it, and `maximum_bytes_billed` enforces it.

## 7. Risks and limitations

Stated up front, because a model that hides these is worse than no model.

- **The cost side is synthesised.** Prices, margins and fill rates are modelled parameters, not measured facts. Every profit figure is conditional on them. Columns are prefixed `est_` so the schema itself says which numbers are real.
- **Ratio transfer is an assumption.** Olist's Brazilian marketplace freight economics are a proxy for Ecuadorian grocery logistics. Defensible as a starting range, not as truth. It sets the *centre* of a sensitivity range whose *width* is deliberately generous.
- **Correlation, not causation.** The promotion flag correlates with volume. It does not prove promotions cause volume — promoted items are selected, not randomised. Anyone reading this as an uplift model will misuse it.
- **Dataset era.** 2013–2018. Absolute levels are historical; the mechanics transfer.
- **Structural gap in Favorita.** Rows with zero unit sales are omitted rather than recorded as zero. Absence of a row means either no demand or no stock, and the data cannot distinguish them. This is exploited as a stockout proxy, which is useful and also unfalsifiable — treat it as a flag, not a measurement.

## 8. Timeline

Six weeks at 1–2h/day. Weeks 1–3 build the pipeline; weeks 4–6 build the forecast, the scenario engine and the presentation layer.

---

# Technical Appendix

## A. Architecture

```
Kaggle: favorita (~4.6 GB)      Kaggle: olist (~120 MB)
        │                               │
        ▼                               ▼
  ingest.py ────────────► Cloud Storage (Parquet, partitioned by month)
        │                               │
        ├───────────────┬───────────────┘
        ▼               ▼
  DuckDB (local)   BigQuery (cloud)      ← same dbt models, two targets
        │               │
        ▼               ▼
  staging ──► intermediate ──► marts.fct_weekly_pnl
                    │                    marts.fct_scenario_grid
                    │
                    ├── int_olist__cost_benchmarks   (~200 rows)
                    └── int_olist__leadtime_dist     (~50 rows)
        │
        ▼
  forecast.py ────► marts.fct_forecast      (13-week horizon)
  simulate.py ────► marts.fct_profit_dist   (Monte Carlo)
        │
        ▼
  Streamlit / Looker Studio

  Airflow orchestrates.  Terraform provisions.
```

## B. Data model

Grain: **product family × ISO week.**

| Layer | Model | Contents |
|---|---|---|
| staging | `stg_favorita__sales`, `__items`, `__stores`, `__oil`, `__holidays`, `__transactions` | typed, renamed, zero-filled |
| staging | `stg_olist__orders`, `__order_items`, `__products` | typed, renamed, deduplicated |
| intermediate | `int_olist__cost_benchmarks` | freight-to-value ratio and price dispersion by category × weight band |
| intermediate | `int_olist__leadtime_dist` | delivery lead time mean, p50, p90, late rate |
| intermediate | `int_favorita__weekly_demand` | units, promo share, transactions by family × week |
| intermediate | `int_favorita__stockout_flags` | zero-sales runs as availability proxy |
| marts | `fct_weekly_pnl` | historical units plus estimated revenue, cost, margin |
| marts | `fct_forecast` | predicted demand, 13 weeks |
| marts | `fct_scenario_grid` | forecast × scenario parameters (cross join) |
| marts | `fct_profit_dist` | Monte Carlo output, P10/P50/P90 |
| seeds | `scenario_parameters.csv` | the assumption set — the heart of the project |
| seeds | `family_price_anchors.csv` | synthesised base price and margin per family, with sources |

The two staging groups never meet at row level. Olist enters the marts only as joined-on coefficients.

## C. The profit model

For family *f* in week *t*:

```
units_base      = forecast(units | promo, holidays, oil, seasonality)
units_demanded  = units_base × promo_uplift^promo_share
units_sold      = units_demanded × fill_rate            ← supply constraint

est_revenue     = units_sold × price × price_index
                  − promo_share × units_sold × price × discount_pct
est_cogs        = est_revenue_gross × (1 − gross_margin_pct)
est_logistics   = units_sold × unit_freight × (1 + freight_delta)
est_waste       = perishable_flag × units_demanded × (1 − sell_through) × unit_cost
est_promo_cost  = promo_share × units_demanded × price × discount_pct

est_profit      = est_revenue − est_cogs − est_logistics − est_waste
```

`price` and `gross_margin_pct` come from the seed. `unit_freight` and `sell_through` are derived from `int_olist__cost_benchmarks` and `int_olist__leadtime_dist`. `fill_rate` is a scenario parameter informed by `int_favorita__stockout_flags`.

Note the asymmetry, which is the single most interesting output: **promotional cost attaches to `units_demanded`, revenue attaches to `units_sold`.** When supply is short you have discounted stock you cannot ship. A revenue-only forecast cannot see this, and it makes the downside tail materially fatter than the upside.

## D. Sensitivity analysis

Three levels, increasing in rigour:

1. **One-at-a-time (OAT)** — vary each parameter across its plausible range, hold others at base. Output: a tornado chart ranking drivers by profit swing. Cheap, and it answers "what do I verify first?"
2. **Scenario grid** — named combinations (base / pessimistic / optimistic / *supply crunch* / *input cost inflation* / *promo war*). Implemented in dbt as a cross join between forecast and seed, so scenarios are version-controlled data rather than code.
3. **Monte Carlo** — 10,000 draws, one distribution per parameter (triangular where you only know min/mode/max, which is most of the time). Output: full profit distribution.

Because the synthesised parameters carry more uncertainty than the measured ones, their ranges should be *wider*. Expect the tornado chart to rank `gross_margin_pct` and `fill_rate` above anything derived from real data. That result is honest and worth stating plainly: it says the model's biggest lever is the thing you know least about, which is exactly the finding that sends someone to go get better data.

**Stretch:** Sobol variance decomposition via SALib. OAT misleads when parameters interact; Sobol attributes variance including interaction terms.

## E. Forecasting approach

Deliberately boring, in this order:

1. **Seasonal naive baseline.** Same week last year. This is the bar, and reporting against it is what makes the result credible.
2. **Statistical model.** ETS or SARIMAX — appropriate for ~230 weekly observations per family.
3. **Gradient boosting**, only if 1 and 2 are exhausted: lags, rolling means, promo share, holiday and payday flags, oil price. LightGBM.

Favorita's four and a half years is a genuine advantage here — enough for four annual cycles, so seasonality is estimable rather than guessed.

Evaluation: rolling-origin backtest, WAPE headline. Never a single time split.

## F. Stack

Python 3.12 · uv · Docker · DuckDB · Google Cloud Storage · BigQuery · dbt Core (`dbt-duckdb` + `dbt-bigquery`) · Airflow 3 · Terraform · statsmodels / LightGBM · SALib · Streamlit · GitHub Actions

## G. On the GPU

The compute profile is a few GB of columnar data, ~230 rows per series, tabular models. **A GPU contributes nothing.** Two defensible uses, both optional: Monte Carlo at 10M draws via CuPy, and `XGBoost(device="cuda")` as a one-line change. Neither belongs in the critical path.
