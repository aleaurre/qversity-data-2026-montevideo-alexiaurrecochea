# qversity-data-2026-montevideo-alexiaurrecochea

End-to-end ELT pipeline over a fintech banking dataset (~5,100 customer
records, 89k transactions, 17k accounts, 7.6k loans). Built as an academic
project for the Qversity program (Montevideo cohort, 2026) over 14 days.

S3 → Airflow → PostgreSQL (Bronze JSONB) → PySpark (Silver staging) →
dbt (Silver dimensions/facts + Gold marts) → Power BI dashboard.

---

## Table of contents

1. [Project overview](#1-project-overview)
2. [Author](#2-author)
3. [Run the pipeline](#3-run-the-pipeline)
4. [Architecture](#4-architecture)
5. [Data model](#5-data-model)
6. [PySpark logic](#6-pyspark-logic)
7. [Insights and findings](#7-insights-and-findings)
8. [Assumptions and design decisions](#8-assumptions-and-design-decisions)
9. [Project structure](#9-project-structure)
10. [Git tags and milestones](#10-git-tags-and-milestones)

---

## 1. Project overview

**Business problem.** A regional fintech bank operates across 7 LATAM
countries with retail, premium, private banking, and SME customer
segments. The dataset captures one snapshot of customers, accounts,
transactions, loans, credit profiles, and digital engagement. The goal
of this project is to build a reproducible analytical pipeline that
answers 24 business questions covering revenue, risk, transaction
patterns, and digital adoption.

**Architecture goal.** Implement the Bronze / Silver / Gold medallion
pattern with a deliberate split of responsibilities:

- **Bronze**: raw JSON ingestion, preserved untouched as `jsonb` for
  auditability.
- **Silver**: cleaned, normalized, and structured into dimensions and facts.
  PySpark handles array flattening (the only step where distributed compute
  semantics genuinely apply); dbt handles flat fields, nested objects, and
  semantic normalization.
- **Gold**: 16 analytics marts mapped to the 24 business questions, each
  with a specific grain (customer, segment, loan, channel, etc.) and tested
  against numeric and categorical invariants.

**Architecture diagram.**

```
                ┌──────────────────────────────────────────┐
                │   S3 (fintech_banking_dataset.json)      │
                └────────────────────┬─────────────────────┘
                                     │
                                     ▼
                ┌──────────────────────────────────────────┐
                │   Apache Airflow (DAG: qversity_pipeline)│
                └────────────────────┬─────────────────────┘
                                     │
                                     ▼
┌──────────────────────────────────────────────────────────────────────┐
│  PostgreSQL (single warehouse: qversity_warehouse)                   │
│                                                                      │
│  ┌─────────────────┐                                                 │
│  │   bronze schema │   raw_fintech_data (JSONB append-only)          │
│  └────────┬────────┘                                                 │
│           │                                                          │
│           ├──► PySpark inside Airflow container                      │
│           │    flatten_accounts / flatten_transactions / flatten_loans│
│           │              │                                           │
│           │              ▼                                           │
│           │    ┌──────────────────┐                                  │
│           │    │ silver_raw       │  stg_accounts, stg_transactions, │
│           │    │   schema         │  stg_loans (Spark outputs)       │
│           │    └────────┬─────────┘                                  │
│           │             │                                            │
│           ▼             ▼                                            │
│  ┌──────────────────────────────────┐                                │
│  │   silver schema (dbt-built)      │                                │
│  │   dim_*, fct_*, agg_*, int_*     │                                │
│  └──────────────┬───────────────────┘                                │
│                 │                                                    │
│                 ▼                                                    │
│  ┌──────────────────────────────────┐                                │
│  │   gold schema (16 marts)         │                                │
│  │   mart_*                         │                                │
│  └──────────────┬───────────────────┘                                │
└─────────────────┼────────────────────────────────────────────────────┘
                  │
                  ▼
       ┌──────────────────────────┐
       │   Power BI Desktop       │
       │   4-page dashboard       │
       └──────────────────────────┘
```

A rendered Mermaid version of the diagram (with per-mart business
questions annotated) lives at
[`docs/diagrams/pipeline_lineage.md`](docs/diagrams/pipeline_lineage.md).

---

## 2. Author

| Field | Value |
|---|---|
| Full name | Alexia Urrecochea |
| Email | alexiaurrecochea@gmail.com |
| City | Montevideo, Uruguay |
| Cohort | Qversity Data Engineering 2026 |
| Repository | `qversity-data-2026-montevideo-alexiaurrecochea` |
| Collaborators (read access) | @serasio, @lualopezpe, @luciafrances, @AgusOlivera |

---

## 3. Run the pipeline

### 3.1 Prerequisites

| Tool | Version |
|---|---|
| Docker Desktop | latest |
| Docker Compose | v2.x (bundled with Docker Desktop) |
| Power BI Desktop | latest (Windows-only, for the dashboard) |
| Python | 3.11+ (only if running scripts outside Docker) |
| Git | any recent version |

No local Java, PySpark, or dbt installation is required — everything runs
inside the Airflow container.

### 3.2 Clone and configure

```bash
git clone https://github.com/alexiaurrecochea/qversity-data-2026-montevideo-alexiaurrecochea.git
cd qversity-data-2026-montevideo-alexiaurrecochea

# Create the .env from the example, then edit POSTGRES_USER / POSTGRES_PASSWORD
# if the defaults are not preferred.
cp env.example .env
```

The `.env` file controls the credentials used by Postgres, Airflow, and dbt.
Required variables:

```bash
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=qversity_warehouse
POSTGRES_USER=qversity
POSTGRES_PASSWORD=<set-locally>
SPARK_TARGET_SCHEMA=silver_raw
```

All three services (Postgres, Airflow scheduler, Airflow webserver) read
these variables. dbt picks them up via `env_var()` calls in `profiles.yml`.

### 3.3 Start the stack

```bash
docker compose up -d --build
```

This builds the custom Airflow image (Airflow 2.10.5 + Python 3.11 +
OpenJDK 17 + PySpark 3.5.1 + dbt-core 1.7 + Postgres JDBC driver 42.7.3)
and starts three containers:

| Container | Role | Port |
|---|---|---|
| `qversity_postgres` | Data warehouse and Airflow metadata DB | 5432 |
| `qversity_airflow_webserver` | Airflow UI | 8080 |
| `qversity_airflow_scheduler` | DAG execution + dbt + PySpark | – |

Wait ~60 seconds for the stack to become healthy. The Airflow UI is then
available at <http://localhost:8080> (default credentials: `airflow` / `airflow`).

To confirm containers are running:

```bash
docker ps --filter "name=qversity_"
```

To verify Postgres is responsive:

```bash
docker exec qversity_postgres pg_isready -U qversity -d qversity_warehouse
```

### 3.4 Trigger the DAG

The pipeline runs as a single DAG (`qversity_pipeline`) with manual
trigger (no schedule). Trigger it via the Airflow UI ("DAGs" tab →
`qversity_pipeline` → ▶ trigger) or from the CLI:

```bash
docker exec qversity_airflow_scheduler airflow dags trigger qversity_pipeline
```

The DAG runs end-to-end in roughly 3–5 minutes and executes:

1. **`ensure_bronze_table`** — creates `bronze.raw_fintech_data` (idempotent).
2. **`download_from_s3`** — fetches the JSON file (no AWS credentials needed; the bucket is public).
3. **`load_to_bronze`** — bulk-inserts ~5,100 records with a unique `load_id` (UUID) per run.
4. **`flatten_accounts` / `flatten_transactions` / `flatten_loans`** — three PySpark jobs in parallel; each writes its staging table to `silver_raw`.
5. **`dbt_run`** — builds all Silver dimensions/facts, intermediate models, and Gold marts.
6. **`dbt_test`** — runs the full test suite (~370 tests).

DAG tasks can be inspected individually from the UI for logs, runtime, and
status.

### 3.5 Manual dbt commands (debugging or partial runs)

The dbt project lives at `/opt/airflow/dbt` inside the scheduler container.
Common commands:

```bash
# Run all models (silver + intermediate + gold)
docker exec qversity_airflow_scheduler bash -lc \
  "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"

# Run only Gold layer
docker exec qversity_airflow_scheduler bash -lc \
  "cd /opt/airflow/dbt && dbt run --select gold.* --profiles-dir /opt/airflow/dbt"

# Run all tests
docker exec qversity_airflow_scheduler bash -lc \
  "cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt"

# Test a specific mart
docker exec qversity_airflow_scheduler bash -lc \
  "cd /opt/airflow/dbt && dbt test --select mart_customer_360 --profiles-dir /opt/airflow/dbt"

# Generate documentation (browsable lineage + tests + column docs)
docker exec qversity_airflow_scheduler bash -lc \
  "cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt"

# Serve docs locally on port 8081
docker exec -d qversity_airflow_scheduler bash -lc \
  "cd /opt/airflow/dbt && dbt docs serve --port 8081 --no-browser --profiles-dir /opt/airflow/dbt"
```

After `dbt docs serve`, open <http://localhost:8081> to browse the model
graph and tests interactively.

### 3.6 Connect Power BI

The dashboard file lives at `powerbi/dashboard.pbix`. To open it with live
data:

1. Open `powerbi/dashboard.pbix` in Power BI Desktop.
2. Update the data source credentials:
   - Server: `localhost`
   - Port: `5432`
   - Database: `qversity_warehouse`
   - Username: as defined in `.env`
   - Password: as defined in `.env`
3. Click **Refresh** to pull from Postgres.

Static screenshots of each dashboard page are saved under
`powerbi/screenshots/` for offline review.

### 3.7 Reset everything

To start from a clean slate (drops the Postgres volume — all data lost):

```bash
docker compose down -v
docker compose up -d --build
```

After restart, the DAG must be triggered again to repopulate Bronze →
Silver → Gold.

---

## 4. Architecture

### 4.1 Role of each technology

| Tool | Version | Role |
|---|---|---|
| Docker / Docker Compose | latest | Containerized dev environment, single-command bootstrap |
| PostgreSQL | 15 | Single warehouse for all four layers (`bronze`, `silver_raw`, `silver`, `gold`) + Airflow metadata |
| Apache Airflow | 2.10.5 | Pipeline orchestration (DAG `qversity_pipeline`) |
| PySpark | 3.5.1 | Array flattening + dedup + JDBC I/O. Runs inside the Airflow scheduler container |
| dbt-core + dbt-postgres | 1.7.x | SQL transformations, tests, lineage, documentation |
| Power BI Desktop | latest | 4-page interactive dashboard on top of Gold |
| Python | 3.11 | Pipeline scripts (DAG code, PySpark jobs) |

### 4.2 Layer responsibilities

**Bronze (`bronze.raw_fintech_data`).**
Append-only `JSONB` table. Each DAG run inserts the full dataset under a
new `load_id` (UUID), preserving every historical batch for auditability.
The Bronze table is never mutated downstream — Silver always reads the
latest batch by joining on the most recent `load_id`.

**Silver raw (`silver_raw.stg_*`).**
PySpark output. Three tables: `stg_accounts`, `stg_transactions`,
`stg_loans`. Cardinality post-flatten: ~17k, ~89k, ~7.6k respectively.
Dedup applied per-array on the natural PK using a window function ordered
by `load_timestamp DESC`. Cleaning at this stage is purely **syntactic**
(trim whitespace, coerce empty strings to NULL).

**Silver (`silver.dim_* / fct_* / agg_* / int_*`).**
dbt-built. 11 modeled tables that consume `silver_raw` plus the raw
Bronze `jsonb` (for customer flat fields, `credit_info`, and
`digital_engagement`). Cleaning here is **semantic** — casing
normalization, Spanish-to-English mapping, date format parsing,
bucketing, and validation flags. Customer-level dedup happens here
(not in PySpark) since the customer record has no nested arrays to
explode.

**Intermediate (`silver.int_*`).**
Three view models that centralize business logic shared across multiple
Gold marts: `int_loan_portfolio_metrics`, `int_customer_risk_profile`,
`int_customer_monthly_revenue`. Created during refactoring (Day 10) to
eliminate duplication and provide a single source of truth for
delinquency, DPD bucketing, and customer revenue.

**Gold (`gold.mart_*`).**
16 analytics marts, each tied to one or more of the 24 business
questions. Materialized as tables for query performance from Power BI.
Each mart has its own grain (customer, segment, country × account_type,
loan_type × dpd_bucket, channel × currency, etc.) and is documented in
`docs/business_questions.md` with the SQL query that produces it.

### 4.3 Why PySpark only for arrays

The split between PySpark and dbt is deliberate:

- **PySpark handles array flattening** (`accounts[]`, `transactions[]`,
  `loans[]`) because the `explode` operation changes cardinality and
  distributed compute semantics genuinely apply when scaling beyond this
  dataset.
- **dbt handles everything else** (flat fields, nested objects,
  normalization, bucketing) because Postgres `jsonb` operators are
  performant at 5k rows and idiomatic for SQL-native modeling.

Customer-level deduplication lives in dbt — not PySpark — because the
customer record has no nested arrays to flatten. Forcing it through Spark
would mean writing a script that does not actually flatten anything,
breaking the `flatten_*` naming convention.

### 4.4 Idempotency

Every part of the pipeline is idempotent:

- **Bronze**: append-with-`load_id`. Re-running the DAG inserts a new
  batch; Silver always reads the latest by `load_timestamp DESC`.
- **Silver raw (Spark)**: dedup by PK keeps only the most recent record
  per `account_id` / `transaction_id` / `loan_id`. Two runs produce the
  same Silver output as one run.
- **Silver dbt**: customer dedup uses `qualify row_number() over (...)`
  with the same recency tie-breaker.
- **Gold**: `dbt run` rebuilds all marts from scratch each invocation.

Verified end-to-end by triggering the DAG twice and confirming counts
remain stable across the entire stack.

---

## 5. Data model

### 5.1 Entity-relationship overview

```
                          dim_customer
                          (root entity)
                                │
       ┌────────────────────────┼──────────────────────────┐
       │                        │                          │
   dim_account              dim_loan                fct_transactions
       │                        │                          ▲
       │                    fct_loans                      │
       │                        │                          │
       └────────────────────────┴──────────────────────────┘
                                │
       ┌────────────────────────┼──────────────────────────┐
       │                        │                          │
   dim_credit_info       dim_digital_engagement     agg_customer_activity
   (1:1 customer)        (1:1 customer)             (1:1 customer)
```

A full ER diagram with column lists is in
[`docs/diagrams/silver_er.md`](docs/diagrams/silver_er.md).
A textual reference table (grain, PK, FK, consumers per table) is in
[`docs/diagrams/data_model_reference.md`](docs/diagrams/data_model_reference.md).

### 5.2 Silver layer — main tables

| Table | Grain | PK | Key columns |
|---|---|---|---|
| `dim_customer` | 1 row / customer | `customer_id` | `customer_segment`, `country`, `status`, `kyc_status`, `risk_score`, `age_bucket`, `tenure_bucket` |
| `dim_account` | 1 row / account | `account_id` | `account_type`, `status`, `currency`, `balance`, `credit_limit` |
| `dim_loan` | 1 row / loan | `loan_id` | `loan_type`, `status`, `currency`, `principal`, `outstanding_balance`, `interest_rate_decimal` |
| `dim_credit_info` | 1 row / customer | `customer_id` | `credit_score` (raw + validated), `utilization_pct`, `late_payments_12m`, `bankruptcy_flag` |
| `dim_digital_engagement` | 1 row / customer | `customer_id` | `mobile_app_registered`, `web_banking_registered`, `preferred_channel`, `avg_monthly_logins` |
| `fct_transactions` | 1 row / transaction | `transaction_id` | `transaction_date`, `amount`, `currency`, `transaction_type`, `channel`, `category`, `status` |
| `fct_loans` | 1 row / loan | `loan_id` | `status`, `outstanding_balance`, `days_past_due`, `interest_rate_decimal` |
| `agg_customer_activity` | 1 row / customer | `customer_id` | `accounts_count`, `transactions_count`, `loans_count`, `total_products` |
| `dim_geography` | 1 row / country | `country` | `country_name`, `region`, `currency_local` (hardcoded, 7 LATAM countries) |
| `dim_date` | 1 row / day | `date_day` | `year`, `quarter`, `month`, `day_of_week_num`, `is_weekend` |

### 5.3 Gold layer — 16 marts

Each mart maps to one or more of the 24 business questions. The full
mart-to-question mapping with SQL queries is in
[`docs/business_questions.md`](docs/business_questions.md).

**Acquisition & demographics**
- `mart_acquisition_trend` (Q12)
- `mart_customer_360` (Q1, Q9, Q10, Q11, Q13, Q14, Q24)

**Revenue**
- `mart_revenue_by_segment_usd` (Q1, USD-converted)
- `mart_account_mix` (Q2, Q22)

**Risk & credit**
- `mart_delinquency_by_segment` (Q5)
- `mart_credit_score_by_country` (Q6)
- `mart_utilization_vs_delinquency` (Q7)
- `mart_risk_buckets` (Q9)
- `mart_loan_dpd` (Q8)
- `mart_loan_composition` (Q4, Q23)

**Transaction patterns**
- `mart_tx_by_channel` (Q3, Q17, Q18)
- `mart_tx_by_category` (Q15)
- `mart_tx_by_dow` (Q16)
- `mart_international_transfers` (Q19)

**Digital engagement**
- `mart_digital_adoption_by_segment` (Q20)
- `mart_channel_preference_by_age` (Q21)

Coverage: **24 / 24** business questions (23 fully answered, 1 with
documented divergence between dataset and spec — see §8).

---

## 6. PySpark logic

Three flatteners run in parallel under Airflow, all writing to the
`silver_raw` schema. Shared infrastructure (Spark session creation, JDBC
config, deduplication helper) lives in `spark/utils.py`.

### 6.1 `spark/flatten_accounts.py`

| Aspect | Detail |
|---|---|
| Source | `bronze.raw_fintech_data` (filtered to latest `load_id`) |
| Target | `silver_raw.stg_accounts` |
| Cardinality | ~17,000 rows (5,100 customers × ~3.3 accounts each) |
| Transformations | `explode(data → accounts)` + `trim()` on all string columns |
| Dedup key | `account_id` |
| Dedup rule | Keep most recent by `load_timestamp DESC` via `ROW_NUMBER()` window |

### 6.2 `spark/flatten_transactions.py`

| Aspect | Detail |
|---|---|
| Source | `bronze.raw_fintech_data` (filtered to latest `load_id`) |
| Target | `silver_raw.stg_transactions` |
| Cardinality | ~89,000 rows (5,100 customers × ~17.4 transactions each) |
| Transformations | `explode(data → transactions)` + `trim()` on all string columns |
| Dedup key | `transaction_id` |
| Dedup rule | Keep most recent by `load_timestamp DESC` |

### 6.3 `spark/flatten_loans.py`

| Aspect | Detail |
|---|---|
| Source | `bronze.raw_fintech_data` (filtered to latest `load_id`) |
| Target | `silver_raw.stg_loans` |
| Cardinality | ~7,600 rows (customers with loans only — 0–3 loans each per spec) |
| Transformations | `explode(data → loans)` + `trim()` on all string columns |
| Dedup key | `loan_id` |
| Dedup rule | Keep most recent by `load_timestamp DESC` |

### 6.4 Deduplication concept

Each flattener applies the same dedup pattern via `spark.utils.deduplicate_by_pk`:

```python
window = Window.partitionBy(pk_col).orderBy(F.col("load_timestamp").desc())
df_deduped = (
    df.withColumn("rn", F.row_number().over(window))
      .filter("rn = 1")
      .drop("rn")
)
```

This is the correct semantics for a staging table: Silver should reflect
the latest known state of each entity, not its history. If multiple DAG
runs have appended the same entity, only the row from the most recent run
survives.

**Why not dedup customers in PySpark?**
The customer record has no nested array to flatten — its flat fields are
already flat in the source JSON. Forcing it through Spark would mean
writing a script that does not actually `explode` anything, breaking the
`flatten_*` naming convention. Customer dedup therefore lives in dbt
(`dim_customer.sql` uses the same `row_number()` rule via SQL).

---

## 7. Insights and findings

### 7.1 Coverage of the 24 business questions

**24 / 24** answered. The complete set of queries with sample results lives
in [`docs/business_questions.md`](docs/business_questions.md). One question
(Q13 — customer status distribution) is documented as a spec-vs-dataset
divergence: the brief lists `active / inactive / suspended / closed` but
the dataset actually contains `active / inactive / suspended / closed`
at the customer level (matches the brief) and `active / frozen / closed`
at the account level (diverges). Both cases are explicitly modeled.

### 7.2 Key insights from the data

Four insights are highlighted in the Power BI dashboard and discussed in
detail in `docs/business_questions.md`:

1. **`risk_score` is anti-predictive of delinquency.**
   Customers with `risk_bucket = low` show 65.3% delinquency; `critical`
   shows 60.4%. The relationship is non-monotonic and inverted compared
   to a calibrated risk model. The synthetic data generator assigns
   `risk_score` independently from financial behavior.

2. **USD dollarization pattern.**
   ~50% of accounts and transactions are USD-denominated across all
   LATAM countries, consistent with real informal dollarization (notably
   in Uruguay and Argentina).

3. **Synthetic data exhibits flatness across financial dimensions.**
   Revenue per segment, delinquency rates, failed transaction rates, and
   digital adoption rates are nearly uniform across customer segments and
   ages. This is a property of the synthetic generator (no joint
   distributions modeled), not a pipeline bug.

4. **International transfers do show realistic structure.**
   Unlike the financial dimensions, international transfer corridors
   exhibit bimodal distribution (large domestic corridors vs. small
   cross-currency ones). Uruguay shows the highest per-country
   international transfer value, consistent with its real role as a
   regional financial hub.

### 7.3 Power BI dashboard

Four pages on top of the Gold schema:

1. **Acquisition & overview** — Q12, Q10, Q11, Q13, Q14, Q24.
2. **Revenue** — Q1 (USD + native), Q2, Q22.
3. **Risk & credit** — Q4, Q5, Q6, Q7, Q8, Q9, Q23.
4. **Transactions & digital** — Q3, Q15, Q16, Q17, Q18, Q19, Q20, Q21.

Static screenshots are saved under `powerbi/screenshots/`.

---

## 8. Assumptions and design decisions

The full decision log lives in [`docs/decisions.md`](docs/decisions.md)
with rationale and rejected alternatives. The most consequential
decisions:

### 8.1 Data quality

| Area | Decision |
|---|---|
| NULLs encoded as strings (`"null"`, `"N/A"`, `""`, `"None"`) | Normalized to real NULL via `safe_cast_*` macros before any aggregation |
| 4 coexisting date formats | Cascading parser in dbt; separator disambiguates day/month order (`/` is DD/MM, `-` is MM-DD) |
| ~3% numeric fields as dirty strings (`$1810568162.59`, `404393.03 USD`, `83,5`) | `parse_money` function in PySpark with regex + Latin decimal handling |
| Outliers up to ~$2 billion in monetary amounts | Preserved (legitimate in `private_banking`); medians used in Gold where outlier sensitivity matters |
| 10% of `credit_score` outside FICO range [300, 850] | `raw + flag + validated` pattern: original preserved, `is_credit_score_valid` boolean exposed, validated version NULL-ed for aggregations |
| Booleans with 5 representations (`true`/`false`, `yes`/`no`, `si`, `Y`/`N`, `0`/`1`) | `safe_cast_boolean` dbt macro with explicit mapping table |

### 8.2 Categoricals

| Area | Decision |
|---|---|
| Casing chaos + Spanish/English mix in every categorical | One `normalize_*` macro per field in dbt (lowercase + Spanish-to-English mapping) |
| `account.status` diverges from spec (`active/frozen/closed` instead of `active/inactive/suspended/closed`) | Divergence documented; `accepted_values` test uses the actual dataset values |
| `collateral_type = "None"` in 48% of loans | Mapped to NULL in Silver; semantically labeled `unsecured` in Gold |
| `preferred_channel` ≠ `transactions.channel` (e.g. `phone` vs `pos`) | No unified `dim_channel`; the two are kept as separate semantic spaces |

### 8.3 Business definitions (Gold marts)

| Definition | Choice |
|---|---|
| **Revenue** | `fee_income (completed fees only) + monthly interest accrued (active loans only)`. Fees are normalized to monthly average by dividing lifetime cumulative by `tenure_months` |
| **Delinquency** | `days_past_due >= 30` (operational); `is_default = days_past_due >= 90 OR status = 'default'` (regulatory, Basel/IFRS9) |
| **DPD buckets** | 6 buckets aligned with IFRS9: `00-Current`, `01-Early(1-29)`, `02-30-59`, `03-60-89`, `04-90-179`, `05-180+` |
| **Utilization buckets** | 6 buckets, including `over_limit (>100%)` for legitimate overlimit cases and `unknown` for NULLs |
| **Age buckets** | Decade-based, neutral labels (`18-24`, `25-34`, ..., `65+`); avoids contested generational names |
| **Tenure buckets** | `new (<6m) / established (6-24m) / loyal (>24m)`. Implemented in `macros/tenure_bucket.sql` |
| **Currency strategy** | Hybrid: 15 marts in native currency (avoids mixing units), 1 mart in USD (`mart_revenue_by_segment_usd`) for cross-country comparability |

### 8.4 Architecture

| Area | Decision |
|---|---|
| Spark vs. dbt split | Spark only for array flattening (cardinality change); dbt for everything else |
| Customer dedup | In dbt, not Spark — no array to explode |
| Bronze idempotency | Append-with-`load_id` (UUID per run), not truncate-reload — preserves audit history |
| Table DDL | Lives in the DAG (`CREATE TABLE IF NOT EXISTS`); `scripts/init_db.sql` only creates schemas |
| dbt placement | Same container as Airflow — single image, no extra service |
| Tests as `warn` | Used selectively for documented generator-side data quality issues (e.g. ~3% NULL balances) so the pipeline runs while issues remain visible |

---

## 9. Project structure

```
qversity-data-2026-montevideo-alexiaurrecochea/
├── README.md                          # this file
├── docker-compose.yml                 # 3 services: postgres, airflow-webserver, airflow-scheduler
├── Dockerfile.airflow                 # custom image (Airflow + Java + dbt + JDBC driver)
├── env.example                        # template for .env
├── requirements.txt                   # Python deps (PySpark, dbt-core, dbt-postgres, ...)
├── .gitignore
│
├── dags/
│   └── qversity_dag.py                # main DAG: ingest → flatten → dbt run → dbt test
│
├── spark/
│   ├── utils.py                       # shared: get_spark_session, JDBC config, deduplicate_by_pk
│   ├── flatten_accounts.py
│   ├── flatten_transactions.py
│   └── flatten_loans.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                   # reads POSTGRES_* via env_var()
│   ├── packages.yml                   # dbt-utils
│   ├── macros/
│   │   ├── normalize_*.sql            # categorical normalization (one per field)
│   │   ├── safe_cast_numeric.sql      # NULL-safe numeric cast with format cleanup
│   │   ├── safe_cast_boolean.sql      # NULL-safe boolean cast (handles yes/no/si/Y/N/0/1)
│   │   ├── parse_date_multi_format.sql
│   │   ├── age_bucket.sql
│   │   ├── tenure_bucket.sql
│   │   └── ...
│   ├── seeds/
│   │   ├── fx_rates.csv               # 9 currencies → USD (mid-market May 2026)
│   │   └── country_currency.csv       # ISO country → local currency
│   ├── models/
│   │   ├── sources.yml
│   │   ├── silver/
│   │   │   ├── dim_customer.sql
│   │   │   ├── dim_account.sql
│   │   │   ├── dim_loan.sql
│   │   │   ├── dim_credit_info.sql
│   │   │   ├── dim_digital_engagement.sql
│   │   │   ├── dim_geography.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── fct_transactions.sql
│   │   │   ├── fct_loans.sql
│   │   │   ├── agg_customer_activity.sql
│   │   │   └── _silver__*.yml         # column-level tests
│   │   ├── intermediate/
│   │   │   ├── int_loan_portfolio_metrics.sql
│   │   │   ├── int_customer_risk_profile.sql
│   │   │   └── int_customer_monthly_revenue.sql
│   │   └── gold/
│   │       ├── mart_*.sql             # 16 marts
│   │       └── _gold__mart.yml        # column-level tests + descriptions
│   └── tests/                         # custom singular tests
│
├── docs/
│   ├── decisions.md                   # full decision log with rationale (Day 1 → Day 10)
│   ├── business_questions.md          # 24 questions, SQL queries, sample results, findings
│   └── diagrams/
│       ├── silver_er.md               # Mermaid ER diagram (Silver layer)
│       ├── pipeline_lineage.md        # Mermaid flow diagram (end-to-end)
│       └── data_model_reference.md    # textual inventory of tables
│
├── notebooks/
│   └── 01_eda.ipynb                   # Day 1 exploratory data analysis (drives the decisions)
│
├── powerbi/
│   ├── dashboard.pbix
│   └── screenshots/
│
├── scripts/
│   ├── init_db.sql                    # creates bronze / silver / silver_raw / gold schemas
│   ├── validate_bronze.sql
│   ├── validate_business_questions.sql
│   └── validation_output.txt          # reproducible snapshot of question outputs
│
└── data/
    └── raw/                           # local cache of the S3 file (gitignored)
```

---

## 10. Git tags and milestones

| Tag | Milestone | Status |
|---|---|---|
| `v0.1.0-bronze` | Bronze ingestion complete | ✅ |
| `v0.2.0-silver` | Silver layer (PySpark + dbt) complete | ✅ |
| `v0.3.0-gold` | Gold layer (16 marts, 196 tests passing) | ✅ |
| `v0.4.0-powerbi` | Dashboard delivered | (in progress) |
| `v1.0.0` | Final submission | (pending) |

To check out a specific milestone:

```bash
git checkout v0.3.0-gold
```

---

## Additional documentation

- **Decision log:** [`docs/decisions.md`](docs/decisions.md) — every
  modeling and architectural decision with rationale, alternatives
  considered, and references to the code that implements it.
- **Business questions:** [`docs/business_questions.md`](docs/business_questions.md)
  — all 24 questions with SQL queries, sample results, and key findings.
- **Data model diagrams:** [`docs/diagrams/`](docs/diagrams/) — ER diagram,
  pipeline lineage, and textual reference of all tables.
- **EDA notebook:** [`notebooks/01_eda.ipynb`](notebooks/01_eda.ipynb) —
  the Day 1 exploration that surfaced the patterns driving the cleaning
  and normalization layers.

---

*Built over 14 days. Code, decisions, and findings preserved for review.*
