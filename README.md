# qversity-data-2026-montevideo-alexiaurrecochea


## Bronze Layer

The Bronze layer persists the raw fintech dataset faithfully in PostgreSQL,
without any transformation beyond ingestion metadata.

### Source

The dataset is a single JSON file (~5,100 customer records) hosted on a public
S3 bucket: https://qversity-raw-public-data.s3.amazonaws.com/fintech_banking_dataset.json

### Target table

Schema: `bronze.raw_fintech_data`

| Column         | Type          | Description                                      |
|----------------|---------------|--------------------------------------------------|
| `id`           | `BIGSERIAL`   | Surrogate primary key (insertion order).         |
| `load_id`      | `UUID`        | Unique identifier per DAG run (batch tag).       |
| `data`         | `JSONB`       | Full original customer record, untouched.        |
| `source_file`  | `TEXT`        | Originating filename.                            |
| `load_timestamp` | `TIMESTAMPTZ` | UTC timestamp of insertion.                    |

Two indexes are created: one on `load_id` (used by Silver to filter the latest
batch) and one on `load_timestamp DESC` (for audit queries).

### Orchestration

The Airflow DAG `qversity_bronze_ingestion` (`dags/qversity_dag.py`) runs three
sequential tasks:

1. **`ensure_bronze_table`** — creates the table and indexes via
   `CREATE TABLE IF NOT EXISTS`. The pipeline is self-bootstrapping; the
   `scripts/init_db.sql` file is responsible only for schema creation, not
   table DDL.
2. **`download_from_s3`** — fetches the JSON file via `requests` (the bucket
   is public, no AWS credentials needed) into `/tmp/qversity/` inside the
   Airflow container.
3. **`load_to_bronze`** — parses the JSON array, generates a fresh `load_id`
   (UUID), and bulk-inserts all records using
   `psycopg2.extras.execute_values` with `page_size=500` for efficient
   batching.

The DAG has `schedule=None` (manual trigger) because the dataset is static.

### Idempotency strategy

Bronze uses **append with per-run `load_id`**. Every DAG execution generates a
new UUID and inserts all 5,100 records under that `load_id`. The table grows
monotonically, but downstream layers always read the latest batch using:

```sql
SELECT data
FROM bronze.raw_fintech_data
WHERE load_id = (
    SELECT load_id FROM bronze.raw_fintech_data
    ORDER BY load_timestamp DESC LIMIT 1
);
```

This approach was chosen over a simple truncate-and-load for two reasons:

- **Auditability** — every historical batch is preserved with its load
  timestamp, making it possible to investigate ingestion issues or replay.
- **Realism** — production pipelines rarely truncate raw layers; append-with-batch-id
  is the standard pattern.

### Validation

After running the DAG, validate ingestion with:

```bash
docker exec -i qversity_postgres psql -U qversity -d qversity_warehouse \
    < scripts/validate_bronze.sql
```

Expected results after one run: total records = 5,100; one distinct `load_id`;
sample of `customer_id`s in the format `CUST-XXXXXXX`.

### Airflow connection

The DAG uses the connection `postgres_warehouse`, registered automatically at
container boot via the `AIRFLOW_CONN_POSTGRES_WAREHOUSE` environment variable
(see `docker-compose.yml`). No manual configuration in the Airflow UI is
required.


## Deduplication Strategy

The pipeline applies deduplication at two layers, each handling the kind of
duplicate that's natural to its abstraction.

### Why deduplicate at all

Bronze is append-only: every DAG run inserts the full dataset again with a
fresh `load_id` and `load_timestamp`. This is intentional — bronze is meant
to be a faithful audit log of what arrived from the source, not a deduped
view. During development the DAG runs many times, so by the time we hit
silver, the same `customer_id` (and every PK nested inside it) appears in
multiple bronze rows.

Without dedup, the silver staging tables would carry those repeats forward,
breaking PK-uniqueness tests in dbt and inflating every aggregate downstream.

### Where dedup happens

**PySpark (silver staging tables) — dedup by array PK.**

Each of the three flatteners (`flatten_accounts.py`, `flatten_transactions.py`,
`flatten_loans.py`) deduplicates by the natural primary key of the array it
explodes:

| Script                       | Output table                  | Dedup key        |
|------------------------------|-------------------------------|------------------|
| `flatten_accounts.py`        | `silver.stg_accounts`         | `account_id`     |
| `flatten_transactions.py`    | `silver.stg_transactions`     | `transaction_id` |
| `flatten_loans.py`           | `silver.stg_loans`            | `loan_id`        |

The shared logic lives in `spark/utils.py::deduplicate_by_pk`, which applies
a window function partitioned by the PK and ordered by `load_timestamp DESC`,
keeping `row_number() == 1`. In plain English: **for each PK, keep the row
that came from the most recent bronze load.**

This is the right semantics for a staging table: silver should reflect the
latest known state of each entity, not its history. If we ever need the
history (SCD2-style), that lives in a dedicated dimensional model in
silver/gold, not in staging.

**dbt (silver dimensions) — dedup by customer_id.**

Customer-level dedup is *not* done in PySpark. The reason is the project's
tool roles: PySpark's job is array flattening, and the customer record itself
has no nested arrays to flatten — its flat fields are already flat in the
source JSON. So `dim_customers` is built directly in dbt, reading from
`bronze.raw_fintech_data` via a staging model that parses the `jsonb` and
applies the same "latest `load_timestamp` wins" rule using `qualify
row_number() over (partition by customer_id order by load_timestamp desc) = 1`.

This split keeps each tool doing what the project asks it to do, and avoids
materializing an intermediate `silver.stg_customers` table that would
duplicate work between the layers.

### Edge cases

- **Same `load_timestamp` for two versions of the same PK** — happens if the
  DAG fires twice in the same second. Spark picks one arbitrarily; since
  the rows are byte-identical when this occurs (same source file, same
  parsing), it doesn't matter which one wins. Documented but not guarded
  against.
- **Null PKs** — `row_number()` treats nulls as their own group and would
  keep one. We don't filter nulls in PySpark; if a null PK appears, it's an
  upstream data-quality bug that dbt's `not_null` test will catch and fail
  loudly on, which is the behavior we want.
- **Customers without loans** — `flatten_loans.py` uses `explode` (not
  `explode_outer`), so customers with empty `loans[]` produce zero rows.
  This is correct: `silver.stg_loans` is a fact table of loans, not a
  customer × loan matrix. Metrics like "% of customers with a loan" are
  built in gold via a `LEFT JOIN` from `dim_customers`.

### Sanity checks

Each flattener logs three numbers per run:
- `bronze records read` — how many rows came from `bronze.raw_fintech_data`
- `<entity> after explode` — how many rows after exploding the array
- `<entity> after dedup` / `duplicates dropped` — final count vs. dropped

In a healthy run with N bronze loads of the same dataset, `duplicates
dropped` should equal `(N-1) × <expected entity count>`. If it's higher, a
PK collision exists upstream that wasn't there before; if it's lower, a load
went partial.


# dbt — section to add to the main README.md

## Transformation layer (dbt)

dbt is responsible for all SQL-based transformations on top of what
PySpark and Airflow produce. Its scope is intentionally narrow: it
**does not ingest** (that's Airflow) and **does not explode arrays**
(that's Spark). dbt owns:

1. **Flattening nested objects** (`credit_info`, `digital_engagement`) and
   the **flat root fields** of the bronze JSONB.
2. **Building dimensions and facts** in the `silver` schema.
3. **Building analytics models** in the `gold` schema that map directly
   onto the 24 business questions.
4. **Testing data quality** (uniqueness, referential integrity,
   accepted values, custom rules).

### Why dbt lives inside the Airflow container

`dbt-core` and `dbt-postgres` are installed in the same Python environment
as Airflow and PySpark. Reasoning:

- The project must run from a single `docker compose up -d --build`. A
  separate dbt service would add a container with no independent purpose
  (dbt has no daemon — it's a CLI invoked on demand).
- Airflow orchestrates dbt via `BashOperator`. The bash command needs
  `dbt` on `$PATH`, which is automatic when dbt is installed in the same
  container.
- Manual debugging is one command:
  ```bash
  docker compose exec airflow-scheduler bash -lc \
    "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"
  ```

### Schema layout

| Schema  | Owner   | Contents                                          |
|---------|---------|---------------------------------------------------|
| bronze  | Airflow | `raw_fintech_data` (JSONB, one row per customer)  |
| silver  | Spark + dbt | Spark: `stg_accounts`, `stg_transactions`, `stg_loans`. dbt: `dim_customer` (+ more on day 6) |
| gold    | dbt     | `customer_summary` (+ more analytics marts later) |

A custom `generate_schema_name` macro keeps schema names unprefixed
(`bronze` / `silver` / `gold`) rather than dbt's default `<target>_<layer>`.

### Day-5 MVP scope

The day-5 commit lands the minimal end-to-end:

- `silver.dim_customer` — flat fields from bronze JSONB, casted and
  enriched with `age` + `age_bucket`. Deduplicated by `customer_id`
  keeping the most recent `load_timestamp` per business key.
- `gold.customer_summary` — one row per customer with product counts.
- Two dbt tasks chained at the end of the DAG: `dbt_run_mvp` then
  `dbt_test_mvp`, each scoped to those two models via `--select`.

This MVP answers business question **Q10 — customer count by country and city**
directly from `gold.customer_summary`. See `sql/day5_mvp_validation.sql`
for the validation queries.

### How to run dbt manually

```bash
# Inside the airflow-scheduler container
cd /opt/airflow/dbt

# Build everything (currently only the two MVP models)
dbt run --profiles-dir /opt/airflow/dbt

# Run only silver
dbt run --profiles-dir /opt/airflow/dbt --select silver

# Test everything
dbt test --profiles-dir /opt/airflow/dbt

# Inspect compiled SQL (great for debugging jsonb extractions)
dbt compile --profiles-dir /opt/airflow/dbt
# -> compiled files appear under dbt/target/compiled/
```

### Environment variables consumed by dbt

These are read by `env_var()` in `dbt/profiles.yml`:

| Variable            | Default     | Purpose                          |
|---------------------|-------------|----------------------------------|
| `POSTGRES_HOST`     | `postgres`  | DNS name in docker network       |
| `POSTGRES_PORT`     | `5432`      | Standard PG port                 |
| `POSTGRES_USER`     | (required)  | DB user with DDL on bronze/silver/gold |
| `POSTGRES_PASSWORD` | (required)  | Pulled from `.env`, never committed |
| `POSTGRES_DB`       | `qversity`  | Database name                    |
| `DBT_SCHEMA`        | `public`    | Target schema; the macro overrides per folder |