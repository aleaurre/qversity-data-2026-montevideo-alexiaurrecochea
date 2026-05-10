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