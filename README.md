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