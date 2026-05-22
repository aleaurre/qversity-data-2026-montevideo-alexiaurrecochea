# Submission Checklist 

This document maps every item from the spec's submission checklist to an
exact command and a pass criterion. Run top to bottom; stop at the first
failure and remediate.

All commands assume:
- you are in the repo root,
- the stack is up (`docker compose up -d --build` completed),
- the Postgres container is named `qversity_postgres`,
- the Airflow scheduler container is named `qversity_airflow_scheduler`.

The PowerShell snippet `docker exec qversity_postgres bash -c '...'` (single
quotes outside, dollar-sign envs inside) is used everywhere to avoid the
documented PowerShell `$env:` expansion gotcha.

---

## Environment

### [ ] `docker compose up -d --build` runs without errors

```powershell
docker compose down -v   # start from a clean slate
docker compose up -d --build
```

**Pass criterion:** the `docker compose ps` output shows
`qversity_postgres`, `qversity_airflow_webserver`, `qversity_airflow_scheduler`
all `(healthy)` within ~90 seconds of `airflow-init` exiting cleanly.

### [ ] PostgreSQL and Airflow are healthy

```powershell
docker compose ps --format json | ConvertFrom-Json |
    Select-Object Name, State, Health
```

**Pass criterion:** every relevant service reads `running` + `healthy`.

### [ ] `env.example` exists with all required variables

```powershell
type env.example
```

**Pass criterion:** the file lists, at minimum:

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`
- `AIRFLOW_UID`, `AIRFLOW_GID`, `AIRFLOW__CORE__EXECUTOR`, `AIRFLOW__CORE__FERNET_KEY`,
  `AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`
- `S3_URL` (or equivalent)
- `SPARK_MASTER`, `JDBC_DRIVER_PATH`, `SPARK_TARGET_SCHEMA`
- `DBT_PROFILES_DIR`, `DBT_PROJECT_DIR`

---

## Bronze

### [ ] DAG downloads JSON from S3

Trigger `qversity_pipeline` from the Airflow UI (or `airflow dags trigger`)
and let `download_from_s3` finish.

```powershell
# Verify the file was fetched into the container
docker exec qversity_airflow_scheduler ls -la /tmp/qversity/
```

**Pass criterion:** `fintech_banking_dataset.json` exists, size ~49 MB.

### [ ] Raw data loaded into bronze schema as jsonb

```powershell
docker exec qversity_postgres bash -c `
  'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema=''bronze'' AND table_name=''raw_fintech_data''
    ORDER BY ordinal_position;"'
```

**Pass criterion:** the `data` column has `data_type = 'jsonb'`.

### [ ] Table contains >= 1,000 records

```powershell
docker exec qversity_postgres bash -c `
  'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
    SELECT COUNT(*) AS rows,
           COUNT(DISTINCT load_id) AS load_ids,
           MIN(load_timestamp) AS first_load,
           MAX(load_timestamp) AS last_load
    FROM bronze.raw_fintech_data;"'
```

**Pass criterion:** `rows >= 5100` (5,100 per run), at least one `load_id`.

---

## Silver — PySpark

### [ ] PySpark runs inside Airflow container

```powershell
docker exec qversity_airflow_scheduler bash -c 'which spark-submit && spark-submit --version 2>&1 | head -5'
```

**Pass criterion:** `spark-submit` is on `$PATH`, Spark 3.5.x.

### [ ] Arrays flattened into silver staging tables

```powershell
docker exec qversity_postgres bash -c `
  'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
    SELECT table_name,
           (SELECT COUNT(*) FROM information_schema.columns c
            WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name) AS n_cols
    FROM information_schema.tables t
    WHERE table_schema=''silver_raw''
    ORDER BY table_name;"'
```

**Pass criterion:** `stg_accounts`, `stg_transactions`, `stg_loans` all
present.

### [ ] Deduplication logic implemented and documented

```powershell
# Verify deduplicate_by_pk lives in spark/utils.py and is called per script
Select-String -Path spark\*.py -Pattern "deduplicate_by_pk"
```

**Pass criterion:** the function is defined in `spark/utils.py` and invoked
in `flatten_accounts.py`, `flatten_transactions.py`, `flatten_loans.py`.
Also documented in `docs/decisions.md` under "PySpark" sections.

### [ ] Staging tables contain no nested arrays

```powershell
docker exec qversity_postgres bash -c `
  'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
    SELECT table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema=''silver_raw''
      AND (data_type LIKE ''%array%'' OR data_type=''jsonb'' OR data_type=''json'')
    ORDER BY 1,2,3;"'
```

**Pass criterion:** zero rows returned (no array/jsonb types in `silver_raw`).

---

## Silver — dbt

### [ ] Silver models clean and standardize PySpark outputs

```powershell
docker exec qversity_airflow_scheduler bash -c `
  'cd /opt/airflow/dbt && dbt ls --resource-type model --select silver --profiles-dir /opt/airflow/dbt'
```

**Pass criterion:** every silver model listed (`dim_customer`, `dim_account`,
`dim_loan`, `dim_credit_info`, `dim_digital_engagement`, `dim_geography`,
`dim_date`, `fct_transactions`, `fct_loans`, `agg_customer_activity` + the
`stg_*` views that dbt builds on top of `silver_raw`).

### [ ] Dimension and fact tables created

```powershell
docker exec qversity_postgres bash -c `
  'psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
    SELECT tablename FROM pg_tables WHERE schemaname=''silver''
    ORDER BY tablename;"'
```

**Pass criterion:** all `dim_*`, `fct_*`, `agg_*` materialized in `silver`.

### [ ] dbt tests defined and passing

```powershell
docker exec qversity_airflow_scheduler bash -c `
  'cd /opt/airflow/dbt && dbt test --select silver --profiles-dir /opt/airflow/dbt'
```

**Pass criterion:** `PASS` count >> 0, `ERROR` count = 0. `WARN` count is
acceptable iff each warning is referenced in `docs/decisions.md` ("Tests
configured as `warn` for documented data quality issues" subsection).

---

## Gold — dbt

### [ ] At least 21/24 business questions answerable from Gold models

The mapping mart → question is documented in `docs/business_questions.md`
and in `README.md §7`. Cross-check by question, not by mart count.

### [ ] Aggregations and joins implemented clearly

Open each `dbt/models/gold/mart_*.sql`. Each file should have a
header comment stating the grain and the questions it answers; columns
should be grouped by purpose (keys / dimensions / measures).

### [ ] All dbt tests pass on Gold

```powershell
docker exec qversity_airflow_scheduler bash -c `
  'cd /opt/airflow/dbt && dbt test --select gold --profiles-dir /opt/airflow/dbt'
```

**Pass criterion:** zero `ERROR`. Warnings, if any, documented in
`docs/decisions.md`.

---

## Power BI

### [ ] `powerbi/dashboard.pbix` exists, connected to gold schema

```powershell
Test-Path powerbi/dashboard.pbix
```

In Power BI Desktop, open the file → Transform data → Data source settings
→ confirm the host is `localhost:5432` and the database is the project's
warehouse name.

### [ ] 4 dashboard pages implemented

Open the .pbix. Pages: **Executive Overview**, **Revenue & Transactions**,
**Risk & Credit**, **Customer & Engagement**.

### [ ] Screenshots saved in `powerbi/screenshots/`

```powershell
Get-ChildItem powerbi/screenshots/
```

**Pass criterion:** at least four .png files, one per page.

---

## Git & Docs

### [ ] All five git tags present

```powershell
git tag --list 'v*'
```

**Pass criterion:** `v0.1.0-bronze`, `v0.2.0-silver`, `v0.3.0-gold`,
`v0.4.0-powerbi`. (`v1.0.0` is added on Day 14, NOT yet at end of Day 13.)

### [ ] README.md includes all required sections

```powershell
Select-String -Path README.md -Pattern '^## \d+\. '
```

**Pass criterion:** sections 1–8 present (Overview, Author, How to run,
Architecture, Data model, PySpark logic, Insights, Assumptions).

### [ ] ERD / data model diagram included

```powershell
Test-Path docs/diagrams/erd_silver_gold.png
Test-Path docs/diagrams/erd_silver_gold.mmd
```

**Pass criterion:** both files exist; README §5 renders the PNG inline.

### [ ] Business insights documented

```powershell
Test-Path docs/business_questions.md
```

**Pass criterion:** the file exists and answers each business question
with a reference to the producing mart.

### [ ] No secrets/credentials committed

```powershell
git log --all --full-history --source -- .env
git ls-files | Select-String -Pattern '\.env$|secret|credential|\.pem$|id_rsa'
```

**Pass criterion:** both commands produce empty output. The
`scripts/day13_cleanup.ps1` automates this check.
