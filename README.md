# Qversity — migración a Databricks (Free Edition)

Migración del pipeline ELT de Qversity desde el stack original
(Docker Compose · PostgreSQL · Airflow · PySpark · dbt-postgres · Power BI)
a **Databricks Free Edition** manteniendo **dbt** (adapter `dbt-databricks`).
Conserva los 18 marts, los 434 tests y la arquitectura medallion.

## Qué cambió (mapa de arquitectura)

| Original | Databricks |
|---|---|
| PostgreSQL 15, schemas bronze/silver/gold | Catálogo `qversity` en Unity Catalog con schemas bronze / silver_raw / silver / gold |
| Bronze JSONB vía Airflow + psycopg2 | `src/ingestion/01_ingest_bronze.py`: S3 público → Volume UC → tabla Delta (`data` STRING) |
| PySpark flatten vía spark-submit + JDBC | `src/flatten/*.py`: notebooks serverless, escritura Delta directa (sin JDBC) |
| dbt-core 1.7 + dbt-postgres | dbt-core + **dbt-databricks** contra SQL Warehouse serverless |
| Airflow DAG `qversity_pipeline` | **Lakeflow Job** definido en `databricks.yml` (Asset Bundle) |
| Power BI ← conector PostgreSQL | Power BI ← conector nativo Databricks (SQL Warehouse) |
| Docker Compose | **Databricks Asset Bundle** (reproducible-as-code) |

La cadena de dependencias del DAG se preserva 1:1:
`ingest_bronze → [flatten_accounts, flatten_transactions, flatten_loans] → dbt_build (deps→seed→run→test)`.

## Reproducibilidad

El proyecto original vendía "`docker compose up` reproduce todo en cualquier
laptop". El equivalente en Databricks es el **Asset Bundle**: en vez de Docker,
cualquiera con un workspace y el Databricks CLI corre el pipeline completo con
dos comandos. La diferencia honesta: el evaluador necesita una cuenta de
Databricks (Free Edition alcanza y es gratis), ya no basta con Docker local.

## Setup (Free Edition)

1. Crear cuenta en **Databricks Free Edition** (reemplazó a Community Edition,
   retirada el 1 de enero de 2026). Es serverless y gratuita.
2. Instalar el CLI y autenticar:
   ```bash
   pip install databricks-cli
   databricks auth login --host https://<tu-workspace>.cloud.databricks.com
   ```
3. Crear un **SQL Warehouse serverless** (viene uno por defecto). Copiar su
   HTTP Path.
4. Completar los `REPLACE-ME` en `databricks.yml` (host, source_url, warehouse_id)
   y exportar las env vars de dbt:
   ```bash
   export DATABRICKS_HOST=<tu-workspace>.cloud.databricks.com
   export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
   export DATABRICKS_TOKEN=<personal-access-token>
   export DATABRICKS_CATALOG=qversity
   ```
5. Deploy + run:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle run qversity_pipeline -t dev
   ```

## Power BI

Cambiar el origen de datos de PostgreSQL al **conector nativo de Databricks**:
Get Data → Azure Databricks → Server Hostname + HTTP Path del SQL Warehouse →
auth con token → seleccionar `qversity.gold.*`. El `.pbix` (modelo, medidas,
visuales, slicers) no cambia; solo se repunta el origen.

## Estado de la migración

**Listo y funcional end-to-end (bronze → silver_raw → dbt):**
- `databricks.yml` (Asset Bundle + Job)
- `src/ingestion/01_ingest_bronze.py`
- `src/flatten/{utils,flatten_accounts,flatten_transactions,flatten_loans}.py`
- `dbt/profiles.yml`, `dbt/dbt_project.yml`
- `dbt/macros/{parse_date_multi_format,generate_schema_name}.sql`
- `dbt/models/silver/_sources.yml`, `dbt/models/silver/stg_transactions.sql`

**Pendiente (mecánico, ver `docs/CONVERSION_GUIDE.md`):**
- `stg_accounts`, `stg_loans`, `dim_customer`, `dim_date`, `agg_customer_activity`
- los 3 `int_*`
- los 18 marts de gold + sus YAML de tests
- las seeds (`fx_rates.csv`, `country_currency.csv`) se copian tal cual

La guía tiene la tabla de reemplazos de dialecto y dos ejemplos resueltos
(día-de-semana y dim_date). El resto porta aplicando esas reglas.
