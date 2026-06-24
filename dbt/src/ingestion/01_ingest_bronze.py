# Databricks notebook source
# =============================================================================
# 01_ingest_bronze
# -----------------------------------------------------------------------------
# Reemplaza el flujo Airflow `ensure_bronze_table -> download_from_s3 ->
# load_to_bronze` (que usaba requests + psycopg2.execute_values contra
# Postgres JSONB).
#
# Acá:
#   1. Bajamos el JSON público de S3 (sin auth, igual que el bucket original)
#      a un Volume de Unity Catalog.
#   2. Lo leemos con Spark y reconstruimos UNA fila por record con su JSON
#      crudo en la columna `data` (STRING), para que el `from_json` con schema
#      explícito de los flatten siga funcionando idéntico.
#   3. Escribimos en `<catalog>.bronze.raw_fintech_data` en modo APPEND con un
#      `load_id` por corrida (idempotencia: el dedup por load_timestamp DESC en
#      los flatten queda con la última carga).
#
# Free Edition: serverless. `spark` y `dbutils` ya vienen provistos.
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("source_url", "https://qversity-raw-public-data.s3.amazonaws.com/fintech_banking_dataset.json")

CATALOG = dbutils.widgets.get("catalog")
SOURCE_URL = dbutils.widgets.get("source_url")

BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.raw_fintech_data"
VOLUME = f"{CATALOG}.{BRONZE_SCHEMA}.landing"
LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/fintech_banking_dataset.json"

# COMMAND ----------

# --- 0. Catalog + schemas + landing volume (idempotente) ---------------------
# Equivale a tu scripts/init_db.sql: crea la estructura medallion al arrancar.
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for sch in ("bronze", "silver_raw", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{sch}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")

spark.sql(f"COMMENT ON SCHEMA {CATALOG}.bronze IS 'Raw ingestion layer — JSON as string'")
spark.sql(f"COMMENT ON SCHEMA {CATALOG}.silver IS 'Cleaned and flattened — staging + normalized'")
spark.sql(f"COMMENT ON SCHEMA {CATALOG}.gold IS 'Analytics-ready models for BI'")

# COMMAND ----------

# --- 1. Descargar el JSON público al Volume -----------------------------------
# El bucket es público (sin auth), igual que en el proyecto original. Si el
# egress serverless estuviera restringido en tu workspace, subí el archivo a
# mano al Volume por la UI (Catalog -> bronze -> landing) y saltá esta celda.
import urllib.request

if SOURCE_URL:
    print(f"[ingest] descargando {SOURCE_URL} -> {LANDING_PATH}")
    urllib.request.urlretrieve(SOURCE_URL, LANDING_PATH)
    print("[ingest] descarga OK")
else:
    print("[ingest] source_url vacío — se asume que el archivo ya está en el Volume")

# COMMAND ----------

# --- 2. Leer el JSON y reconstruir el contrato de bronze ----------------------
from pyspark.sql.functions import (
    col, to_json, struct, lit, current_timestamp, expr, monotonically_increasing_id,
)

# El source es un array JSON (multiline). Cada elemento = un customer record.
raw = spark.read.option("multiline", "true").json(LANDING_PATH)

# load_id por corrida: identifica esta carga para idempotencia/dedup downstream.
load_id = spark.sql("select uuid()").first()[0]

bronze_df = (
    raw
    # `data`: el JSON crudo de cada record como STRING, igual que el jsonb
    # original. Los flatten le aplican from_json con su schema parcial.
    .select(to_json(struct("*")).alias("data"))
    .withColumn("id", monotonically_increasing_id())
    .withColumn("load_id", lit(load_id))
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("source", lit(SOURCE_URL if SOURCE_URL else LANDING_PATH))
)

# COMMAND ----------

# --- 3. Escribir Delta en modo APPEND (idempotencia por load_id) --------------
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

count = spark.table(BRONZE_TABLE).filter(col("load_id") == load_id).count()
print(f"[ingest] load_id={load_id} — {count} records escritos en {BRONZE_TABLE}")
