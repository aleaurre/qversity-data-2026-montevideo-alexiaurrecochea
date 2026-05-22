"""
Qversity ELT — Bronze ingestion + Silver flatten (PySpark) + dbt (silver/gold).

DAG that:
  1. Ensures bronze.raw_fintech_data exists.
  2. Downloads the fintech dataset from a public S3 bucket.
  3. Loads each customer record into bronze.raw_fintech_data as JSONB.
  4. Runs three PySpark jobs IN PARALLEL that flatten each nested array
     (accounts, transactions, loans) into the silver_raw schema, with
     dedup by each array's natural PK and basic syntactic cleanup.
  5. Loads the static dbt seeds (fx_rates, country_currency) into silver.
  6. Runs the full dbt project (staging → silver → intermediate → gold)
     and the full test suite.

Idempotency: every run gets a unique load_id (UUID). The Spark dedup logic
keeps only the most recent version of each (account_id | transaction_id |
loan_id), so re-running end-to-end is safe and produces no duplicates
downstream.

Customer-level dedup is NOT done in PySpark — that's a dbt silver concern,
since `dim_customer` is a pure flat structure (no array flattening needed)
and lives naturally in the dbt layer per the project's tool roles.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
S3_URL = (
    "https://qversity-raw-public-data.s3.amazonaws.com/"
    "fintech_banking_dataset.json"
)
SOURCE_FILE = "fintech_banking_dataset.json"
POSTGRES_CONN_ID = "postgres_warehouse"
TARGET_TABLE = "bronze.raw_fintech_data"
LOCAL_DIR = Path("/tmp/qversity")

# Spark job constants
SPARK_SCRIPTS_DIR = "/opt/airflow/spark"
JDBC_DRIVER = "/opt/spark/jars/postgresql-42.7.3.jar"

# dbt job constants
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Env vars que necesita cualquier spark-submit del proyecto.
# BashOperator no propaga el env del scheduler por default, así que se lo
# pasamos explícito. Mantenemos esto en un dict reutilizable para que el
# factory de tasks no duplique el bloque.
SPARK_ENV = {
    "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT":     os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB":       os.getenv("POSTGRES_DB",   "qversity_warehouse"),
    "POSTGRES_USER":     os.getenv("POSTGRES_USER", ""),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
    "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

# Env para dbt. Mismas POSTGRES_* que Spark, profiles.yml las lee con env_var().
# Idéntica lógica que SPARK_ENV: BashOperator no propaga, lo pasamos explícito.
DBT_ENV = {
    "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT":     os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB":       os.getenv("POSTGRES_DB",   "qversity_warehouse"),
    "POSTGRES_USER":     os.getenv("POSTGRES_USER", ""),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
    "DBT_PROFILES_DIR":  DBT_PROJECT_DIR,
    "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

DDL = """
CREATE TABLE IF NOT EXISTS bronze.raw_fintech_data (
    id              BIGSERIAL PRIMARY KEY,
    load_id         UUID NOT NULL,
    data            JSONB NOT NULL,
    source_file     TEXT NOT NULL,
    load_timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_fintech_load_id
    ON bronze.raw_fintech_data (load_id);

CREATE INDEX IF NOT EXISTS idx_raw_fintech_load_timestamp
    ON bronze.raw_fintech_data (load_timestamp DESC);
"""

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: build a spark-submit BashOperator for a flatten script.
#
# Los 3 flatteners comparten la misma forma de invocación (mismo --jars,
# mismo --master, mismo env). Lo único que cambia es qué script ejecutan.
# Usamos un factory para evitar repetir el bloque tres veces — si mañana
# hay que cambiar la versión del JDBC driver o un flag de spark-submit, se
# toca en un solo lugar.
# ---------------------------------------------------------------------------
def build_flatten_task(script_name: str) -> BashOperator:
    """
    Returns a BashOperator that runs spark-submit on the given script.

    Args:
        script_name: nombre del script sin extensión (ej: "flatten_accounts").
                     El task_id se deriva del mismo nombre.
    """
    return BashOperator(
        task_id=script_name,
        bash_command=(
            f"spark-submit "
            f"--jars {JDBC_DRIVER} "
            f"--driver-class-path {JDBC_DRIVER} "
            f"--master local[*] "
            f"{SPARK_SCRIPTS_DIR}/{script_name}.py"
        ),
        env=SPARK_ENV,
        append_env=True,  # keep inherited env vars (Airflow internals, etc.)
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="qversity_pipeline",
    description="Bronze ingestion + Silver flatten (PySpark) + full dbt silver/gold build.",
    start_date=datetime(2026, 1, 1),
    schedule=None,             # manual trigger; dataset is static
    catchup=False,
    tags=["qversity", "bronze", "silver", "gold", "spark", "dbt"],
    default_args={
        "owner": "qversity",
        "retries": 1,
    },
)
def qversity_pipeline():

    @task
    def ensure_bronze_table() -> None:
        """Create bronze.raw_fintech_data table if it does not exist."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(DDL)
        logger.info("Bronze table ensured.")

    @task
    def download_from_s3() -> str:
        """Download JSON dataset from public S3 bucket to local /tmp."""
        LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        local_path = LOCAL_DIR / SOURCE_FILE

        logger.info("Downloading %s ...", S3_URL)
        with requests.get(S3_URL, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with local_path.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    fh.write(chunk)

        size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info("Downloaded to %s (%.2f MB)", local_path, size_mb)
        return str(local_path)

    @task
    def load_to_bronze(local_path: str) -> int:
        """Parse JSON and bulk-insert each record into bronze.raw_fintech_data."""
        with open(local_path, "r", encoding="utf-8") as fh:
            records = json.load(fh)

        if not isinstance(records, list):
            raise ValueError(
                f"Expected a JSON array at top level, got {type(records).__name__}"
            )
        if not records:
            raise ValueError("Dataset is empty; aborting load.")

        load_id = str(uuid.uuid4())
        logger.info("Loading %d records with load_id=%s", len(records), load_id)

        rows = [
            (load_id, json.dumps(rec), SOURCE_FILE)
            for rec in records
        ]

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO bronze.raw_fintech_data
                        (load_id, data, source_file)
                    VALUES %s
                    """,
                    rows,
                    page_size=500,
                )
            conn.commit()
        finally:
            conn.close()

        logger.info("Inserted %d records for load_id=%s", len(rows), load_id)
        return len(rows)

    # -----------------------------------------------------------------------
    # Spark tasks: 3 flatteners in parallel
    # -----------------------------------------------------------------------
    # Cada uno lee bronze independientemente y escribe su propia staging
    # table en silver_raw. No hay dependencias entre ellos, así que pueden
    # ir en paralelo. Para que esto efectivamente paralelice hace falta que
    # Airflow esté corriendo con LocalExecutor (o superior); con
    # SequentialExecutor van a ejecutarse uno tras otro igual.
    flatten_accounts     = build_flatten_task("flatten_accounts")
    flatten_transactions = build_flatten_task("flatten_transactions")
    flatten_loans        = build_flatten_task("flatten_loans")

    # -----------------------------------------------------------------------
    # dbt tasks: seeds → full project build → full test suite
    # -----------------------------------------------------------------------
    # Por qué BashOperator y no un operador dbt dedicado:
    #   - El provider oficial airflow-dbt requiere pinning de versiones y
    #     setup extra; para un proyecto de 14 días con un único warehouse,
    #     BashOperator es más simple, más transparente en logs, y permite
    #     copiar el comando exacto desde la UI de Airflow para reproducir
    #     manualmente.
    #   - dbt CLI devuelve exit codes apropiados (no-cero ante test failure
    #     o error de compilación), así que el task de Airflow falla bien.
    #
    # Por qué pasamos `--profiles-dir` Y la env var DBT_PROFILES_DIR:
    # redundancia defensiva. Si en algún momento se invoca el comando
    # fuera de este DAG (debug manual desde shell), el flag explícito
    # lo hace funcionar sin depender del entorno.
    #
    # Por qué dbt_seed corre como una task separada antes de dbt_run:
    # los seeds (CSVs en dbt/seeds/: fx_rates, country_currency) son inputs
    # estáticos que varios modelos joinean. dbt NO los carga como parte de
    # `dbt run` — necesita una invocación explícita de `dbt seed`. Es
    # idempotente: si los seeds ya existen, los recrea con el mismo
    # contenido del CSV.
    #
    # Sin `--select`: dbt corre TODO el proyecto (3 stg + 7 dim + 2 fct +
    # 1 agg + 3 int + 18 marts = 34 modelos, ~434 tests).
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt seed "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        env=DBT_ENV,
        append_env=True,
    )

    # -----------------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------------
    # Pipeline completo:
    #   ensure → download → load → [3 flatteners en paralelo]
    #         → dbt_seed → dbt_run → dbt_test
    #
    # Por qué dbt_run >> dbt_test (secuencial) en lugar de paralelo:
    # los tests aseveran sobre el output del run. Correrlos en paralelo
    # haría race sobre la creación de las tablas. El costo de serializar
    # dos tasks de ~5s es cero.
    ensure = ensure_bronze_table()
    local_path = download_from_s3()
    loaded = load_to_bronze(local_path)

    ensure >> local_path >> loaded >> [
        flatten_accounts,
        flatten_transactions,
        flatten_loans,
    ] >> dbt_seed >> dbt_run >> dbt_test


qversity_pipeline()
