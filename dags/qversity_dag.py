"""
Qversity ELT — Bronze ingestion + Silver flatten (accounts).

DAG that:
  1. Ensures bronze.raw_fintech_data exists.
  2. Downloads the fintech dataset from a public S3 bucket.
  3. Loads each customer record into bronze.raw_fintech_data as JSONB.
  4. Runs a PySpark job (spark-submit) that flattens accounts[] into
     silver.stg_accounts with dedup and syntactic cleaning.

Idempotency: every run gets a unique load_id (UUID). The Spark dedup logic
keeps only the most recent version of each account_id, so re-running
end-to-end is safe and produces no duplicates downstream.
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
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="qversity_pipeline",
    description="Bronze ingestion + Silver flatten of accounts via PySpark.",
    start_date=datetime(2026, 1, 1),
    schedule=None,             # manual trigger; dataset is static
    catchup=False,
    tags=["qversity", "bronze", "silver", "spark"],
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
    # Spark task: flatten accounts[] from bronze into silver.stg_accounts
    # -----------------------------------------------------------------------
    # We call spark-submit as a subprocess via BashOperator. The script lives
    # in /opt/airflow/spark/ (mounted from ./spark on the host). Postgres
    # credentials are passed through env vars to the subprocess, since
    # BashOperator does not propagate the scheduler's env by default.
    flatten_accounts = BashOperator(
        task_id="flatten_accounts",
        bash_command=(
            f"spark-submit "
            f"--jars {JDBC_DRIVER} "
            f"--driver-class-path {JDBC_DRIVER} "
            f"--master local[*] "
            f"{SPARK_SCRIPTS_DIR}/flatten_accounts.py"
        ),
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB":   os.getenv("POSTGRES_DB",   "qversity_warehouse"),
            "POSTGRES_USER":     os.getenv("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
        },
        append_env=True,  # keep inherited env vars
    )

    # -----------------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------------
    ensure = ensure_bronze_table()
    local_path = download_from_s3()
    loaded = load_to_bronze(local_path)

    ensure >> local_path >> loaded >> flatten_accounts


qversity_pipeline()