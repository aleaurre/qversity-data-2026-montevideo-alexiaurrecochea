"""
Qversity ELT — Bronze layer ingestion.

DAG that downloads the fintech banking dataset from a public S3 bucket and
loads each customer record into bronze.raw_fintech_data as JSONB.

Idempotency: every run gets a unique load_id (UUID). Downstream layers (Silver)
always read the latest batch, so re-running is safe and produces no duplicates
from Silver's perspective.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
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
    dag_id="qversity_bronze_ingestion",
    description="Download fintech dataset from S3 and load to bronze layer.",
    start_date=datetime(2026, 1, 1),
    schedule=None,             # manual trigger; dataset is static
    catchup=False,
    tags=["qversity", "bronze", "ingestion"],
    default_args={
        "owner": "qversity",
        "retries": 1,
    },
)
def qversity_bronze_ingestion():

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
        logger.info(
            "Loading %d records with load_id=%s", len(records), load_id
        )

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

    # Task dependencies
    ensure = ensure_bronze_table()
    local_path = download_from_s3()
    loaded = load_to_bronze(local_path)

    ensure >> local_path >> loaded


qversity_bronze_ingestion()