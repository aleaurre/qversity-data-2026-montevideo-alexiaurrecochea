# Databricks notebook source
# =============================================================================
# flatten_accounts
# -----------------------------------------------------------------------------
# Port Delta-native de spark/flatten_accounts.py.
#
# Cambios respecto al original:
#   - Lee `<catalog>.bronze.raw_fintech_data` con spark.read.table (no JDBC).
#   - `data` ya es STRING (igual que el jsonb leído por JDBC), así que el
#     from_json con schema explícito queda IDÉNTICO.
#   - Escribe a `<catalog>.silver_raw.stg_accounts` como Delta (no JDBC).
#   - La limpieza sintáctica (trim, vacío->NULL) y el dedup NO cambian.
#
# La normalización semántica (casing, idioma) sigue siendo trabajo de dbt.
# =============================================================================

# COMMAND ----------
# MAGIC %run ./utils

# COMMAND ----------
dbutils.widgets.text("catalog", "qversity")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, from_json, length, trim, when
from pyspark.sql.types import (
    ArrayType, DoubleType, StringType, StructField, StructType,
)

# Schema explícito del array accounts[] (idéntico al original).
ACCOUNT_SCHEMA = StructType([
    StructField("account_id",    StringType(), nullable=False),
    StructField("account_type",  StringType(), nullable=True),
    StructField("currency",      StringType(), nullable=True),
    StructField("balance",       DoubleType(), nullable=True),
    StructField("credit_limit",  DoubleType(), nullable=True),
    StructField("interest_rate", DoubleType(), nullable=True),
    StructField("opened_date",   StringType(), nullable=True),  # parseado en dbt
    StructField("status",        StringType(), nullable=True),
    StructField("branch_code",   StringType(), nullable=True),
])

CUSTOMER_PARTIAL_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("accounts",    ArrayType(ACCOUNT_SCHEMA), nullable=True),
])

# COMMAND ----------
def flatten_accounts(bronze_df: DataFrame) -> DataFrame:
    parsed = bronze_df.select(
        col("id").alias("bronze_id"),
        col("load_timestamp"),
        from_json(col("data"), CUSTOMER_PARTIAL_SCHEMA).alias("parsed"),
    )

    exploded = parsed.select(
        col("bronze_id"),
        col("load_timestamp"),
        col("parsed.customer_id").alias("customer_id"),
        explode(col("parsed.accounts")).alias("acct"),
    )

    flat = exploded.select(
        col("customer_id"),
        col("acct.account_id").alias("account_id"),
        col("acct.account_type").alias("account_type"),
        col("acct.currency").alias("currency"),
        col("acct.balance").alias("balance"),
        col("acct.credit_limit").alias("credit_limit"),
        col("acct.interest_rate").alias("interest_rate"),
        col("acct.opened_date").alias("opened_date"),
        col("acct.status").alias("status"),
        col("acct.branch_code").alias("branch_code"),
        col("bronze_id"),
        col("load_timestamp"),
    )

    # Limpieza sintáctica: trim + vacío->NULL. Sin casing/idioma (eso es dbt).
    string_cols = ["customer_id", "account_id", "account_type",
                   "currency", "status", "branch_code"]
    for c in string_cols:
        flat = flat.withColumn(
            c, when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))
        )
    return flat

# COMMAND ----------
bronze_df = spark.read.table(f"{CATALOG}.bronze.raw_fintech_data")
print(f"[flatten_accounts] bronze records read: {bronze_df.count()}")

flat = flatten_accounts(bronze_df)
print(f"[flatten_accounts] accounts after explode: {flat.count()}")

deduped = deduplicate_by_pk(flat, "account_id")
print(f"[flatten_accounts] accounts after dedup: {deduped.count()}")

write_silver_raw(deduped, CATALOG, "stg_accounts")
