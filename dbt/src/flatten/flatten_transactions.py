# Databricks notebook source
# =============================================================================
# flatten_transactions  — port Delta-native de spark/flatten_transactions.py
# Mismos cambios que flatten_accounts: read.table + from_json + write Delta.
# OJO: el campo de fecha viene como `date` en el JSON y se renombra a
# `transaction_date` para que coincida con stg_transactions (dbt).
# =============================================================================

# COMMAND ----------
# MAGIC %run ./utils

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, from_json, length, trim, when
from pyspark.sql.types import (
    ArrayType, DoubleType, StringType, StructField, StructType,
)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("account_id",     StringType(), nullable=True),
    StructField("date",           StringType(), nullable=True),  # -> transaction_date
    StructField("amount",         DoubleType(), nullable=True),
    StructField("currency",       StringType(), nullable=True),
    StructField("type",           StringType(), nullable=True),
    StructField("category",       StringType(), nullable=True),
    StructField("merchant",       StringType(), nullable=True),
    StructField("channel",        StringType(), nullable=True),
    StructField("status",         StringType(), nullable=True),
    StructField("description",    StringType(), nullable=True),
])

CUSTOMER_PARTIAL_SCHEMA = StructType([
    StructField("customer_id",  StringType(), nullable=False),
    StructField("transactions", ArrayType(TRANSACTION_SCHEMA), nullable=True),
])

# COMMAND ----------
def flatten_transactions(bronze_df: DataFrame) -> DataFrame:
    parsed = bronze_df.select(
        col("id").alias("bronze_id"),
        col("load_timestamp"),
        from_json(col("data"), CUSTOMER_PARTIAL_SCHEMA).alias("parsed"),
    )

    exploded = parsed.select(
        col("bronze_id"),
        col("load_timestamp"),
        col("parsed.customer_id").alias("customer_id"),
        explode(col("parsed.transactions")).alias("tx"),
    )

    flat = exploded.select(
        col("customer_id"),
        col("tx.transaction_id").alias("transaction_id"),
        col("tx.account_id").alias("account_id"),
        col("tx.date").alias("transaction_date"),
        col("tx.amount").alias("amount"),
        col("tx.currency").alias("currency"),
        col("tx.type").alias("transaction_type"),
        col("tx.category").alias("category"),
        col("tx.merchant").alias("merchant"),
        col("tx.channel").alias("channel"),
        col("tx.status").alias("status"),
        col("tx.description").alias("description"),
        col("bronze_id"),
        col("load_timestamp"),
    )

    string_cols = ["customer_id", "transaction_id", "account_id", "currency",
                   "transaction_type", "category", "merchant", "channel",
                   "status", "description"]
    for c in string_cols:
        flat = flat.withColumn(
            c, when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))
        )
    return flat

# COMMAND ----------
bronze_df = spark.read.table(f"{CATALOG}.bronze.raw_fintech_data")
print(f"[flatten_transactions] bronze records read: {bronze_df.count()}")

flat = flatten_transactions(bronze_df)
print(f"[flatten_transactions] transactions after explode: {flat.count()}")

deduped = deduplicate_by_pk(flat, "transaction_id")
print(f"[flatten_transactions] transactions after dedup: {deduped.count()}")

write_silver_raw(deduped, CATALOG, "stg_transactions")
