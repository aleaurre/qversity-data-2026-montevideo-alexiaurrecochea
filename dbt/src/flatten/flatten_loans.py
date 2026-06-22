# Databricks notebook source
# =============================================================================
# flatten_loans  — port Delta-native de spark/flatten_loans.py
# explode() (no explode_outer): los customers con 0 loans quedan AUSENTES de
# stg_loans a propósito, para que loan_id pueda testearse not_null en dbt.
# La cardinalidad cliente<->loan se resuelve después con LEFT JOIN en dbt.
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
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# Schema del array loans[]. Campos confirmados por los marts/ERD del proyecto.
# Ajustá tipos/nombres si tu JSON difiere — esto es el único punto sensible.
LOAN_SCHEMA = StructType([
    StructField("loan_id",             StringType(),  nullable=False),
    StructField("type",                StringType(),  nullable=True),  # -> loan_type
    StructField("principal",           DoubleType(),  nullable=True),
    StructField("outstanding_balance", DoubleType(),  nullable=True),
    StructField("monthly_payment",     DoubleType(),  nullable=True),
    StructField("interest_rate",       DoubleType(),  nullable=True),  # escala 0-100
    StructField("currency",            StringType(),  nullable=True),
    StructField("status",              StringType(),  nullable=True),
    StructField("start_date",          StringType(),  nullable=True),  # parseado en dbt
    StructField("end_date",            StringType(),  nullable=True),  # parseado en dbt
    StructField("collateral_type",     StringType(),  nullable=True),
    StructField("days_past_due",       IntegerType(), nullable=True),
])

CUSTOMER_PARTIAL_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("loans",       ArrayType(LOAN_SCHEMA), nullable=True),
])

# COMMAND ----------
def flatten_loans(bronze_df: DataFrame) -> DataFrame:
    parsed = bronze_df.select(
        col("id").alias("bronze_id"),
        col("load_timestamp"),
        from_json(col("data"), CUSTOMER_PARTIAL_SCHEMA).alias("parsed"),
    )

    exploded = parsed.select(
        col("bronze_id"),
        col("load_timestamp"),
        col("parsed.customer_id").alias("customer_id"),
        explode(col("parsed.loans")).alias("loan"),   # explode, NO explode_outer
    )

    flat = exploded.select(
        col("customer_id"),
        col("loan.loan_id").alias("loan_id"),
        col("loan.type").alias("loan_type"),
        col("loan.principal").alias("principal"),
        col("loan.outstanding_balance").alias("outstanding_balance"),
        col("loan.monthly_payment").alias("monthly_payment"),
        col("loan.interest_rate").alias("interest_rate"),
        col("loan.currency").alias("currency"),
        col("loan.status").alias("status"),
        col("loan.start_date").alias("start_date"),
        col("loan.end_date").alias("end_date"),
        col("loan.collateral_type").alias("collateral_type"),
        col("loan.days_past_due").alias("days_past_due"),
        col("bronze_id"),
        col("load_timestamp"),
    )

    string_cols = ["customer_id", "loan_id", "loan_type", "currency",
                   "status", "collateral_type"]
    for c in string_cols:
        flat = flat.withColumn(
            c, when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))
        )
    return flat

# COMMAND ----------
bronze_df = spark.read.table(f"{CATALOG}.bronze.raw_fintech_data")
print(f"[flatten_loans] bronze records read: {bronze_df.count()}")

flat = flatten_loans(bronze_df)
print(f"[flatten_loans] loans after explode: {flat.count()}")

deduped = deduplicate_by_pk(flat, "loan_id")
print(f"[flatten_loans] loans after dedup: {deduped.count()}")

write_silver_raw(deduped, CATALOG, "stg_loans")
