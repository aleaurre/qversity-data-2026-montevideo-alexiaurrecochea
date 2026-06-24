# Databricks notebook source
# =============================================================================
# flatten_loans  — port Delta-native de spark/flatten_loans.py
# FIX: se agrega `term_months` (IntegerType) al schema y al select — era el
# único campo que la versión reconstruida de memoria se había comido, y por eso
# stg_loans fallaba con "term_months cannot be resolved".
# explode() (no explode_outer): los customers con 0 loans quedan ausentes.
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
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# Schema del array loans[] — IDÉNTICO al LOAN_SCHEMA del proyecto original.
LOAN_SCHEMA = StructType([
    StructField("loan_id",             StringType(),  nullable=False),
    StructField("type",                StringType(),  nullable=True),   # -> loan_type
    StructField("currency",            StringType(),  nullable=True),
    StructField("principal",           StringType(), nullable=True),
    StructField("outstanding_balance", StringType(), nullable=True),
    StructField("monthly_payment",     StringType(), nullable=True),
    StructField("interest_rate",       StringType(), nullable=True),
    StructField("term_months",         IntegerType(), nullable=True),   # <-- FIX: faltaba
    StructField("start_date",          StringType(),  nullable=True),   # parseado en dbt
    StructField("end_date",            StringType(),  nullable=True),   # parseado en dbt
    StructField("status",              StringType(),  nullable=True),
    StructField("days_past_due",       IntegerType(), nullable=True),
    StructField("collateral_type",     StringType(),  nullable=True),
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
        explode(col("parsed.loans")).alias("loan"),
    )

    flat = exploded.select(
        col("customer_id"),
        col("loan.loan_id").alias("loan_id"),
        col("loan.type").alias("loan_type"),
        col("loan.currency").alias("currency"),
        col("loan.principal").alias("principal"),
        col("loan.outstanding_balance").alias("outstanding_balance"),
        col("loan.interest_rate").alias("interest_rate"),
        col("loan.term_months").alias("term_months"),          # <-- FIX
        col("loan.monthly_payment").alias("monthly_payment"),
        col("loan.start_date").alias("start_date"),
        col("loan.end_date").alias("end_date"),
        col("loan.status").alias("status"),
        col("loan.days_past_due").alias("days_past_due"),
        col("loan.collateral_type").alias("collateral_type"),
        col("bronze_id"),
        col("load_timestamp"),
    )

    from pyspark.sql.functions import regexp_replace
    for num_col in ["principal", "outstanding_balance", "monthly_payment", "interest_rate"]:
        flat = flat.withColumn(
            num_col,
            regexp_replace(col(num_col), r"[^0-9.\-]", "").cast(DoubleType())
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
