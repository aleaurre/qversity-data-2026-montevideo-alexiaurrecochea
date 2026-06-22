"""
Flatten del array `loans[]` desde bronze hacia silver.

Lee `bronze.raw_fintech_data` (jsonb), expande el array `data.loans`
(0-3 elementos por customer) y escribe el resultado en
`<TARGET_SCHEMA>.stg_loans` (típicamente `silver_raw.stg_loans`).

Cada fila de salida representa UN préstamo. Customers sin préstamos NO
aparecen en esta tabla — esto es intencional: es una tabla de hechos
de préstamos, no una tabla de "customer × loan_status".

Si en gold/dbt necesitamos métricas tipo "% de customers con loan", eso
se resuelve con un LEFT JOIN desde `dim_customers` hacia esta tabla, que
es la forma idiomática de modelarlo. Por eso usamos `explode` normal y
no `explode_outer`: si usáramos outer, tendríamos filas con todos los
campos de loan en NULL para customers sin loans, lo que rompe los tests
de not_null que vamos a poner sobre `loan_id` en dbt.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, from_json, length, trim, when
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dbt.src.flatten.utils import deduplicate_by_pk, get_jdbc_config, get_spark_session, TARGET_SCHEMA


# Schema explícito del array `loans[]`.
# Mismas razones que en los otros flatteners: jsonb → string vía JDBC, y
# preferimos schema fijo sobre inferencia.
#
# Notas sobre tipos:
#   - `term_months` y `days_past_due` van como Integer. Spark los castea desde
#     el JSON automáticamente. Si vienen como string en algún record, salen
#     NULL — los tests de dbt los van a atrapar.
#   - `days_past_due` puede ser 0 (loan al día) o positivo (en mora). NO lo
#     normalizamos a 0 si viene NULL: NULL significa "no aplica / no reportado"
#     y es información útil para distinguir de "0 días en mora".
#   - Montos como Double siguiendo el patrón del proyecto. Cast a numeric
#     con precisión financiera correcta se hace en dbt silver.
LOAN_SCHEMA = StructType([
    StructField("loan_id",             StringType(),  nullable=False),
    StructField("type",                StringType(),  nullable=True),
    StructField("currency",            StringType(),  nullable=True),
    StructField("principal",           DoubleType(),  nullable=True),
    StructField("outstanding_balance", DoubleType(),  nullable=True),
    StructField("interest_rate",       DoubleType(),  nullable=True),
    StructField("term_months",         IntegerType(), nullable=True),
    StructField("monthly_payment",     DoubleType(),  nullable=True),
    StructField("start_date",          StringType(),  nullable=True),  # parseamos abajo
    StructField("end_date",            StringType(),  nullable=True),  # parseamos abajo
    StructField("status",              StringType(),  nullable=True),
    StructField("days_past_due",       IntegerType(), nullable=True),
    StructField("collateral_type",     StringType(),  nullable=True),
])


def read_bronze(spark: SparkSession, jdbc: dict) -> DataFrame:
    """
    Lee bronze.raw_fintech_data via JDBC. Particionado por `id` en 4 splits.
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", "bronze.raw_fintech_data")
        .option("user", jdbc["properties"]["user"])
        .option("password", jdbc["properties"]["password"])
        .option("driver", jdbc["properties"]["driver"])
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "100000")
        .option("numPartitions", "4")
        .load()
    )


def flatten_loans(bronze_df: DataFrame) -> DataFrame:
    """
    Toma el DataFrame de bronze (con `data` como string JSON) y devuelve
    un DataFrame plano a nivel préstamo, con limpieza sintáctica básica.

    Usa `explode` (no `explode_outer`): customers sin loans desaparecen,
    que es lo correcto para una tabla de hechos.
    """
    customer_partial_schema = StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("loans",       ArrayType(LOAN_SCHEMA), nullable=True),
    ])

    parsed = bronze_df.select(
        col("id").alias("bronze_id"),
        col("load_timestamp"),
        from_json(col("data"), customer_partial_schema).alias("parsed"),
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
        col("loan.term_months").alias("term_months"),
        col("loan.monthly_payment").alias("monthly_payment"),
        col("loan.start_date").alias("start_date"),
        col("loan.end_date").alias("end_date"),
        col("loan.status").alias("status"),
        col("loan.days_past_due").alias("days_past_due"),
        col("loan.collateral_type").alias("collateral_type"),
        col("bronze_id"),
        col("load_timestamp"),
    )

    # ─── Limpieza sintáctica ───────────────────────────────────────────────
    # Trim + empty → NULL para columnas categóricas / identificadores.
    # No tocamos campos numéricos ni dates (ya parseados).
    string_cols = [
        "customer_id", "loan_id", "loan_type", "currency",
        "status", "collateral_type",
    ]
    for c in string_cols:
        flat = flat.withColumn(
            c,
            when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))
        )

    return flat


def write_silver(df: DataFrame, jdbc: dict) -> None:
    """Escribe a <TARGET_SCHEMA>.stg_loans con overwrite + truncate."""
    (
        df.write
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", f"{TARGET_SCHEMA}.stg_loans")
        .option("user", jdbc["properties"]["user"])
        .option("password", jdbc["properties"]["password"])
        .option("driver", jdbc["properties"]["driver"])
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main() -> int:
    spark = get_spark_session("flatten-loans")
    spark.sparkContext.setLogLevel("WARN")

    try:
        jdbc = get_jdbc_config()

        bronze_df = read_bronze(spark, jdbc)
        bronze_count = bronze_df.count()
        print(f"[flatten_loans] bronze records read: {bronze_count}")

        flat = flatten_loans(bronze_df)
        flat_count = flat.count()
        print(f"[flatten_loans] loans after explode: {flat_count}")

        deduped = deduplicate_by_pk(flat, "loan_id")
        final_count = deduped.count()
        dropped = flat_count - final_count
        print(f"[flatten_loans] loans after dedup: {final_count}")
        print(f"[flatten_loans] duplicates dropped: {dropped}")

        write_silver(deduped, jdbc)
        print(f"[flatten_loans] wrote {final_count} rows to {TARGET_SCHEMA}.stg_loans")

        return 0
    except Exception as e:
        print(f"[flatten_loans] FAILED: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())