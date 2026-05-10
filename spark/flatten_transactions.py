"""
Flatten del array `transactions[]` desde bronze hacia silver.

Lee `bronze.raw_fintech_data` (jsonb), expande el array `data.transactions`
(5-30 elementos por customer → ~75k-150k filas para 5k customers), y escribe
el resultado en `silver.stg_transactions`.

Cada fila de salida representa UNA transacción. Propagamos `customer_id`
desde el record padre para mantener la relación con la futura `dim_customer`.
La FK real con la cuenta (`account_id`) ya viene dentro del objeto de
transacción.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, from_json, length, to_date, trim, when
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from utils import deduplicate_by_pk, get_jdbc_config, get_spark_session


# Schema explícito del array `transactions[]`.
# Mismas razones que en flatten_accounts: jsonb llega como string vía JDBC,
# necesitamos `from_json` con un schema fijo para no depender de inferencia
# (que sobre 75k+ filas sería lento y frágil ante nulls esporádicos).
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("account_id",     StringType(), nullable=True),
    StructField("date",           StringType(), nullable=True),  # parseamos abajo
    StructField("amount",         DoubleType(), nullable=True),
    StructField("currency",       StringType(), nullable=True),
    StructField("type",           StringType(), nullable=True),
    StructField("category",       StringType(), nullable=True),
    StructField("merchant",       StringType(), nullable=True),
    StructField("channel",        StringType(), nullable=True),
    StructField("status",         StringType(), nullable=True),
    StructField("description",    StringType(), nullable=True),
])


def read_bronze(spark: SparkSession, jdbc: dict) -> DataFrame:
    """
    Lee la tabla bronze.raw_fintech_data via JDBC.

    Particionamos la lectura por la PK `id` (BIGSERIAL) en 4 splits paralelos.
    Para 10k records en bronze es overkill, pero es la práctica correcta y
    se nota cuando crece el dataset.

    `data` viene como string (no struct) porque el driver JDBC de Postgres
    no mapea jsonb a struct nativo de Spark. Lo parseamos con `from_json`
    aguas abajo.
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


def flatten_transactions(bronze_df: DataFrame) -> DataFrame:
    """
    Toma el DataFrame de bronze (con `data` como string JSON) y devuelve
    un DataFrame plano a nivel transacción, con limpieza sintáctica básica
    (trim, vacíos → NULL).

    NO normalizamos casing ni mapeamos categorías acá: eso es decisión de
    negocio y vive en dbt silver. PySpark se limita a flatten + dedup +
    syntactic cleanup.
    """
    customer_partial_schema = StructType([
        StructField("customer_id",  StringType(), nullable=False),
        StructField("transactions", ArrayType(TRANSACTION_SCHEMA), nullable=True),
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
        explode(col("parsed.transactions")).alias("tx"),
    )

    # Casteo de `date`:
    #   El JSON expone `date` como string. Usamos `to_date` (no `to_timestamp`)
    #   porque ninguna de las business questions del Section 7 necesita hora
    #   del día — la más fina es Q16 ("transaction volume by day of week"),
    #   que se resuelve con date. Si en el futuro aparece una pregunta sobre
    #   horarios, se agrega `transaction_ts` sin romper este modelo.
    #
    # Casteo de `amount`:
    #   Lo mantenemos como Double siguiendo el patrón de `flatten_accounts.py`
    #   (donde balance también es Double). El cast a numeric/decimal con
    #   precisión financiera correcta se hace en dbt silver, donde tenemos
    #   más contexto sobre la grain de cada métrica. Documentado en README.
    flat = exploded.select(
        col("customer_id"),
        col("tx.transaction_id").alias("transaction_id"),
        col("tx.account_id").alias("account_id"),
        to_date(col("tx.date"), "yyyy-MM-dd").alias("transaction_date"),
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

    # ─── Limpieza sintáctica ───────────────────────────────────────────────
    # Trim de strings + empty → NULL para TODAS las columnas string, incluida
    # `description`. Criterio: un espacio al borde nunca es señal, es ruido
    # upstream. Aplicamos la misma regla a todos los campos por consistencia
    # con flatten_accounts y para no introducir excepciones que después haya
    # que justificar caso por caso. Si en el futuro aparece un caso donde el
    # whitespace de borde tenga valor semántico, se trata acá explícitamente.
    string_cols = [
        "customer_id", "transaction_id", "account_id", "currency",
        "transaction_type", "category", "merchant", "channel", "status",
        "description",
    ]
    for c in string_cols:
        flat = flat.withColumn(
            c,
            when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))
        )

    return flat


def write_silver(df: DataFrame, jdbc: dict) -> None:
    """
    Escribe a silver.stg_transactions en modo overwrite con truncate=true.

    Mismo razonamiento que en flatten_accounts: staging refleja la última
    vista de bronze, y truncate preserva permisos/constraints que dbt o
    nosotros podamos haber definido sobre la tabla.
    """
    (
        df.write
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", "silver.stg_transactions")
        .option("user", jdbc["properties"]["user"])
        .option("password", jdbc["properties"]["password"])
        .option("driver", jdbc["properties"]["driver"])
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main() -> int:
    spark = get_spark_session("flatten-transactions")
    spark.sparkContext.setLogLevel("WARN")

    try:
        jdbc = get_jdbc_config()

        bronze_df = read_bronze(spark, jdbc)
        bronze_count = bronze_df.count()
        print(f"[flatten_transactions] bronze records read: {bronze_count}")

        flat = flatten_transactions(bronze_df)
        flat_count = flat.count()
        print(f"[flatten_transactions] transactions after explode: {flat_count}")

        deduped = deduplicate_by_pk(flat, "transaction_id")
        final_count = deduped.count()
        dropped = flat_count - final_count
        print(f"[flatten_transactions] transactions after dedup: {final_count}")
        print(f"[flatten_transactions] duplicates dropped: {dropped}")

        write_silver(deduped, jdbc)
        print(f"[flatten_transactions] wrote {final_count} rows to silver.stg_transactions")

        return 0
    except Exception as e:
        print(f"[flatten_transactions] FAILED: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()



if __name__ == "__main__":
    sys.exit(main())