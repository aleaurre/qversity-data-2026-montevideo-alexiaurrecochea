"""
Flatten del array `accounts[]` desde bronze hacia silver.

Lee `bronze.raw_fintech_data` (jsonb), expande el array `data.accounts`
(2-5 elementos por customer), y escribe el resultado en `silver.stg_accounts`.

Cada fila de salida representa UNA cuenta. El `customer_id` se propaga desde
el record padre para mantener la FK con la futura `dim_customer`.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, to_date, to_timestamp
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DoubleType,
)

# Importamos desde el mismo directorio. spark-submit agrega el dir del script
# al sys.path automáticamente, así que `from utils import ...` funciona.
from utils import get_spark_session, get_jdbc_config


# Schema explícito del array `accounts[]`.
# Lo declaramos a mano (en vez de dejar que Spark infiera) por dos razones:
#   1. El JSON viene de jsonb, que Spark recibiría como string. Necesitamos
#      decirle qué estructura esperar para hacer `from_json`.
#   2. Inferir schema sobre 10k+ records implica un pase extra y nos arriesga
#      a que un null en un campo poco frecuente cambie el tipo entre runs.
ACCOUNT_SCHEMA = StructType([
    StructField("account_id",     StringType(), nullable=False),
    StructField("account_type",   StringType(), nullable=True),
    StructField("currency",       StringType(), nullable=True),
    StructField("balance",        DoubleType(), nullable=True),
    StructField("credit_limit",   DoubleType(), nullable=True),
    StructField("interest_rate",  DoubleType(), nullable=True),
    StructField("opened_date",    StringType(), nullable=True),  # parseamos abajo
    StructField("status",         StringType(), nullable=True),
    StructField("branch_code",    StringType(), nullable=True),
])


def read_bronze(spark: SparkSession, jdbc: dict) -> DataFrame:
    """
    Lee la tabla bronze.raw_fintech_data via JDBC.

    Importante: Postgres expone columnas `jsonb` como tipo `string` cuando
    se las lee con el driver JDBC estándar (no como struct nativo). Esto es
    una particularidad del driver — no hay mapeo directo jsonb ↔ Spark struct.
    Por eso leemos `data` como string y después aplicamos `from_json` con
    el schema explícito.
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", "bronze.raw_fintech_data")
        .option("user", jdbc["properties"]["user"])
        .option("password", jdbc["properties"]["password"])
        .option("driver", jdbc["properties"]["driver"])
        # Particionado de lectura: dividimos el read en 4 splits paralelos
        # usando la PK `id` (BIGSERIAL). Para 10k records es overkill, pero
        # es la práctica correcta y se nota cuando el dataset crece.
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "100000")
        .option("numPartitions", "4")
        .load()
    )


def flatten_accounts(bronze_df: DataFrame) -> DataFrame:
    """
    Toma el DataFrame de bronze (con `data` como string JSON) y devuelve
    un DataFrame plano a nivel cuenta, con limpieza sintáctica básica
    (trim de strings, vacíos → NULL).

    NO normalizamos casing ni traducciones acá: eso es decisión de negocio
    y vive en dbt silver, no en Spark.
    """
    from pyspark.sql.functions import from_json, trim, when, length
    from pyspark.sql.types import ArrayType

    customer_partial_schema = StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("accounts",    ArrayType(ACCOUNT_SCHEMA), nullable=True),
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
        to_date(col("acct.opened_date"), "yyyy-MM-dd").alias("opened_date"),
        col("acct.status").alias("status"),
        col("acct.branch_code").alias("branch_code"),
        col("bronze_id"),
        col("load_timestamp"),
    )

    # ─── Limpieza sintáctica ───────────────────────────────────────────────
    # Aplicamos trim() a todas las columnas string y convertimos strings
    # vacíos resultantes a NULL. Esto es seguro hacerlo en Spark porque
    # NO toma decisiones de negocio: un espacio al inicio nunca es señal,
    # es un bug. La normalización semántica (case, idioma) vive en dbt.
    string_cols = [
        "customer_id", "account_id", "account_type",
        "currency", "status", "branch_code",
    ]
    for c in string_cols:
        flat = flat.withColumn(
            c,
            when(length(trim(col(c))) == 0, None).otherwise(trim(col(c)))
        )

    return flat


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplica accounts.

    Criterio: si bronze tiene múltiples loads del mismo dataset (cosa que ya
    sospechamos viendo 10.200 records en bronze cuando esperábamos ~5.000),
    el mismo `account_id` puede aparecer en más de un `bronze_id`. Nos
    quedamos con la versión MÁS RECIENTE (mayor `load_timestamp`), que es
    la semántica correcta para un staging table que alimenta silver.

    Si dos versiones del mismo account_id tienen el mismo load_timestamp
    (caso degenerado: el DAG cargó dos veces en el mismo segundo), Spark
    elige una arbitrariamente — registramos un warning para el README.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    w = Window.partitionBy("account_id").orderBy(col("load_timestamp").desc())

    deduped = (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )
    return deduped


def write_silver(df: DataFrame, jdbc: dict) -> None:
    """
    Escribe a silver.stg_accounts en modo overwrite.

    `mode=overwrite` es lo correcto para una staging table: el contrato es
    "esto refleja la última vista de bronze". Si un día queremos historiar
    cambios, eso ya pertenece a una capa SCD2 en silver/gold, no acá.

    `truncate=true` reusa la tabla existente en lugar de droppearla y
    recrearla. Importante porque si la dropea, perdemos cualquier permiso
    o constraint que dbt haya definido encima.
    """
    (
        df.write
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", "silver.stg_accounts")
        .option("user", jdbc["properties"]["user"])
        .option("password", jdbc["properties"]["password"])
        .option("driver", jdbc["properties"]["driver"])
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main() -> int:
    spark = get_spark_session("flatten-accounts")
    spark.sparkContext.setLogLevel("WARN")  # menos ruido en logs de Airflow

    try:
        jdbc = get_jdbc_config()

        bronze_df = read_bronze(spark, jdbc)
        bronze_count = bronze_df.count()
        print(f"[flatten_accounts] bronze records read: {bronze_count}")

        flat = flatten_accounts(bronze_df)
        flat_count = flat.count()
        print(f"[flatten_accounts] accounts after explode: {flat_count}")

        deduped = deduplicate(flat)
        final_count = deduped.count()
        print(f"[flatten_accounts] accounts after dedup: {final_count}")

        write_silver(deduped, jdbc)
        print(f"[flatten_accounts] wrote {final_count} rows to silver.stg_accounts")

        return 0
    except Exception as e:
        print(f"[flatten_accounts] FAILED: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())