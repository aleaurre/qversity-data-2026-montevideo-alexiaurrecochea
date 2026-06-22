# Databricks notebook source
# =============================================================================
# utils — helpers compartidos por los notebooks de flatten.
#
# Diferencia clave vs. la versión Postgres (spark/utils.py original):
#   - NO hay get_jdbc_config(): en Databricks no leemos/escribimos vía JDBC.
#     Spark lee y escribe tablas Delta directo en Unity Catalog.
#   - NO hay get_spark_session(): el runtime serverless ya provee `spark`.
#   - deduplicate_by_pk() se mantiene IDÉNTICA: misma lógica ROW_NUMBER()
#     OVER (PARTITION BY pk ORDER BY load_timestamp DESC, bronze_id DESC).
#
# Se importa con `%run ../flatten/utils` o como módulo según cómo lo orquestes.
# =============================================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def deduplicate_by_pk(df: DataFrame, pk: str) -> DataFrame:
    """
    Si el mismo record aparece en varias cargas de bronze, gana la más reciente
    (load_timestamp DESC); empates se rompen por bronze_id DESC. Idéntico al
    contrato del proyecto original.
    """
    w = Window.partitionBy(pk).orderBy(col("load_timestamp").desc(), col("bronze_id").desc())
    return (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )


def write_silver_raw(df: DataFrame, catalog: str, table: str) -> None:
    """
    Escribe a <catalog>.silver_raw.<table> como Delta en overwrite.
    Reemplaza el write JDBC con truncate=true del original: en Delta el
    overwrite reescribe los datos pero conserva la tabla (y sus grants UC),
    que es exactamente el comportamiento que buscaba `truncate=true`.
    """
    fqn = f"{catalog}.silver_raw.{table}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqn)
    )
    print(f"[flatten] wrote {df.count()} rows to {fqn}")
