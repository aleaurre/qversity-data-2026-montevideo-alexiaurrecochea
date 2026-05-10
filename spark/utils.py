"""
Utilities compartidos por todos los jobs PySpark.

La función principal es `get_spark_session`, que centraliza la configuración
de la JVM, el classpath del driver JDBC de Postgres, y los parámetros de
conexión al warehouse. Cualquier script de Spark del proyecto debería
obtener su sesión desde acá.
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession


# Path absoluto al driver JDBC dentro del container de Airflow.
# Lo dejamos como constante a nivel módulo porque es invariante del entorno
# (lo fija el Dockerfile.airflow).
JDBC_DRIVER_PATH = "/opt/spark/jars/postgresql-42.7.3.jar"


def get_spark_session(app_name: str = "qversity-spark") -> SparkSession:
    """
    Crea (o recupera) una SparkSession lista para hablar con Postgres por JDBC.

    Notas de configuración:
    - `spark.jars` registra el JAR del driver para que esté disponible en el
      classpath del driver y de los executors. Con `local[*]` driver y executor
      son el mismo proceso, pero seteamos ambos por consistencia y por si en
      el futuro se mueve a un cluster real.
    - `spark.sql.session.timeZone=UTC` evita que Spark "ayude" convirtiendo
      timestamps a la zona horaria del sistema. Queremos UTC en todo el
      pipeline y dejar que la capa BI haga la conversión a local si hace falta.
    - `spark.sql.shuffle.partitions=4` baja el default (200) porque corremos
      en `local[*]` con un dataset chico; 200 particiones generan miles de
      tareas mínimas y desperdician overhead.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars", JDBC_DRIVER_PATH)
        .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH)
        .config("spark.executor.extraClassPath", JDBC_DRIVER_PATH)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def get_jdbc_config() -> dict:
    """
    Construye el dict de propiedades JDBC desde variables de entorno.

    Las credenciales NO se hardcodean: se leen de las mismas env vars que ya
    existen en `.env` desde el día 1 (POSTGRES_USER, POSTGRES_PASSWORD, etc.).
    Esto mantiene el script reusable entre dev/prod y evita commits de
    credenciales por accidente.
    """
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "qversity_warehouse")
    user = os.environ["POSTGRES_USER"]       # explicitamente requerido
    password = os.environ["POSTGRES_PASSWORD"]  # explicitamente requerido

    return {
        "url": f"jdbc:postgresql://{host}:{port}/{db}",
        "properties": {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
        },
    }

# ---------------------------------------------------------------------------
# Dedup helper — shared by all flatteners.
#
# Centralizamos la lógica de dedup para no repetir el window function en
# cada script. Cada flattener llama a esto con su PK natural:
#   - flatten_accounts.py     → "account_id"
#   - flatten_transactions.py → "transaction_id"
#   - flatten_loans.py        → "loan_id"
#
# Criterio: si bronze tiene múltiples loads del mismo dataset (cosa esperable
# en desarrollo, donde el DAG corre varias veces), la misma PK del array
# puede aparecer en más de un `bronze_id`. Nos quedamos con la versión MÁS
# RECIENTE — mayor `load_timestamp` — que es la semántica correcta para un
# staging que alimenta silver/dbt.
#
# Casos degenerados:
#   - dos versiones con el mismo load_timestamp (DAG corrió dos veces en el
#     mismo segundo): Spark elige una arbitrariamente. No es determinista
#     pero los datos son idénticos en ese caso, así que da igual.
#   - PK nula: row_number() la trata como un grupo aparte y la conserva.
#     No filtramos nulls acá; si aparecen, es un bug de upstream que dbt
#     debe atrapar con un test not_null.
# ---------------------------------------------------------------------------
def deduplicate_by_pk(df, pk_column: str):
    """
    Deduplica un DataFrame staging por su PK natural, quedándose con la
    versión de mayor `load_timestamp`.

    Args:
        df: DataFrame con columnas `pk_column` y `load_timestamp`.
        pk_column: nombre de la PK natural del array (ej: "account_id").

    Returns:
        DataFrame con una fila por valor único de `pk_column`.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, row_number

    w = Window.partitionBy(pk_column).orderBy(col("load_timestamp").desc())

    return (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )