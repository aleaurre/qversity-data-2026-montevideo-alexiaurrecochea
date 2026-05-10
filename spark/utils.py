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