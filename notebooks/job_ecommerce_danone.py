# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce — job por mes o año
# MAGIC Usa `MES=ALL` para reemplazar el año seleccionado completo; `MES=1`–`12` carga un mes.
# MAGIC MES vacío es inválido. Para repetir un periodo exitoso, usa FORCE_REPROCESS=true.
# MAGIC Define `ANIO`, `MES` y `FORCE_REPROCESS` antes de ejecutar todas las celdas.
# MAGIC El cómputo necesita las dependencias del proyecto, el conector Spark de
# MAGIC Snowflake y acceso al scope documentado en README. No se instalan librerías
# MAGIC ni se repite la inicialización de Hive/DBFS desde este notebook.
# MAGIC Tras validar el lote, el ETL crea si faltan los tres catálogos y la fact
# MAGIC Snowflake. El DDL manual está en `conf/snowflake_business_tables.sql`.
# MAGIC Las librerías Python instaladas solo en el notebook de reset no se comparten
# MAGIC con esta tarea. Instala `snowflake-connector-python>=3.12,<5` y
# MAGIC `cryptography>=44,<51` como librerías de esta tarea, o mediante %pip en este
# MAGIC mismo notebook y reinicia Python antes de volver a ejecutar desde el principio.
# MAGIC El preflight valida el import antes de escribir tablas y registra la versión
# MAGIC cargada y las rutas de los módulos para identificar código antiguo en caché.

# COMMAND ----------

# Edit these variables manually before running the notebook.
ANIO = 2026
MES = "ALL"  # Use 1–12 for a month, or "ALL" for the selected year.
FORCE_REPROCESS = True

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

# Databricks injects these objects into the notebook namespace.
spark = globals()["spark"]
dbutils = globals()["dbutils"]
display = globals()["display"]

from src.main import run_pipeline
from src.transform import validate_parameters

# Fail before starting a pipeline attempt if notebook parameters are incomplete.
ANIO, MES, FORCE_REPROCESS = validate_parameters(ANIO, MES, FORCE_REPROCESS)

# COMMAND ----------

# Exceptions propagate so Databricks marks the task as failed when appropriate.
run_record = run_pipeline(ANIO, MES, FORCE_REPROCESS, spark=spark, dbutils=dbutils)

# COMMAND ----------

print(f"RUN_ID={run_record['RUN_ID']} | STATUS={run_record['STATUS']} | ANIO={ANIO} | MES={MES}")
display(
    spark.createDataFrame(
        [
            (
                run_record["RUN_ID"],
                run_record["STATUS"],
                ANIO,
                None if MES == "ALL" else MES,
                run_record["SOURCE_ROWS"],
                run_record["STAGING_ROWS"],
                run_record["TRANSFORMED_ROWS"],
                run_record["TARGET_ROWS_BEFORE"],
                run_record["TARGET_ROWS_AFTER"],
            )
        ],
        "RUN_ID string, STATUS string, ANIO int, MES int, SOURCE_ROWS long, STAGING_ROWS long, "
        "TRANSFORMED_ROWS long, TARGET_ROWS_BEFORE long, TARGET_ROWS_AFTER long",
    )
)
