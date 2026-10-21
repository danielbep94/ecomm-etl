# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce — reset de las tablas de prueba autorizadas
# MAGIC Ejecuta este notebook manualmente con los jobs y otros escritores detenidos.
# MAGIC Vacía todas las filas de las cinco tablas Delta y recrea las cuatro tablas
# MAGIC Snowflake configuradas con el contrato actual. Estas tablas fueron designadas
# MAGIC como tablas de prueba por el usuario, aunque sus nombres incluyen PRD_MDP.
# MAGIC Conserva PIPELINE_RUNS, ecommerce_validation_results y los CSV en incoming/current.
# MAGIC El reset no es atómico entre tablas o sistemas; si falla, revisa el último
# MAGIC TABLE_NAME en los logs y vuelve a ejecutarlo hasta obtener nueve tablas vacías.
# MAGIC Necesita los permisos de carga y OWNERSHIP/CREATE OR REPLACE en Snowflake.
# MAGIC Antes del primer uso, instala las dependencias Python en este mismo cómputo:
# MAGIC `%pip install "snowflake-connector-python>=3.12,<5" "cryptography>=44,<51"`.
# MAGIC Después ejecuta `dbutils.library.restartPython()` en otra celda y vuelve a
# MAGIC ejecutar el notebook desde el principio. Clonar el repo no instala paquetes;
# MAGIC el conector Spark/JDBC tampoco instala el conector Python.

# COMMAND ----------

# MAGIC %run ../notebooks/_bootstrap

# COMMAND ----------

# Databricks injects these objects into the notebook namespace.
spark = globals()["spark"]
dbutils = globals()["dbutils"]
display = globals()["display"]

from uuid import uuid4

from conf.credentials import get_snowflake_options
from logs.logger import get_logger
from tests.reset_business_tables import empty_business_tables, reset_business_test_tables

_reset_id = str(uuid4())
_reset_logger = get_logger(_reset_id)
_delta_targets, _snowflake_targets = empty_business_tables(spark)
_target_rows = [("DELTA", table) for table, _, _ in _delta_targets]
_target_rows += [("SNOWFLAKE", table) for table in _snowflake_targets]
print(f"RESET_ID={_reset_id} | TARGET_TABLES={len(_target_rows)}")
display(spark.createDataFrame(_target_rows, "SYSTEM string, TABLE_NAME string"))

# COMMAND ----------

_reset_options = get_snowflake_options(dbutils)
_reset_results = reset_business_test_tables(spark, _reset_options, _reset_logger)
display(spark.createDataFrame(_reset_results, "SYSTEM string, TABLE_NAME string, ROWS_AFTER long"))
print(f"RESET_ID={_reset_id} | STATUS=SUCCEEDED | EMPTY_TABLES={len(_reset_results)}")
print("Vuelve a subir los cuatro CSV a incoming y ejecuta el job con ANIO/MES y FORCE_REPROCESS=true.")
print("El historial se conserva; FORCE_REPROCESS=true permite repetir periodos exitosos anteriores al reset.")
