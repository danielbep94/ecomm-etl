# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce — QA persistido
# MAGIC Usa `MES=ALL` para consultar el año seleccionado completo; `MES=1`–`12` consulta un mes.
# MAGIC MES vacío es inválido. Usa el mismo ANIO/MES de la carga para consultar sus datos.
# MAGIC Define `ANIO` y `MES`; los filtros opcionales vacíos consultan el periodo completo.
# MAGIC Este notebook lee Delta y Snowflake sin consultar `incoming` ni ejecutar cargas.
# MAGIC `FAIL_ON_QA_ERROR=true` hace fallar la tarea después de mostrar los hallazgos
# MAGIC cuando el estado final es `FAIL`. Las advertencias no hacen fallar el job.

# MAGIC `qa_joined_ecommerce_data` muestra las ventas de Snowflake cruzadas con
# MAGIC productos EDP/Waters y tiendas por RUN_ID + ID. Incluye atributos PRODUCT_*/STORE_*
# MAGIC y las banderas PRODUCT_MATCHED/STORE_MATCHED para localizar claves sin catálogo.
# MAGIC También concilia los tres catálogos Snowflake y verifica los joins por RUN_ID.
# MAGIC `qa_sales_summary` resume por ANIO/MES/CADENA/MARCA, con UNIDADES y VENTAS.

# COMMAND ----------

# Edit these variables manually; keep ANIO/MES equal to the loaded period.
ANIO = 2026
MES = "ALL"
FAIL_ON_QA_ERROR = True
RUN_ID = None
LOGICAL_DATASET = None
BU = None
ID_INTERNO = None
ID_STORE = None

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

# Databricks injects these objects into the notebook namespace.
spark = globals()["spark"]
dbutils = globals()["dbutils"]
display = globals()["display"]

from src.main import run_qa_validation
from src.transform import validate_parameters

ANIO, MES, FAIL_ON_QA_ERROR = validate_parameters(ANIO, MES, FAIL_ON_QA_ERROR)

# COMMAND ----------

qa_results = run_qa_validation(
    ANIO,
    MES,
    run_id=RUN_ID,
    logical_dataset=LOGICAL_DATASET,
    bu=BU,
    id_interno=ID_INTERNO,
    id_store=ID_STORE,
    spark=spark,
    dbutils=dbutils,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ventas cruzadas con los catálogos
# MAGIC Usa los filtros de periodo, BU, producto y tienda definidos arriba.

# COMMAND ----------

print("qa_joined_ecommerce_data")
display(qa_results["qa_joined_ecommerce_data"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen mensual por cadena y marca
# MAGIC VENTAS = SUM(MONTO). Las devoluciones se incluyen con su signo original.
# MAGIC Los totales de grupos con todas las medidas nulas permanecen nulos.

# COMMAND ----------

qa_sales_summary = qa_results["qa_sales_summary"]
print("qa_sales_summary")
display(qa_sales_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultados de validación

# COMMAND ----------

for _name, _dataframe in qa_results.items():
    if _name not in {"qa_joined_ecommerce_data", "qa_sales_summary"}:
        print(_name)
        display(_dataframe)

# COMMAND ----------

_final = qa_results["qa_final_status"].first()
if _final is None or _final.STATUS not in {"PASS", "PASS_WITH_WARNINGS", "FAIL"}:
    raise RuntimeError("QA no devolvió un estado final válido.")
print(f"ANIO={ANIO} | MES={MES} | QA_STATUS={_final.STATUS}")
if FAIL_ON_QA_ERROR and _final.STATUS == "FAIL":
    raise RuntimeError("QA detectó inconsistencias. Revisa las salidas anteriores antes de continuar.")
