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

# COMMAND ----------

import sys
from pathlib import Path

# Resolve the objects injected by the Databricks notebook runtime.
spark = globals()["spark"]
dbutils = globals()["dbutils"]
display = globals()["display"]

# Databricks Git folders may start in the notebook directory or the driver cwd.
_candidates = [Path.cwd()]
if globals().get("__file__"):
    _candidates.append(Path(__file__).resolve().parent)
try:
    _notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    if _notebook_path:
        _workspace_path = _notebook_path if _notebook_path.startswith("/Workspace/") else "/Workspace" + _notebook_path
        _candidates.append(Path(_workspace_path).parent)
except Exception:
    pass
_repo_root = None
for _candidate in _candidates:
    for _parent in (_candidate, *_candidate.parents):
        if (_parent / "src" / "main.py").is_file() and (_parent / "conf" / "settings.py").is_file():
            _repo_root = _parent
            break
    if _repo_root is not None:
        break
if _repo_root is None:
    raise RuntimeError("No se encontró la raíz de ecomm-etl. Ejecuta este notebook desde la Git folder clonada.")
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# COMMAND ----------

from uuid import uuid4

from conf.credentials import get_snowflake_options
from logs.logger import get_logger
from src.load import _empty_business_tables, reset_business_test_tables

_reset_id = str(uuid4())
_reset_logger = get_logger(_reset_id)
_delta_targets, _snowflake_targets = _empty_business_tables(spark)
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
