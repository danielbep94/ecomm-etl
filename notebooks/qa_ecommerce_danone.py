# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Finance DANGO — QA persistido
# MAGIC Define `ANIO` y `MES`; los filtros opcionales vacíos consultan el periodo completo.
# MAGIC Este notebook lee Delta y Snowflake sin consultar `incoming` ni ejecutar cargas.
# MAGIC `FAIL_ON_QA_ERROR=true` hace fallar la tarea después de mostrar los hallazgos
# MAGIC cuando el estado final es `FAIL`. Las advertencias no hacen fallar el job.

# MAGIC También concilia los tres catálogos Snowflake y verifica los joins por RUN_ID.

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

from src.main import run_qa_validation
from src.run_tracking import get_widget_value
from src.transform import validate_parameters

for _name in ("ANIO", "MES", "RUN_ID", "LOGICAL_DATASET", "BU", "ID_INTERNO", "ID_STORE"):
    try:
        dbutils.widgets.get(_name)
    except Exception:
        dbutils.widgets.text(_name, "")
try:
    dbutils.widgets.get("FAIL_ON_QA_ERROR")
except Exception:
    dbutils.widgets.dropdown("FAIL_ON_QA_ERROR", "true", ["true", "false"])

ANIO, MES, FAIL_ON_QA_ERROR = validate_parameters(
    get_widget_value(dbutils, "ANIO"),
    get_widget_value(dbutils, "MES"),
    get_widget_value(dbutils, "FAIL_ON_QA_ERROR", "true"),
)

# COMMAND ----------

qa_results = run_qa_validation(
    ANIO,
    MES,
    run_id=get_widget_value(dbutils, "RUN_ID"),
    logical_dataset=get_widget_value(dbutils, "LOGICAL_DATASET"),
    bu=get_widget_value(dbutils, "BU"),
    id_interno=get_widget_value(dbutils, "ID_INTERNO"),
    id_store=get_widget_value(dbutils, "ID_STORE"),
    spark=spark,
    dbutils=dbutils,
)

# COMMAND ----------

for _name, _dataframe in qa_results.items():
    print(_name)
    display(_dataframe)

# COMMAND ----------

_final = qa_results["qa_final_status"].first()
if _final is None or _final.STATUS not in {"PASS", "PASS_WITH_WARNINGS", "FAIL"}:
    raise RuntimeError("QA no devolvió un estado final válido.")
print(f"ANIO={ANIO} | MES={MES} | QA_STATUS={_final.STATUS}")
if FAIL_ON_QA_ERROR and _final.STATUS == "FAIL":
    raise RuntimeError("QA detectó inconsistencias. Revisa las salidas anteriores antes de continuar.")
