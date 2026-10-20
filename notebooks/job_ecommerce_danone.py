# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Finance DANGO — job mensual
# MAGIC Define `ANIO`, `MES` y `FORCE_REPROCESS` antes de ejecutar todas las celdas.
# MAGIC El cómputo necesita las dependencias del proyecto, el conector Spark de
# MAGIC Snowflake y acceso al scope documentado en README. No se instalan librerías
# MAGIC ni se repite la inicialización de Hive/DBFS desde este notebook.
# MAGIC Tras validar el lote, el ETL crea si faltan los tres catálogos y la fact
# MAGIC Snowflake. El DDL manual está en `conf/snowflake_business_tables.sql`.

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

from src.main import run_pipeline
from src.run_tracking import get_widget_value
from src.transform import validate_parameters

for _name in ("ANIO", "MES"):
    try:
        dbutils.widgets.get(_name)
    except Exception:
        dbutils.widgets.text(_name, "")
try:
    dbutils.widgets.get("FORCE_REPROCESS")
except Exception:
    dbutils.widgets.dropdown("FORCE_REPROCESS", "false", ["false", "true"])

# Fail before starting a pipeline attempt if notebook parameters are incomplete.
ANIO, MES, FORCE_REPROCESS = validate_parameters(
    get_widget_value(dbutils, "ANIO"),
    get_widget_value(dbutils, "MES"),
    get_widget_value(dbutils, "FORCE_REPROCESS", "false"),
)

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
                MES,
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
