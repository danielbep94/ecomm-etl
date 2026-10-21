# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce — QA persistido
# MAGIC Usa `MES=ALL` para reemplazar el año seleccionado completo; `MES=1`–`12` carga un mes.
# MAGIC MES vacío es inválido. Usa el mismo ANIO/MES de la carga para consultar sus datos.
# MAGIC Define `ANIO` y `MES`; los filtros opcionales vacíos consultan el periodo completo.
# MAGIC Este notebook lee Delta y Snowflake sin consultar `incoming` ni ejecutar cargas.
# MAGIC `FAIL_ON_QA_ERROR=true` hace fallar la tarea después de mostrar los hallazgos
# MAGIC cuando el estado final es `FAIL`. Las advertencias no hacen fallar el job.

# MAGIC `qa_joined_ecommerce_data` muestra las ventas de Snowflake cruzadas con
# MAGIC productos EDP/Waters y tiendas por RUN_ID + ID. Incluye atributos PRODUCT_*/STORE_*
# MAGIC y las banderas PRODUCT_MATCHED/STORE_MATCHED para localizar claves sin catálogo.
# MAGIC También concilia los tres catálogos Snowflake y verifica los joins por RUN_ID.

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
# MAGIC ## Resultados de validación

# COMMAND ----------

for _name, _dataframe in qa_results.items():
    if _name != "qa_joined_ecommerce_data":
        print(_name)
        display(_dataframe)

# COMMAND ----------

_final = qa_results["qa_final_status"].first()
if _final is None or _final.STATUS not in {"PASS", "PASS_WITH_WARNINGS", "FAIL"}:
    raise RuntimeError("QA no devolvió un estado final válido.")
print(f"ANIO={ANIO} | MES={MES} | QA_STATUS={_final.STATUS}")
if FAIL_ON_QA_ERROR and _final.STATUS == "FAIL":
    raise RuntimeError("QA detectó inconsistencias. Revisa las salidas anteriores antes de continuar.")
