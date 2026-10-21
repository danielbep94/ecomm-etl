# Databricks notebook source
# MAGIC %md
# MAGIC Shared repository startup for the job, QA and manual test reset.

# COMMAND ----------

import sys
from pathlib import Path

# Resolve the objects injected by the Databricks notebook runtime.
dbutils = globals()["dbutils"]

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
