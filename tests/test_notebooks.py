from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src import main as M
from src.transform import ValidationError

ROOT = Path(__file__).resolve().parents[1]


class Widgets:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, name):
        return self.values[name]

    def text(self, name, default):
        self.values[name] = default

    def dropdown(self, name, default, choices):
        assert default in choices
        self.values[name] = default


def execute_notebook(name, values, *, path=None):
    source = ROOT / "notebooks" / name
    runtime = {
        "__name__": "__databricks_notebook__",
        "__file__": str(path or source),
        "dbutils": SimpleNamespace(widgets=Widgets(values)),
        "spark": Mock(),
        "display": Mock(),
    }
    exec(compile(source.read_text(), str(source), "exec"), runtime)
    return runtime


def test_job_passes_boolean_parameters_and_displays_summary(monkeypatch):
    run = Mock(
        return_value={
            "RUN_ID": "run-1",
            "STATUS": "SUCCEEDED",
            "SOURCE_ROWS": 10,
            "STAGING_ROWS": 2,
            "TRANSFORMED_ROWS": 2,
            "TARGET_ROWS_BEFORE": 0,
            "TARGET_ROWS_AFTER": 2,
        }
    )
    monkeypatch.setattr(M, "run_pipeline", run)
    runtime = execute_notebook("job_ecommerce_danone.py", {"ANIO": "2025", "MES": "7", "FORCE_REPROCESS": "false"})
    assert run.call_args.args == (2025, 7, False)
    assert run.call_args.kwargs == {"spark": runtime["spark"], "dbutils": runtime["dbutils"]}
    run.assert_called_once()
    runtime["display"].assert_called_once()


def test_blank_job_parameters_do_not_start_pipeline(monkeypatch):
    run = Mock()
    monkeypatch.setattr(M, "run_pipeline", run)
    with pytest.raises(ValidationError, match="ANIO"):
        execute_notebook("job_ecommerce_danone.py", {})
    run.assert_not_called()


def test_notebook_bootstrap_reports_missing_clone(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="raíz de ecomm-etl"):
        execute_notebook("job_ecommerce_danone.py", {}, path=tmp_path / "isolated" / "job.py")


@pytest.mark.parametrize(
    "status,fail_flag,raises",
    [("PASS", "true", False), ("PASS_WITH_WARNINGS", "true", False), ("FAIL", "false", False), ("FAIL", "true", True)],
)
def test_qa_displays_results_and_can_fail_task(monkeypatch, status, fail_flag, raises):
    output = {
        name: Mock()
        for name in [
            "qa_pipeline_history",
            "qa_validation_results",
            "qa_period_detection",
            "qa_catalog_duplicate_summary",
            "qa_catalog_duplicate_detail",
            "qa_cross_catalog_collisions",
            "qa_product_catalog_coverage",
            "qa_store_catalog_coverage",
            "qa_fact_duplicate_summary",
            "qa_negative_values",
            "qa_join_multiplication_check",
            "qa_source_delta_reconciliation",
            "qa_delta_snowflake_reconciliation",
            "qa_final_status",
            "qa_snowflake_catalog_reconciliation",
            "qa_snowflake_star_relationships",
            "qa_joined_ecommerce_data",
        ]
    }
    output["qa_final_status"].first.return_value = SimpleNamespace(STATUS=status)
    qa = Mock(return_value=output)
    run = Mock(side_effect=AssertionError("QA must not run an ETL"))
    monkeypatch.setattr(M, "run_qa_validation", qa)
    monkeypatch.setattr(M, "run_pipeline", run)
    values = {"ANIO": "2025", "MES": "7", "FAIL_ON_QA_ERROR": fail_flag}
    if raises:
        with pytest.raises(RuntimeError, match="QA detectó inconsistencias"):
            execute_notebook("qa_ecommerce_danone.py", values)
    else:
        runtime = execute_notebook("qa_ecommerce_danone.py", values)
        assert runtime["display"].call_count == 17
        assert runtime["display"].call_args_list[0].args == (output["qa_joined_ecommerce_data"],)
    qa.assert_called_once()
    assert qa.call_args.args == (2025, 7)
    assert qa.call_args.kwargs["run_id"] is None and qa.call_args.kwargs["logical_dataset"] is None
    run.assert_not_called()


def test_qa_forwards_optional_filters(monkeypatch):
    result = Mock()
    result.first.return_value = SimpleNamespace(STATUS="PASS")
    qa = Mock(return_value={"qa_final_status": result, "qa_joined_ecommerce_data": Mock()})
    monkeypatch.setattr(M, "run_qa_validation", qa)
    execute_notebook(
        "qa_ecommerce_danone.py",
        {
            "ANIO": "2025",
            "MES": "7",
            "RUN_ID": "run-1",
            "LOGICAL_DATASET": "ECOMMERCE_FACT",
            "BU": "EDP",
            "ID_INTERNO": "123",
            "ID_STORE": "456",
        },
    )
    assert {key: qa.call_args.kwargs[key] for key in ["run_id", "logical_dataset", "bu", "id_interno", "id_store"]} == {
        "run_id": "run-1",
        "logical_dataset": "ECOMMERCE_FACT",
        "bu": "EDP",
        "id_interno": "123",
        "id_store": "456",
    }
