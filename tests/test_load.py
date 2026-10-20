import contextlib
import os
import uuid
from decimal import Decimal
from unittest.mock import Mock

import pytest
from pyspark.sql import functions as F

from conf import settings as C
from src import load as L
from src.run_tracking import PipelineError


@pytest.fixture
def monthly(spark):
    return spark.createDataFrame(
        [(2025, 7, 1, 10.0), (2025, 7, 2, -2.5)], "ANIO int, MES int, ID_INTERNO long, MONTO double"
    )


def test_exact_reconciliation_rejects_equal_totals_but_different_records(monthly):
    assert L.frame_metrics(monthly) == {"rows": 2, "amount": Decimal("7.500000")}
    assert L.multiset_difference(monthly, monthly) == 0
    changed = monthly.withColumn("ID_INTERNO", F.col("ID_INTERNO") + 100)
    assert L.compare_metrics(L.frame_metrics(monthly), L.frame_metrics(changed))
    assert L.multiset_difference(monthly, changed) == 4


def test_delta_period_isolation_and_replace_where(spark, monthly, monkeypatch):
    writer = Mock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    monkeypatch.setattr(type(monthly), "write", property(lambda self: writer))
    monkeypatch.setattr(L, "ensure_delta_table", Mock())
    L.load_monthly_delta(spark, C.FACT_CURATED_TABLE, monthly, 2025, 7)
    writer.option.assert_called_with("replaceWhere", "ANIO = 2025 AND MES = 7")
    writer.mode.assert_called_once_with("overwrite")
    with pytest.raises(PipelineError):
        L.load_monthly_delta(spark, C.FACT_CURATED_TABLE, monthly.withColumn("MES", F.lit(8)), 2025, 7)
    assert writer.saveAsTable.call_count == 1


class Cursor:
    def __init__(self, fail=False):
        self.commands, self.result, self.fail = [], None, fail

    def execute(self, sql):
        self.commands.append(sql)
        if sql.startswith("INSERT") and self.fail:
            raise RuntimeError("secret=private")
        if sql.startswith("SELECT COUNT(*),"):
            self.result = (2, Decimal("7.5"))
        elif sql.startswith("SELECT COUNT(*) FROM (("):
            self.result = (0,)
        return self

    def fetchone(self):
        return self.result

    def close(self):
        pass


@pytest.mark.parametrize("fail", [False, True])
def test_snowflake_transaction_and_rollback(spark, monthly, monkeypatch, fail):
    cursor = Cursor(fail)
    conn = Mock()
    conn.cursor.return_value = cursor

    @contextlib.contextmanager
    def connection(options):
        yield conn

    monkeypatch.setattr(L, "snowflake_connection", connection)
    writer = Mock()
    writer.format.return_value = writer
    writer.options.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    monkeypatch.setattr(type(monthly), "write", property(lambda self: writer))
    monkeypatch.setattr(L, "snowflake_read", lambda *args: monthly)
    observed = {}
    if fail:
        with pytest.raises(PipelineError) as captured:
            L.load_snowflake_period(spark, {}, monthly, 2025, 7, "11111111-1111-1111-1111-111111111111", observed)
        assert "private" not in str(captured.value)
    else:
        result = L.load_snowflake_period(spark, {}, monthly, 2025, 7, "11111111-1111-1111-1111-111111111111", observed)
        assert result["reconciliation_status"] == "PASS"
    assert observed["target_rows_before"] == 2
    assert cursor.commands.count("BEGIN") == 1
    assert ("ROLLBACK" in cursor.commands) == fail
    assert ("COMMIT" in cursor.commands) != fail
    assert any(sql.startswith("DELETE FROM") and sql.endswith("WHERE ANIO=2025 AND MES=7") for sql in cursor.commands)
    assert not any("PIPELINE_RUNS" in sql for sql in cursor.commands)
    assert any(sql.startswith("DROP TABLE IF EXISTS") for sql in cursor.commands)


def test_idempotency_classification(spark):
    empty = spark.createDataFrame([], "STATUS string")
    failed = spark.createDataFrame([("FAILED",)], "STATUS string")
    success = spark.createDataFrame([("SUCCEEDED",), ("FAILED",)], "STATUS string")
    assert L.classify_run(empty, False) == "FIRST_LOAD"
    assert L.classify_run(failed, False) == "RETRY"
    assert L.classify_run(success, False) == "DUPLICATE_PERIOD"
    assert L.classify_run(success, True) == "REPROCESS"


def test_unsafe_identifiers_rejected():
    with pytest.raises(ValueError):
        L.sql_identifier("DB.SCHEMA.X; DROP TABLE USERS")


@pytest.mark.skipif(not os.environ.get("ECOMM_TEST_DELTA_JARS"), reason="Optional local Delta JARs not configured")
def test_real_delta_catalog_snapshot_history_and_same_run_idempotency(spark, catalogs):
    from src import transform as V
    from src.run_tracking import utc_now

    edp, _, _ = catalogs
    table = "default.catalog_test_" + uuid.uuid4().hex

    def snapshot(frame, run_id):
        metadata = {
            "RUN_ID": run_id,
            "LOGICAL_DATASET": "EDP_CATALOG",
            "ORIGINAL_FILE_NAME": "edp.csv",
            "SOURCE_PATH": "file:/test/edp.csv",
            "FILE_FINGERPRINT": "a" * 64,
            "FILE_YEAR": None,
        }
        return V.add_operational_metadata(frame, metadata, 2025, 7, catalog_bu="EDP", timestamp=utc_now())

    first = snapshot(edp, "first")
    second = snapshot(edp.withColumn("NOMBRE", F.lit("Updated")), "second")
    try:
        L.load_catalog_snapshot(spark, table, first)
        L.load_catalog_snapshot(spark, table, second)
        L.load_catalog_snapshot(spark, table, second)
        result = spark.table(table)
        assert result.count() == 2
        active = result.where("IS_ACTIVE = true").collect()
        assert len(active) == 1 and active[0].RUN_ID == "second" and active[0].NOMBRE == "Updated"
        assert result.where("RUN_ID='first'").first().VALID_TO_UTC is not None
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.skipif(not os.environ.get("ECOMM_TEST_DELTA_JARS"), reason="Optional local Delta JARs not configured")
def test_real_delta_monthly_replacement_preserves_other_month(spark, monthly):
    table = "default.fact_test_" + uuid.uuid4().hex
    try:
        L.load_monthly_delta(spark, table, monthly.withColumn("MES", F.lit(6)), 2025, 6)
        L.load_monthly_delta(spark, table, monthly, 2025, 7)
        replacement = monthly.where("ID_INTERNO=1").withColumn("MONTO", F.lit(99.0))
        L.load_monthly_delta(spark, table, replacement, 2025, 7)
        assert spark.table(table).where("MES=6").count() == 2
        july = spark.table(table).where("MES=7").collect()
        assert len(july) == 1 and july[0].MONTO == 99.0
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
