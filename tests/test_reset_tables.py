"""Exercise manual test reset separately from production loading."""

import contextlib
import os
import uuid
from unittest.mock import Mock

import pytest

from conf import settings as C
from src import load as L
from src.run_tracking import PipelineError
from tests import reset_business_tables as R
from tests.snowflake_fakes import Cursor


class ResetCursor(Cursor):
    def execute(self, sql):
        super().execute(sql)
        if sql.startswith("SELECT COUNT(*) FROM"):
            self.result = (0,)
        return self


@pytest.mark.skipif(not os.environ.get("ECOMM_TEST_DELTA_JARS"), reason="Optional local Delta JARs not configured")
def test_explicit_test_reset_replaces_all_business_schemas_and_preserves_validation(spark, monkeypatch):
    from logs.logger import get_logger

    for key in [
        "CATALOG_EDP_TABLE",
        "CATALOG_WATERS_TABLE",
        "CATALOG_STORE_TABLE",
        "FACT_STAGING_TABLE",
        "FACT_CURATED_TABLE",
        "VALIDATION_RESULTS_TABLE",
    ]:
        monkeypatch.setattr(C, key, "default.reset_" + uuid.uuid4().hex)
    monkeypatch.setattr(L, "verify_hive_objects", Mock())
    delta, snowflake = R.empty_business_tables(spark)
    cursor, conn = ResetCursor(snowflake), Mock()
    conn.cursor.return_value = cursor

    @contextlib.contextmanager
    def connection(options):
        yield conn

    monkeypatch.setattr(L, "snowflake_connection", connection)
    tables = [table for table, _, _ in delta] + [C.VALIDATION_RESULTS_TABLE]
    try:
        # Legacy populated schemas exercise a reset that the regular loader refuses.
        for table in tables:
            spark.createDataFrame(
                [("keep history" if table == C.VALIDATION_RESULTS_TABLE else "old business data",)], "LEGACY string"
            ).write.format("delta").saveAsTable(table)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        try:
            result = R.reset_business_test_tables(spark, {}, get_logger("test-reset"))
        finally:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        assert len(result) == 9 and all(r["ROWS_AFTER"] == 0 for r in result)
        assert {r["TABLE_NAME"] for r in result} == set(tables[:-1]) | set(snowflake)
        for table, frame, _ in delta:
            actual = spark.table(table)
            assert actual.count() == 0
            assert {f.name: f.dataType for f in actual.schema} == {f.name: f.dataType for f in frame.schema}
            assert not {"BU", "SKU", "PRODUCT_SKU"} & set(actual.columns)
        assert [len(frame.columns) for _, frame, _ in delta] == [25, 25, 16, 15, 29]
        assert spark.table(C.FACT_STAGING_TABLE).schema["UNIDADES"].dataType.simpleString() == "double"
        assert spark.table(C.VALIDATION_RESULTS_TABLE).first().LEGACY == "keep history"
        replacements = [sql for sql in cursor.commands if sql.startswith("CREATE OR REPLACE TABLE")]
        assert len(replacements) == 4 and all(sql.endswith("COPY GRANTS") for sql in replacements)
        assert all(C.PIPELINE_RUN_HISTORY_TABLE not in sql for sql in cursor.commands)
    finally:
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_reset_failure_reports_partial_progress_and_never_modifies_delta(spark, monkeypatch):
    monkeypatch.setattr(L, "verify_hive_objects", Mock())
    _, snowflake = R.empty_business_tables(spark)
    cursor = ResetCursor(snowflake, fail="CREATE OR REPLACE TABLE " + C.SNOWFLAKE_CATALOG_TABLES["WATERS_CATALOG"])
    conn, logger = Mock(), Mock()
    conn.cursor.return_value = cursor

    @contextlib.contextmanager
    def connection(options):
        yield conn

    monkeypatch.setattr(L, "snowflake_connection", connection)
    monkeypatch.setattr(spark.catalog, "tableExists", lambda _: False)
    writer = Mock()
    monkeypatch.setattr(type(next(iter(snowflake.values()))), "write", property(lambda _: writer))
    with pytest.raises(PipelineError, match="after 1 verified tables") as captured:
        R.reset_business_test_tables(spark, {}, logger)
    assert captured.value.error_type == "TEST_RESET_ERROR" and "private" not in str(captured.value)
    writer.format.assert_not_called()
    assert logger.event.call_args.kwargs["COMPLETED_TABLES"] == 1


def test_reset_refuses_shared_history_targets_before_any_mutation(spark, monkeypatch):
    monkeypatch.setattr(L, "verify_hive_objects", Mock())
    monkeypatch.setattr(C, "FACT_STAGING_TABLE", "hive_metastore." + C.VALIDATION_RESULTS_TABLE)
    connection = Mock()
    monkeypatch.setattr(L, "snowflake_connection", connection)
    with pytest.raises(PipelineError, match="excluding shared history"):
        R.reset_business_test_tables(spark, {}, Mock())
    connection.assert_not_called()


@pytest.mark.parametrize("failure", ["dependency", "connection", "first_ddl"])
def test_reset_distinguishes_preflight_failure_from_unverified_first_ddl(monkeypatch, failure):
    spark, logger, frame = Mock(), Mock(), Mock(schema=[])
    spark.catalog.tableExists.return_value = False
    delta = [
        (t, frame, None)
        for t in [
            C.CATALOG_EDP_TABLE,
            C.CATALOG_WATERS_TABLE,
            C.CATALOG_STORE_TABLE,
            C.FACT_STAGING_TABLE,
            C.FACT_CURATED_TABLE,
        ]
    ]
    snowflake = dict.fromkeys(list(C.SNOWFLAKE_CATALOG_TABLES.values()) + [C.TARGET_SNOWFLAKE_TABLE], frame)
    monkeypatch.setattr(L, "verify_hive_objects", Mock())
    monkeypatch.setattr(R, "empty_business_tables", lambda _: (delta, snowflake))
    code = "RUNTIME_DEPENDENCY_ERROR" if failure == "dependency" else "SNOWFLAKE_CONNECTION_ERROR"
    cursor = Mock()
    cursor.execute.side_effect = RuntimeError("DDL failed")
    conn = Mock()
    conn.cursor.return_value = cursor

    @contextlib.contextmanager
    def connection(options):
        if failure != "first_ddl":
            raise PipelineError("Install dependencies or check the connection.", code)
        yield conn

    monkeypatch.setattr(L, "snowflake_connection", connection)
    with pytest.raises(PipelineError) as captured:
        R.reset_business_test_tables(spark, {}, logger)
    if failure == "first_ddl":
        assert "after 0 verified tables" in str(captured.value)
        assert "before changing any table" not in str(captured.value)
        cursor.execute.assert_called_once()
    else:
        assert "before changing any table" in str(captured.value)
        assert captured.value.error_type == code
        cursor.execute.assert_not_called()
        assert logger.event.call_args.kwargs["ERROR_TYPE"] == code
    frame.write.format.assert_not_called()
