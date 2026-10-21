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


@pytest.fixture
def star_payloads(spark, catalogs):
    from src import transform as V
    from src.run_tracking import utc_now

    run_id = "11111111-1111-1111-1111-111111111111"
    metadata = {
        "RUN_ID": run_id,
        "LOGICAL_DATASET": "ECOMMERCE_FACT",
        "ORIGINAL_FILE_NAME": "fact.csv",
        "SOURCE_PATH": "file:/test/fact.csv",
        "FILE_FINGERPRINT": "a" * 64,
        "FILE_YEAR": 2025,
    }
    source = spark.createDataFrame(
        [(202507, 1, 1, 1.5, 10.0, "EDP"), (202507, 1, 2, None, -2.5, "WATERS")],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double, BU string",
    )
    fact = V.star_fact(V.add_operational_metadata(V.normalize_bu(source), metadata, 2025, 7, timestamp=utc_now()))
    dimensions = {}
    for dataset, frame in zip(C.SNOWFLAKE_CATALOG_TABLES, catalogs):
        current = V.add_operational_metadata(
            frame,
            {**metadata, "LOGICAL_DATASET": dataset},
            2025,
            7,
            catalog_bu="EDP" if dataset == "EDP_CATALOG" else "WATERS" if dataset == "WATERS_CATALOG" else None,
            timestamp=utc_now(),
        )
        current = (
            current.withColumn("IS_ACTIVE", F.lit(True))
            .withColumn("VALID_FROM_UTC", F.col("LOAD_TIMESTAMP_UTC"))
            .withColumn("VALID_TO_UTC", F.lit(None).cast("timestamp"))
        )
        old = (
            current.withColumn("RUN_ID", F.lit("older-run"))
            .withColumn("IS_ACTIVE", F.lit(False))
            .withColumn("VALID_TO_UTC", F.current_timestamp())
        )
        dimensions[dataset] = old.unionByName(current)
    return fact, dimensions, run_id


class Cursor:
    def __init__(self, payloads, fail=None, incompatible=False):
        self.commands, self.result, self.fail = [], None, fail
        self.payloads, self.incompatible = payloads, incompatible

    def execute(self, sql):
        self.commands.append(sql)
        if self.fail and self.fail != "relationships" and sql.startswith(self.fail):
            raise RuntimeError("secret=private")
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            table = next(table for table in self.payloads if "TABLE_NAME='" + table.split(".")[-1].upper() + "'" in sql)
            rows = []
            for field in self.payloads[table].schema:
                kind = L._sql_type(field.dataType)
                rows.append(
                    (
                        field.name,
                        "NUMBER"
                        if kind.startswith("NUMBER")
                        else "TEXT"
                        if kind == "VARCHAR"
                        else "FLOAT"
                        if kind == "DOUBLE"
                        else kind,
                        38,
                        0,
                    )
                )
            self.result = rows[:-1] if self.incompatible else rows
        elif sql.startswith("SELECT COUNT(*) FROM (("):
            self.result = (0,)
        elif sql.startswith("SELECT COUNT(*) AS JOINED_ROWS"):
            self.result = (2, 1 if self.fail == "relationships" else 0, 0)
        elif sql.startswith("SELECT COUNT(*),"):
            self.result = (2, Decimal("7.5"))
        elif sql.startswith("SELECT COUNT(*) FROM"):
            self.result = (2,)
        return self

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result

    def close(self):
        pass


@pytest.mark.parametrize(
    "scope,fail",
    [
        (7, None),
        (7, "INSERT"),
        (7, "MERGE INTO PRD_MDP.MDP_STG.CATALOG_WATERS"),
        (7, "relationships"),
        (7, "CREATE TRANSIENT TABLE PRD_MDP.MDP_STG.CATALOG_WATERS"),
        ("ALL", None),
        ("ALL", "INSERT"),
    ],
)
def test_snowflake_transaction_and_rollback(spark, star_payloads, monkeypatch, fail, scope):
    fact, catalogs, run_id = star_payloads
    if scope == "ALL":
        fact = fact.withColumn("MES", F.when(F.col("NORMALIZED_BU") == "WATERS", 8).otherwise(7)).withColumn(
            "TIME_ID", F.when(F.col("NORMALIZED_BU") == "WATERS", F.lit(202508)).otherwise(F.col("TIME_ID"))
        )
    payloads = {C.SNOWFLAKE_CATALOG_TABLES[k]: v for k, v in catalogs.items()}
    payloads[C.TARGET_SNOWFLAKE_TABLE] = fact
    cursor = Cursor(payloads, fail)
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
    monkeypatch.setattr(type(fact), "write", property(lambda self: writer))
    monkeypatch.setattr(
        L,
        "snowflake_read",
        lambda _, options, query: next(
            frame for table, frame in payloads.items() if "FROM " + table + "_STAGE_" in query
        ),
    )
    observed = {}
    if fail:
        with pytest.raises(PipelineError) as captured:
            L.load_snowflake_period(spark, {}, fact, 2025, scope, run_id, observed, catalogs)
        assert "private" not in str(captured.value)
    else:
        result = L.load_snowflake_period(spark, {}, fact, 2025, scope, run_id, observed, catalogs)
        assert result["reconciliation_status"] == "PASS"
        assert len(result["catalog_metrics"]) == 3
        assert result["star_relationships"]["status"] == "PASS"
        assert sum(sql.startswith("MERGE INTO") for sql in cursor.commands) == 3
        assert sum(sql.startswith("DELETE FROM") for sql in cursor.commands) == 1
        assert sum(sql.startswith("INSERT INTO") for sql in cursor.commands) == 1
    assert observed["target_rows_before"] == 2
    if fail and fail.startswith("CREATE TRANSIENT"):
        assert "BEGIN" not in cursor.commands
        assert not any(sql.startswith(("MERGE", "DELETE", "INSERT", "COMMIT")) for sql in cursor.commands)
        assert sum(sql.startswith("DROP TABLE IF EXISTS") for sql in cursor.commands) == 1
        return
    assert cursor.commands.count("BEGIN") == 1
    assert ("ROLLBACK" in cursor.commands) == bool(fail)
    assert ("COMMIT" in cursor.commands) != bool(fail)
    assert sum(sql.startswith("CREATE TABLE IF NOT EXISTS") for sql in cursor.commands) == 4
    assert sum(sql.startswith("DROP TABLE IF EXISTS") for sql in cursor.commands) == 4
    assert not any("PIPELINE_RUNS" in sql for sql in cursor.commands)
    transaction = cursor.commands[cursor.commands.index("BEGIN") + 1 :]
    transaction = transaction[: next(i for i, sql in enumerate(transaction) if sql in {"COMMIT", "ROLLBACK"})]
    assert not any(sql.startswith(("CREATE", "ALTER", "DROP")) for sql in transaction)
    assert all("RUN_ID=s.RUN_ID" in sql for sql in transaction if sql.startswith("MERGE"))
    delete = (
        next(sql for sql in transaction if sql.startswith("DELETE"))
        if not (fail and fail.startswith("MERGE"))
        else None
    )
    if delete:
        expected_where = "WHERE ANIO=2025" if scope == "ALL" else "WHERE ANIO=2025 AND MES=7"
        assert delete.endswith(expected_where) and "FACT_" in delete
    assert observed["remaining_staging_tables"] == []


def test_existing_incompatible_schema_is_not_replaced(star_payloads):
    fact, _, _ = star_payloads
    cursor = Cursor({C.TARGET_SNOWFLAKE_TABLE: fact}, incompatible=True)
    with pytest.raises(PipelineError, match="incompatible schema"):
        L.ensure_snowflake_table(cursor, C.TARGET_SNOWFLAKE_TABLE, fact)
    assert not any(sql.startswith(("ALTER", "DROP", "DELETE", "INSERT")) for sql in cursor.commands)


def test_duplicate_dimension_keys_rejected(star_payloads):
    _, catalogs, run_id = star_payloads
    catalog = catalogs["EDP_CATALOG"]
    with pytest.raises(PipelineError, match="duplicate versioned"):
        L._validate_catalog_history(catalog.unionByName(catalog), "EDP_CATALOG", run_id)


def test_historical_star_joins_and_fact_projection(star_payloads):
    from src import transform as V

    fact, catalogs, _ = star_payloads
    fact = fact.withColumn("RUN_ID", F.lit("older-run"))
    projected = V.star_fact(fact.withColumn("PRODUCT_NOMBRE", F.lit("description")))
    assert len(projected.columns) == 15 and "PRODUCT_NOMBRE" not in projected.columns
    metrics = V.versioned_coverage_metrics(projected, *catalogs.values())
    assert metrics["FACT_ROWS"] == metrics["ROWS_AFTER_PRODUCT_JOIN"] == metrics["ROWS_AFTER_STORE_JOIN"] == 2
    assert metrics["UNMATCHED_PRODUCT_ROWS"] == metrics["UNMATCHED_STORE_ROWS"] == 0
    bad = V.versioned_coverage_metrics(projected.withColumn("RUN_ID", F.lit("missing-run")), *catalogs.values())
    assert bad["UNMATCHED_PRODUCT_ROWS"] == bad["UNMATCHED_STORE_ROWS"] == 2


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


def test_published_ddl_matches_loaded_star_schemas(star_payloads):
    import re
    from pathlib import Path

    fact, catalogs, _ = star_payloads
    from src.transform import star_fact

    fact = star_fact(fact)
    ddl = (Path(__file__).resolve().parents[1] / "conf" / "snowflake_business_tables.sql").read_text()
    payloads = {C.SNOWFLAKE_CATALOG_TABLES[k]: frame for k, frame in catalogs.items()}
    payloads[C.TARGET_SNOWFLAKE_TABLE] = fact
    assert ddl.count("CREATE TABLE IF NOT EXISTS") == 4
    for table, frame in payloads.items():
        match = re.search(re.escape(table) + r" \(\n(.*?)\n\);", ddl, re.DOTALL)
        assert match, table
        fields = [line.strip().rstrip(",") for line in match[1].splitlines()]
        assert fields == [field.name + " " + L._sql_type(field.dataType) for field in frame.schema]


@pytest.mark.parametrize("dataset", ["EDP_CATALOG", "WATERS_CATALOG"])
def test_history_rejects_duplicate_product_codes_with_distinct_ids(star_payloads, dataset):
    _, catalogs, run_id = star_payloads
    history = catalogs[dataset]
    extra = history.where(F.col("RUN_ID") == run_id).withColumn("ID_INTERNO", F.col("ID_INTERNO") + 100)
    with pytest.raises(PipelineError, match="UPC"):
        L._validate_catalog_history(history.unionByName(extra), dataset, run_id)


def test_qa_returns_actual_historical_joined_sales(star_payloads):
    from src import transform as V

    fact, catalogs, _ = star_payloads
    fact = fact.withColumn("RUN_ID", F.lit("older-run")).withColumn("ID_INTERNO", F.lit(1).cast("long"))
    edp = catalogs["EDP_CATALOG"].withColumn(
        "NOMBRE", F.when(F.col("RUN_ID") == "older-run", "EDP historical").otherwise("EDP current")
    )
    waters = (
        catalogs["WATERS_CATALOG"]
        .withColumn("ID_INTERNO", F.lit(1).cast("long"))
        .withColumn("NOMBRE", F.when(F.col("RUN_ID") == "older-run", "Waters historical").otherwise("Waters current"))
    )
    stores = catalogs["STORE_CATALOG"].withColumn(
        "CADENA", F.when(F.col("RUN_ID") == "older-run", "Historical chain").otherwise("Current chain")
    )
    joined = V.join_star_fact(fact, edp, waters, stores)
    rows = {row.NORMALIZED_BU: row for row in joined.collect()}
    assert joined.count() == fact.count() == 2
    assert rows["EDP"].PRODUCT_NOMBRE == "EDP historical"
    assert rows["WATERS"].PRODUCT_NOMBRE == "Waters historical"
    assert all(
        row.STORE_CADENA == "Historical chain" and row.PRODUCT_MATCHED and row.STORE_MATCHED for row in rows.values()
    )
    assert L.frame_metrics(joined) == L.frame_metrics(fact)


def test_qa_join_keeps_sales_with_missing_or_duplicated_catalog_keys(star_payloads):
    from src import transform as V

    fact, catalogs, run_id = star_payloads
    edp, waters, stores = catalogs.values()
    no_match = V.join_star_fact(
        fact, edp, waters.where(F.col("RUN_ID") != run_id), stores.where(F.col("RUN_ID") != run_id)
    )
    rows = {row.NORMALIZED_BU: row for row in no_match.collect()}
    assert len(rows) == 2
    assert rows["EDP"].PRODUCT_MATCHED
    assert not rows["WATERS"].PRODUCT_MATCHED and rows["WATERS"].PRODUCT_NOMBRE is None
    assert all(not row.STORE_MATCHED and row.STORE_CADENA is None for row in rows.values())
    duplicated = edp.unionByName(edp.where(F.col("RUN_ID") == run_id))
    assert V.join_star_fact(fact, duplicated, waters, stores).count() == 3
    assert (
        L.qa_snowflake_relationships(
            fact.sparkSession, fact, {"EDP_CATALOG": duplicated, "WATERS_CATALOG": waters, "STORE_CATALOG": stores}
        )
        .first()
        .STATUS
        == "FAIL"
    )


def test_tracking_overlap_queries_protect_months_and_years(spark, monkeypatch):
    queries = []
    monkeypatch.setattr(L, "snowflake_read", lambda _, options, query: queries.append(query))
    L.read_pipeline_history(spark, {}, 2026, "ALL")
    L.read_pipeline_history(spark, {}, 2026, 7)
    assert "PERIOD_START_DATE<='2026-12-31'" in queries[0] and "PERIOD_END_DATE>='2026-01-01'" in queries[0]
    assert "PERIOD_START_DATE<='2026-07-31'" in queries[1] and "PERIOD_END_DATE>='2026-07-01'" in queries[1]


@pytest.mark.skipif(not os.environ.get("ECOMM_TEST_DELTA_JARS"), reason="Optional local Delta JARs not configured")
def test_real_delta_annual_replacement_preserves_other_year(spark, monthly):
    table = "default.year_test_" + uuid.uuid4().hex
    try:
        L.load_monthly_delta(spark, table, monthly.withColumn("ANIO", F.lit(2024)), 2024, "ALL")
        annual = monthly.unionByName(monthly.withColumn("MES", F.lit(8)))
        L.load_monthly_delta(spark, table, annual, 2025, "ALL")
        assert spark.table(table).where("ANIO=2024").count() == 2
        assert spark.table(table).where("ANIO=2025").count() == 4
        # Annual replacement also removes old months absent from the replacement file.
        L.load_monthly_delta(spark, table, monthly, 2025, "ALL")
        assert spark.table(table).where("ANIO=2025 AND MES=8").count() == 0
        assert spark.table(table).where("ANIO=2024").count() == 2
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("extra_bu", [False, True])
def test_known_legacy_empty_snowflake_schema_is_updated(star_payloads, extra_bu):
    fact, _, _ = star_payloads
    legacy = fact.withColumn("UNIDADES", F.col("UNIDADES").cast("long"))
    if extra_bu:
        legacy = legacy.withColumn("BU", F.lit("EDP"))

    class EmptyLegacyCursor(Cursor):
        def execute(self, sql):
            super().execute(sql)
            if sql.startswith("SELECT COUNT(*) FROM"):
                self.result = (0,)
            if sql.startswith("CREATE OR REPLACE TABLE"):
                self.payloads[C.TARGET_SNOWFLAKE_TABLE] = fact
            return self

    cursor = EmptyLegacyCursor({C.TARGET_SNOWFLAKE_TABLE: legacy})
    assert L.ensure_snowflake_table(cursor, C.TARGET_SNOWFLAKE_TABLE, fact)
    assert any(sql.startswith("CREATE OR REPLACE TABLE") and sql.endswith("COPY GRANTS") for sql in cursor.commands)
    populated = Cursor({C.TARGET_SNOWFLAKE_TABLE: legacy})
    with pytest.raises(PipelineError, match="explicit migration"):
        L.ensure_snowflake_table(populated, C.TARGET_SNOWFLAKE_TABLE, fact)
    assert not any(sql.startswith("CREATE OR REPLACE") for sql in populated.commands)


@pytest.mark.skipif(not os.environ.get("ECOMM_TEST_DELTA_JARS"), reason="Optional local Delta JARs not configured")
@pytest.mark.parametrize("populated", [False, True])
def test_delta_legacy_schema_upgrade_requires_empty_table(spark, monkeypatch, populated):
    table = "default.schema_" + uuid.uuid4().hex
    monkeypatch.setattr(C, "FACT_CURATED_TABLE", table)
    expected = spark.createDataFrame([(2026, 1, 17.5)], "ANIO int, MES int, UNIDADES double")
    legacy = expected.withColumn("UNIDADES", F.col("UNIDADES").cast("long"))
    try:
        (legacy if populated else legacy.limit(0)).write.format("delta").saveAsTable(table)
        if populated:
            with pytest.raises(PipelineError, match="explicit migration"):
                L.ensure_delta_table(spark, table, expected)
            assert spark.table(table).count() == 1
            assert spark.table(table).schema["UNIDADES"].dataType.simpleString() == "bigint"
        else:
            L.ensure_delta_table(spark, table, expected)
            assert "BU" not in spark.table(table).columns
            assert spark.table(table).schema["UNIDADES"].dataType.simpleString() == "double"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


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
    delta, snowflake = L._empty_business_tables(spark)
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
            result = L.reset_business_test_tables(spark, {}, get_logger("test-reset"))
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
    _, snowflake = L._empty_business_tables(spark)
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
        L.reset_business_test_tables(spark, {}, logger)
    assert captured.value.error_type == "TEST_RESET_ERROR" and "private" not in str(captured.value)
    writer.format.assert_not_called()
    assert logger.event.call_args.kwargs["COMPLETED_TABLES"] == 1


def test_reset_refuses_shared_history_targets_before_any_mutation(spark, monkeypatch):
    monkeypatch.setattr(L, "verify_hive_objects", Mock())
    monkeypatch.setattr(C, "FACT_STAGING_TABLE", "hive_metastore." + C.VALIDATION_RESULTS_TABLE)
    connection = Mock()
    monkeypatch.setattr(L, "snowflake_connection", connection)
    with pytest.raises(PipelineError, match="excluding shared history"):
        L.reset_business_test_tables(spark, {}, Mock())
    connection.assert_not_called()
