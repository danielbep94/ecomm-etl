import datetime as dt
from unittest.mock import Mock

import pytest

from conf import settings as C
from src import main as M
from src.run_tracking import PipelineError

_REAL_CATALOG_DUPLICATE_VALIDATOR = M.V.validate_catalog_duplicates


class Frame:
    def __init__(self, n=2):
        self.n = n

    def cache(self):
        return self

    def unpersist(self):
        return self

    def count(self):
        return self.n


@pytest.fixture
def pipeline(monkeypatch):
    spark, dbutils = Mock(), Mock()
    monkeypatch.setattr(M, "_runtime", lambda *args: (spark, dbutils))
    monkeypatch.setattr(M, "databricks_context", lambda dbutils: {})
    monkeypatch.setattr(M, "get_snowflake_options", lambda _: {"configured": True})
    monkeypatch.setattr(M.L, "get_snowflake_connector", Mock(return_value=Mock(__version__="test-connector")))
    monkeypatch.setattr(M.L, "verify_hive_objects", Mock())
    monkeypatch.setattr(M.E, "verify_runtime_directories", Mock())
    names = [C.FACT_FILE_TEMPLATE.format(ANIO=2025)] + [
        d["expected_file_name"] for d in C.EXPECTED_STATIC_FILES.values()
    ]

    def manifest(dbutils, run_id, expected_year=None):
        return [
            {
                "RUN_ID": run_id,
                "ORIGINAL_FILE_NAME": name,
                "SOURCE_PATH": C.INCOMING_PATH + "/" + name,
                "FILE_SIZE": 4,
                "MODIFICATION_TIMESTAMP": dt.datetime.now(dt.timezone.utc),
                "FILE_FINGERPRINT": None,
                "FILE_YEAR": None,
                "LOGICAL_DATASET": None,
            }
            for name in names
        ]

    monkeypatch.setattr(M.E, "list_incoming_files", manifest)
    monkeypatch.setattr(M.E, "fingerprint_file", lambda *args: "a" * 64)

    def header(spark, path):
        if "FULL" in path:
            return C.FACT_EXPECTED_COLUMNS
        if "POS" in path:
            return C.STORE_EXPECTED_COLUMNS
        return C.EDP_EXPECTED_COLUMNS

    monkeypatch.setattr(M.E, "read_csv_header", header)
    monkeypatch.setattr(M.E, "read_source_strings", lambda *args: Frame())
    monkeypatch.setattr(
        M.V, "validate_and_cast_source", lambda raw, dataset, *args: (Frame(), 3 if dataset == "ECOMMERCE_FACT" else 2)
    )
    monkeypatch.setattr(M.V, "validate_catalog_duplicates", lambda *args: 0)
    monkeypatch.setattr(M.V, "validate_cross_catalogs", lambda *args: 0)
    monkeypatch.setattr(M.V, "infer_fact_bu", lambda frame, *args: (frame, {"derived_rows": 2}))
    monkeypatch.setattr(
        M.V, "detect_fact_period", lambda *args: (Frame(), {"REQUESTED_PERIOD_ROWS": 2, "UNKNOWN_BU_ROWS": 0})
    )
    monkeypatch.setattr(M.V, "validate_fact_duplicates", lambda *args: 0)
    monkeypatch.setattr(M.V, "validate_negative_values", lambda *args: (0, 0))
    monkeypatch.setattr(
        M.V, "validate_references", lambda *args: {"UNMATCHED_PRODUCT_ROWS": 0, "UNMATCHED_STORE_ROWS": 0}
    )
    monkeypatch.setattr(M.V, "enrich_fact", lambda *args: Frame())
    monkeypatch.setattr(M.V, "add_operational_metadata", lambda frame, *args, **kwargs: frame)
    monkeypatch.setattr(M.V, "period_filter", lambda *args: Frame())
    monkeypatch.setattr(M.L, "read_pipeline_history", Mock())
    monkeypatch.setattr(M.L, "classify_run", lambda *args: "FIRST_LOAD")

    def persist(spark, report):
        report.persisted = True

    monkeypatch.setattr(M.L, "persist_validation_results", persist)
    monkeypatch.setattr(M.L, "delta_period_metrics", lambda *args: {"rows": 0})
    monkeypatch.setattr(M.L, "load_catalog_snapshot", Mock())
    monkeypatch.setattr(M.L, "load_monthly_delta", Mock())
    monkeypatch.setattr(
        M.L, "reconcile_source_to_delta", lambda *args: {"target_rows_after": 2, "reconciliation_status": "PASS"}
    )
    monkeypatch.setattr(
        M.L, "load_snowflake_period", lambda *args, **kwargs: {"target_rows_before": 0, "target_rows_after": 2}
    )
    monkeypatch.setattr(M.L, "reconcile_delta_to_snowflake", lambda *args: {"reconciliation_status": "PASS"})
    move, delete = Mock(), Mock(return_value={"deleted": 4, "remaining": 0, "failed_files": []})
    monkeypatch.setattr(M.E, "move_batch_to_current", move)
    monkeypatch.setattr(M.E, "delete_submitted_batch", delete)
    tracking = Mock(return_value=True)
    monkeypatch.setattr(M, "append_run_record_safely", tracking)
    return {"spark": spark, "dbutils": dbutils, "tracking": tracking, "move": move, "delete": delete}


@pytest.mark.parametrize("run_type,force", [("FIRST_LOAD", False), ("RETRY", False), ("REPROCESS", True)])
def test_successful_first_load_retry_and_forced_reprocess(pipeline, monkeypatch, run_type, force):
    monkeypatch.setattr(M.L, "classify_run", lambda *args: run_type)
    record = M.run_pipeline(2025, 7, force)
    assert record["STATUS"] == "SUCCEEDED"
    assert record["SOURCE_ROWS"] == 3 and record["STAGING_ROWS"] == 2 and record["TRANSFORMED_ROWS"] == 2
    pipeline["tracking"].assert_called_once()
    pipeline["move"].assert_called_once()
    pipeline["delete"].assert_not_called()
    assert M.L.load_monthly_delta.call_count == 2


def test_duplicate_period_validation_never_loads_business_tables(pipeline, monkeypatch):
    monkeypatch.setattr(M.L, "classify_run", lambda *args: "DUPLICATE_PERIOD")
    with pytest.raises(M.V.ValidationError) as captured:
        M.run_pipeline(2025, 7)
    assert captured.value.error_type == "DUPLICATE_PERIOD_VALIDATION_ERROR"
    M.L.load_catalog_snapshot.assert_not_called()
    M.L.load_monthly_delta.assert_not_called()
    pipeline["move"].assert_not_called()
    pipeline["delete"].assert_called_once()
    assert pipeline["tracking"].call_args.args[3]["STATUS"] == "VALIDATION_FAILED"


def test_bad_header_fails_complete_batch_and_tracks_once(pipeline, monkeypatch):
    monkeypatch.setattr(M.E, "read_csv_header", lambda *args: ["UNKNOWN"])
    with pytest.raises(M.V.ValidationError):
        M.run_pipeline(2025, 7)
    M.L.load_catalog_snapshot.assert_not_called()
    pipeline["delete"].assert_called_once()
    pipeline["tracking"].assert_called_once()


def test_validation_exception_and_tracking_identify_the_failed_rule(pipeline, monkeypatch):
    validate = M.V.validate_and_cast_source

    def with_invalid_factor(raw, dataset, report, source, caches):
        if dataset == "WATERS_CATALOG":
            report.add(dataset, "POSITIVE_FACTOR_CONVERSION", 1, column="FACTOR_CONVERSION", source=source)
        return validate(raw, dataset, report, source, caches)

    monkeypatch.setattr(M.V, "validate_and_cast_source", with_invalid_factor)
    with pytest.raises(M.V.ValidationError, match="WATERS_CATALOG.POSITIVE_FACTOR_CONVERSION") as captured:
        M.run_pipeline(2025, "ALL", True)
    assert "1 affected" in str(captured.value) and "RUN_ID=" in str(captured.value)
    assert "WATERS_CATALOG.POSITIVE_FACTOR_CONVERSION" in pipeline["tracking"].call_args.args[3]["ERROR_MESSAGE"]
    M.L.load_catalog_snapshot.assert_not_called()
    M.L.load_monthly_delta.assert_not_called()


def test_technical_failure_preserves_original_error_and_cleans_files(pipeline, monkeypatch):
    failure = PipelineError("Safe failure", "SNOWFLAKE_LOAD_ERROR")

    def fail(*args, **kw):
        raise failure

    monkeypatch.setattr(M.L, "load_snowflake_period", fail)
    with pytest.raises(PipelineError) as captured:
        M.run_pipeline(2025, 7)
    assert captured.value is failure
    pipeline["delete"].assert_called_once()
    pipeline["move"].assert_not_called()
    pipeline["tracking"].assert_called_once()
    assert pipeline["tracking"].call_args.args[3]["STATUS"] == "FAILED"


def test_missing_connector_is_detected_before_extraction_and_delta_writes(pipeline, monkeypatch):
    failure = PipelineError("Missing Python module: snowflake", "RUNTIME_DEPENDENCY_ERROR")
    monkeypatch.setattr(M.L, "get_snowflake_connector", Mock(side_effect=failure))
    extract = Mock()
    monkeypatch.setattr(M.E, "read_source_strings", extract)
    publish = Mock()
    monkeypatch.setattr(M.L, "load_snowflake_period", publish)
    with pytest.raises(PipelineError) as captured:
        M.run_pipeline(2025, "ALL", True)
    assert captured.value is failure
    extract.assert_not_called()
    M.L.verify_hive_objects.assert_not_called()
    M.L.load_catalog_snapshot.assert_not_called()
    M.L.load_monthly_delta.assert_not_called()
    publish.assert_not_called()
    record = pipeline["tracking"].call_args.args[3]
    assert record["ERROR_TYPE"] == "RUNTIME_DEPENDENCY_ERROR" and record["STATUS"] == "FAILED"
    pipeline["delete"].assert_called_once()  # Existing submitted-batch cleanup policy.


def test_tracking_failure_after_business_success_fails_job(pipeline):
    pipeline["tracking"].return_value = False
    with pytest.raises(RuntimeError, match="Business load succeeded"):
        M.run_pipeline(2025, 7)
    pipeline["move"].assert_called_once()
    pipeline["delete"].assert_not_called()
    pipeline["tracking"].assert_called_once()


def test_cleanup_failure_does_not_replace_original_error(pipeline, monkeypatch):
    original = PipelineError("Safe failure", "SNOWFLAKE_LOAD_ERROR")

    def fail(*args, **kw):
        raise original

    monkeypatch.setattr(M.L, "load_snowflake_period", fail)
    pipeline["delete"].side_effect = RuntimeError("cleanup failed")
    with pytest.raises(PipelineError) as captured:
        M.run_pipeline(2025, 7)
    assert captured.value is original
    pipeline["tracking"].assert_called_once()


@pytest.mark.parametrize(
    "scope,missing_history,repeated_sales",
    [(7, False, False), ("ALL", False, False), ("ALL", True, False), ("ALL", False, True)],
)
def test_qa_uses_persisted_snapshots_without_incoming(
    spark, catalogs, monkeypatch, scope, missing_history, repeated_sales
):
    from src import transform as V
    from src.run_tracking import utc_now

    edp, waters, stores = catalogs
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
        [(202507, 1, 1, 1, 10.0, "EDP")]
        + ([(202507, 1, 1, 1, 10.0, "EDP"), (202507, 1, 1, 2, 5.0, "EDP")] if repeated_sales else []),
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES long, MONTO double, BU string",
    )
    source = V.star_fact(V.add_operational_metadata(V.normalize_bu(source), metadata, 2025, 7, timestamp=utc_now()))
    from pyspark.sql import functions as F

    second_id = "22222222-2222-2222-2222-222222222222"
    second = (
        source.withColumn("TIME_ID", F.lit(202508).cast("long"))
        .withColumn("MES", F.lit(8))
        .withColumn("RUN_ID", F.lit(second_id))
    )
    changed_product = edp.withColumn("NOMBRE", F.lit("Changed product"))
    changed_store = stores.withColumn("CADENA", F.lit("Changed chain"))

    def versions(first, second):
        return first.withColumn("RUN_ID", F.lit(run_id)).unionByName(second.withColumn("RUN_ID", F.lit(second_id)))

    tables = {
        C.FACT_STAGING_TABLE: source.unionByName(second),
        C.FACT_CURATED_TABLE: V.enrich_fact(source, edp, waters, stores).unionByName(
            V.enrich_fact(second, changed_product, waters, changed_store)
        ),
        C.CATALOG_EDP_TABLE: versions(edp, changed_product),
        C.CATALOG_WATERS_TABLE: versions(waters, waters),
        C.CATALOG_STORE_TABLE: versions(stores, changed_store),
    }
    monkeypatch.setattr(spark, "table", lambda name: tables[name])
    monkeypatch.setattr(M.L, "verify_hive_objects", lambda spark: spark.conf.set("spark.sql.session.timeZone", "UTC"))
    monkeypatch.setattr(M, "get_snowflake_options", lambda _: {})

    def snowflake_read(_, options, query):
        remote = {
            C.TARGET_SNOWFLAKE_TABLE: V.star_fact(tables[C.FACT_CURATED_TABLE]),
            **{
                C.SNOWFLAKE_CATALOG_TABLES[key]: tables[table]
                for key, table in [
                    ("EDP_CATALOG", C.CATALOG_EDP_TABLE),
                    ("WATERS_CATALOG", C.CATALOG_WATERS_TABLE),
                    ("STORE_CATALOG", C.CATALOG_STORE_TABLE),
                ]
            },
        }
        frame = next(frame for table, frame in remote.items() if "FROM " + table in query)
        return frame.where(F.expr(query.split(" WHERE ", 1)[1])) if " WHERE " in query else frame

    monkeypatch.setattr(M.L, "snowflake_read", snowflake_read)
    monkeypatch.setattr(
        M.L,
        "read_pipeline_history",
        lambda *args: spark.createDataFrame(
            [(run_id, "SUCCEEDED")] + ([] if missing_history else [(second_id, "SUCCEEDED")]),
            "RUN_ID string, STATUS string",
        ),
    )

    def validation_results(_, anio, mes, selected_run=None, logical_dataset=None):
        frame = spark.createDataFrame(
            [(run_id, "PASS"), (second_id, "PASS"), ("superseded", "FAIL")], "RUN_ID string, VALIDATION_STATUS string"
        )
        return frame.where(F.col("RUN_ID") == selected_run) if selected_run else frame

    monkeypatch.setattr(M.L, "qa_validation_results", validation_results)
    monkeypatch.setattr(M.E, "list_incoming_files", Mock(side_effect=AssertionError("QA must not read incoming")))
    result = M.run_qa_validation(2025, scope, id_interno="1e0", spark=spark, dbutils=Mock())
    assert len(result) == 18
    expected_status = "FAIL" if missing_history else "PASS_WITH_WARNINGS" if repeated_sales else "PASS"
    assert result["qa_final_status"].first().STATUS == expected_status
    rows_per_month = 3 if repeated_sales else 1
    expected_rows = rows_per_month * (2 if scope == "ALL" else 1)
    assert result["qa_product_catalog_coverage"].first().MATCHED_PRODUCT_ROWS == expected_rows
    joined = result["qa_joined_ecommerce_data"].orderBy("MES").collect()
    assert len(joined) == expected_rows
    assert joined[0].PRODUCT_NOMBRE == "Product" and joined[0].STORE_CADENA == "Chain"
    assert joined[0].PRODUCT_MATCHED and joined[0].STORE_MATCHED
    july = [row for row in joined if row.MES == 7]
    assert sorted(row.MONTO for row in july) == ([5.0, 10.0, 10.0] if repeated_sales else [10.0])
    summary = result["qa_sales_summary"].orderBy("MES").collect()
    assert len(summary) == (2 if scope == "ALL" else 1)
    assert summary[0].ANIO == 2025 and summary[0].MES == 7
    assert summary[0].CADENA == "Chain" and summary[0].MARCA == "Brand"
    assert summary[0].UNIDADES == (4.0 if repeated_sales else 1.0)
    assert summary[0].VENTAS == (25 if repeated_sales else 10)
    duplicates = result["qa_fact_duplicate_summary"].collect()
    assert len(duplicates) == (2 if repeated_sales else 0)
    if repeated_sales:
        assert all(row.N == 3 and row.BUSINESS_VARIANTS == 2 for row in duplicates)
    if scope == "ALL":
        assert joined[-1].PRODUCT_NOMBRE == "Changed product" and joined[-1].STORE_CADENA == "Changed chain"
        assert result["qa_catalog_duplicate_detail"].count() == 0
        assert result["qa_snowflake_star_relationships"].first().STATUS == "PASS"
        assert summary[1].MES == 8 and summary[1].CADENA == "Changed chain"
    M.E.list_incoming_files.assert_not_called()


@pytest.mark.skipif(
    not __import__("os").environ.get("ECOMM_TEST_DELTA_JARS"), reason="Optional local Delta JARs not configured"
)
@pytest.mark.parametrize("scope", [7, "ALL"])
def test_complete_csv_batch_real_delta_and_file_movement(spark, catalogs, tmp_path, monkeypatch, scope):
    import csv
    import uuid
    from pathlib import Path
    from types import SimpleNamespace
    from urllib.parse import unquote, urlparse

    from pyspark.sql import functions as F

    incoming, current = tmp_path / "incoming", tmp_path / "current"
    incoming.mkdir()
    current.mkdir()
    monkeypatch.setattr(C, "INCOMING_PATH", incoming.as_uri())
    monkeypatch.setattr(C, "CURRENT_PATH", current.as_uri())
    for key in [
        "CATALOG_EDP_TABLE",
        "CATALOG_WATERS_TABLE",
        "CATALOG_STORE_TABLE",
        "FACT_STAGING_TABLE",
        "FACT_CURATED_TABLE",
        "VALIDATION_RESULTS_TABLE",
    ]:
        monkeypatch.setattr(C, key, "default.integration_" + uuid.uuid4().hex)

    def write_csv(name, columns, rows):
        with (incoming / name).open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            writer.writerows(rows)

    edp, waters, stores = catalogs
    edp = edp.withColumn("NOMBRE", F.lit("\ufeff Cafe\u0301\u00a0  Danone\u200b "))
    stores = stores.withColumn("CADENA", F.lit("\u00a0Chain\x00 Online "))
    waters = waters.withColumn("FACTOR_CONVERSION", F.lit(None).cast("double"))
    for dataset, frame in [("EDP_CATALOG", edp), ("WATERS_CATALOG", waters), ("STORE_CATALOG", stores)]:
        columns = C.SOURCE_DEFINITIONS[dataset][0]
        write_csv(
            C.EXPECTED_STATIC_FILES[dataset]["expected_file_name"],
            columns,
            [tuple(row[c] for c in columns) for row in frame.collect()],
        )
    write_csv(
        C.FACT_FILE_TEMPLATE.format(ANIO=2025),
        C.FACT_EXPECTED_COLUMNS,
        [
            (202507, 1, 1, 2.5, 20.0),
            (202507, 1, 1, 2.5, 20.0),
            (202507, 1, 1, 1.25, 5.0),
            (202508, 1, 2, " -   ", " -   "),
        ],
    )

    class LocalFS:
        @staticmethod
        def path(uri):
            return Path(unquote(urlparse(uri).path))

        def ls(self, uri):
            path = self.path(uri)
            if not path.exists():
                raise FileNotFoundError("No such file")
            paths = list(path.iterdir()) if path.is_dir() else [path]
            return [
                SimpleNamespace(
                    path=p.as_uri() + ("/" if p.is_dir() else ""),
                    name=p.name + ("/" if p.is_dir() else ""),
                    size=p.stat().st_size,
                    modificationTime=p.stat().st_mtime_ns // 1000000,
                )
                for p in paths
            ]

        def rm(self, uri, recursive=False):
            self.path(uri).unlink()
            return True

        def mv(self, src, dest, recursive=False):
            self.path(src).rename(self.path(dest))
            return True

        def mkdirs(self, uri):
            self.path(uri).mkdir(parents=True, exist_ok=True)
            return True

    dbutils = SimpleNamespace(fs=LocalFS())
    monkeypatch.setattr(
        M.L, "verify_hive_objects", lambda session: session.conf.set("spark.sql.session.timeZone", "UTC")
    )
    monkeypatch.setattr(M, "get_snowflake_options", lambda _: {})
    monkeypatch.setattr(M.L, "read_pipeline_history", lambda *args: spark.createDataFrame([], "STATUS string"))

    def snowflake_load(spark, options, frame, *args, **kwargs):
        assert set(kwargs["catalog_frames"]) == set(C.SNOWFLAKE_CATALOG_TABLES)
        july = frame.where("MES=7")
        assert july.count() == 3
        assert july.where("UNIDADES=2.5 AND MONTO=20.0").count() == 2
        assert july.agg(F.sum("MONTO")).first()[0] == 45.0
        metrics = M.L.frame_metrics(frame)
        return {"target_rows_before": 0, "target_rows_after": metrics["rows"], "reconciliation_status": "PASS"}

    monkeypatch.setattr(M.L, "load_snowflake_period", snowflake_load)
    monkeypatch.setattr(M.L, "reconcile_delta_to_snowflake", lambda *args: {"reconciliation_status": "PASS"})
    tracking = Mock(return_value=True)
    monkeypatch.setattr(M, "append_run_record_safely", tracking)
    tables = [
        getattr(C, key)
        for key in [
            "CATALOG_EDP_TABLE",
            "CATALOG_WATERS_TABLE",
            "CATALOG_STORE_TABLE",
            "FACT_STAGING_TABLE",
            "FACT_CURATED_TABLE",
            "VALIDATION_RESULTS_TABLE",
        ]
    ]
    try:
        record = M.run_pipeline(2025, scope, False, spark=spark, dbutils=dbutils)
        assert record["STATUS"] == "SUCCEEDED"
        expected_rows = 4 if scope == "ALL" else 3
        assert (
            record["SOURCE_ROWS"] == 4
            and record["STAGING_ROWS"] == expected_rows
            and record["TRANSFORMED_ROWS"] == expected_rows
        )
        result = spark.table(C.FACT_CURATED_TABLE).orderBy("MES").collect()
        assert len(result) == expected_rows and result[0].MES == 7
        assert sorted((row.UNIDADES, row.MONTO) for row in result if row.MES == 7) == [
            (1.25, 5.0),
            (2.5, 20.0),
            (2.5, 20.0),
        ]
        if scope == "ALL":
            assert result[-1].MES == 8 and result[-1].MONTO is None and result[-1].UNIDADES is None
            assert str(record["PERIOD_START_DATE"]) == "2025-01-01"
            assert str(record["PERIOD_END_DATE"]) == "2025-12-31"
            assert spark.table(C.CATALOG_EDP_TABLE).first().MES is None
        assert result[0].PRODUCT_MARCA == "Brand" and result[0].STORE_CADENA == "Chain Online"
        assert result[0].PRODUCT_NOMBRE == "Café Danone"
        assert spark.table(C.CATALOG_EDP_TABLE).first().NOMBRE == "Café Danone"
        assert "SKU" not in spark.table(C.CATALOG_EDP_TABLE).columns
        assert "BU" not in spark.table(C.FACT_CURATED_TABLE).columns
        assert spark.table(C.CATALOG_WATERS_TABLE).first().FACTOR_CONVERSION is None
        assert spark.table(C.VALIDATION_RESULTS_TABLE).where("VALIDATION_STATUS='WARNING'").count() >= 3
        assert {row.NORMALIZED_BU for row in result} == ({"EDP", "WATERS"} if scope == "ALL" else {"EDP"})
        metrics = __import__("json").loads(record["BUSINESS_METRICS_JSON"])
        assert metrics["bu_derivation"]["derived_rows"] == 4
        duplicates = (
            spark.table(C.VALIDATION_RESULTS_TABLE)
            .where("VALIDATION_CATEGORY='DUPLICATE' AND LOGICAL_DATASET='ECOMMERCE_FACT'")
            .collect()
        )
        assert {row.VALIDATION_RULE: row.AFFECTED_ROWS for row in duplicates} == {
            "EXACT_FACT_DUPLICATE": 2,
            "DUPLICATE_BUSINESS_KEY": 1,
        }
        assert all(row.VALIDATION_STATUS == "WARNING" and row.SEVERITY == "WARNING" for row in duplicates)
        assert spark.table(C.VALIDATION_RESULTS_TABLE).where(F.col("VALIDATION_STATUS") == "FAIL").count() == 0
        assert list(incoming.iterdir()) == [] and len(list(current.iterdir())) == 4
        tracking.assert_called_once()
    finally:
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("dataset", ["EDP_CATALOG", "WATERS_CATALOG", "STORE_CATALOG"])
def test_catalog_duplicates_block_every_business_write(pipeline, spark, catalogs, monkeypatch, dataset):
    real_validate = _REAL_CATALOG_DUPLICATE_VALIDATOR
    frames = dict(zip(C.SNOWFLAKE_CATALOG_TABLES, catalogs))
    duplicate = frames[dataset].unionByName(frames[dataset])

    def validate(frame, name, report, source):
        return real_validate(duplicate, name, report, source) if name == dataset else 0

    monkeypatch.setattr(M.V, "validate_catalog_duplicates", validate)
    snowflake_load = Mock()
    monkeypatch.setattr(M.L, "load_snowflake_period", snowflake_load)
    with pytest.raises(M.V.ValidationError, match="failed validation"):
        M.run_pipeline(2025, 7)
    M.L.load_catalog_snapshot.assert_not_called()
    M.L.load_monthly_delta.assert_not_called()
    snowflake_load.assert_not_called()
    pipeline["move"].assert_not_called()
    assert pipeline["tracking"].call_args.args[3]["STATUS"] == "VALIDATION_FAILED"
