"""Month/year ETL entry point and read-only persisted-data QA for Databricks."""

import os
import sys
from pathlib import Path

# Python script tasks may start with only src/ on sys.path.
_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from conf import settings as C
from conf.credentials import get_snowflake_options
from logs.logger import get_logger
from src import extract as E
from src import load as L
from src import transform as V
from src.run_tracking import (
    PipelineError,
    RunContext,
    append_run_record_safely,
    get_widget_value,
    metrics_to_json,
    utc_now,
)


def _runtime(spark=None, dbutils=None):
    spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    if dbutils is None:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    return spark, dbutils


def databricks_context(dbutils):
    context = {}
    try:
        tags = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
        for key, tag in [
            ("DATABRICKS_JOB_ID", "jobId"),
            ("DATABRICKS_JOB_RUN_ID", "jobRunId"),
            ("DATABRICKS_TASK_RUN_ID", "runId"),
            ("DATABRICKS_TASK_NAME", "taskKey"),
            ("ATTEMPT_NUMBER", "taskAttemptNumber"),
        ]:
            try:
                value = tags.get(tag)
                if value.isDefined():
                    context[key] = str(value.get())
            except Exception:
                continue
    except Exception:
        pass
    for key in [
        "DATABRICKS_JOB_ID",
        "DATABRICKS_JOB_RUN_ID",
        "DATABRICKS_TASK_RUN_ID",
        "DATABRICKS_TASK_NAME",
        "ATTEMPT_NUMBER",
        "TRIGGER_TYPE",
    ]:
        context[key] = os.environ.get(key) or context.get(key)
    try:
        attempt = int(context.get("ATTEMPT_NUMBER") or 0)
    except (TypeError, ValueError):
        attempt = 0
    return {
        "databricks_job_id": context.get("DATABRICKS_JOB_ID"),
        "databricks_job_run_id": context.get("DATABRICKS_JOB_RUN_ID"),
        "databricks_task_run_id": context.get("DATABRICKS_TASK_RUN_ID"),
        "databricks_task_name": context.get("DATABRICKS_TASK_NAME"),
        "attempt_number": attempt,
        "trigger_type": context.get("TRIGGER_TYPE") or ("JOB" if context.get("DATABRICKS_JOB_ID") else "INTERACTIVE"),
    }


def initial_metrics(anio, mes, force):
    return {
        "anio": anio,
        "mes": mes,
        "force_reprocess": force,
        "run_type": None,
        "failed_stage": None,
        "files_expected": 4,
        "files_discovered": 0,
        "files_validated": 0,
        "files_loaded": 0,
        "files_moved_to_current": 0,
        "files_deleted_after_failure": 0,
        "source_files": {},
        "catalog_metrics": dict.fromkeys(
            [
                "edp_rows",
                "waters_rows",
                "store_rows",
                "edp_duplicate_keys",
                "waters_duplicate_keys",
                "store_duplicate_keys",
                "cross_catalog_collisions",
            ]
        ),
        "fact_metrics": dict.fromkeys(
            [
                "annual_source_rows",
                "requested_period_rows",
                "transformed_rows",
                "unknown_bu_rows",
                "unmatched_product_rows",
                "unmatched_store_rows",
                "duplicate_business_keys",
                "negative_unit_rows",
                "negative_amount_rows",
            ]
        ),
        "delta_metrics": dict.fromkeys(
            [
                "target_rows_before",
                "target_rows_after",
                "source_amount",
                "target_amount",
                "amount_difference",
                "reconciliation_status",
            ]
        ),
        "snowflake_metrics": dict.fromkeys(
            [
                "target_rows_before",
                "target_rows_after",
                "source_amount",
                "target_amount",
                "amount_difference",
                "reconciliation_status",
            ]
        ),
        "validation_metrics": {"total_rules": 0, "passed_rules": 0, "warning_rules": 0, "failed_rules": 0},
        "cleanup_metrics": {"incoming_files_deleted": 0, "incoming_files_remaining": None, "current_files_updated": 0},
    }


def _file_mapping(files):
    result = {}
    for item in files:
        key = item["LOGICAL_DATASET"] or "UNMAPPED:" + item["ORIGINAL_FILE_NAME"]
        if key in result:
            result[key] = ([result[key]] if isinstance(result[key], str) else result[key]) + [
                item["ORIGINAL_FILE_NAME"]
            ]
        else:
            result[key] = item["ORIGINAL_FILE_NAME"]
    return result


def classify_exception(exc, stage):
    if isinstance(exc, PipelineError):
        return exc.error_type
    return {
        "RUNTIME": "HIVE_LOAD_ERROR",
        "DISCOVERY": "FILE_DISCOVERY_ERROR",
        "EXTRACTION": "SCHEMA_VALIDATION_ERROR",
        "CREDENTIALS": "SNOWFLAKE_CONNECTION_ERROR",
        "IDEMPOTENCY": "SNOWFLAKE_CONNECTION_ERROR",
        "DELTA": "DELTA_LOAD_ERROR",
        "SNOWFLAKE": "SNOWFLAKE_LOAD_ERROR",
        "RECONCILIATION": "RECONCILIATION_ERROR",
        "FILE_MOVEMENT": "DBFS_MOVE_ERROR",
    }.get(stage, "UNEXPECTED_ERROR")


def run_pipeline(anio, mes, force_reprocess=False, *, spark=None, dbutils=None):
    spark, dbutils = _runtime(spark, dbutils)
    run_context = RunContext(
        pipeline_name=C.PIPELINE_NAME,
        pipeline_version=C.PIPELINE_VERSION,
        environment=C.PIPELINE_ENVIRONMENT,
        **databricks_context(dbutils),
    )
    logger = get_logger(run_context.run_id)
    source_files = {}
    source_rows = staging_rows = transformed_rows = target_rows_before = target_rows_after = None
    period_start_date = period_end_date = None
    final_status, error_type, error_message = "FAILED", None, None
    original_exception, tracking_written = None, False
    snowflake_options, credentials_attempted = None, False
    manifest, cache_frames, typed, by_dataset = [], [], {}, {}
    discovery_completed = False
    report = V.ValidationReport(spark, run_context.run_id)
    business_metrics = initial_metrics(None, None, None)
    stage = "PARAMETERS"
    try:
        anio, mes, force_reprocess = V.validate_parameters(anio, mes, force_reprocess)
        report.anio, report.mes = anio, None if mes == "ALL" else mes
        logger.extra.update(ANIO=anio, MES=mes)
        business_metrics.update(anio=anio, mes=mes, force_reprocess=force_reprocess)
        period_start_date, period_end_date = V.period_bounds(anio, mes)
        logger.event(stage, "PASSED")
        stage = "RUNTIME"
        L.verify_hive_objects(spark)
        E.verify_runtime_directories(dbutils)
        stage = "DISCOVERY"
        manifest = E.list_incoming_files(dbutils, run_context.run_id, anio)
        discovery_completed = True
        issues = E.validate_file_batch(manifest, anio)
        business_metrics["files_discovered"] = len(manifest)
        for item, rule, count, description in issues:
            report.add(
                item.get("LOGICAL_DATASET") if item else None,
                rule,
                max(count, 1),
                category="FILE_NAME" if rule in {"FILE_NAME", "FILE_YEAR"} else "FILE_DISCOVERY",
                description=description,
                source=item.get("ORIGINAL_FILE_NAME") if item else None,
            )
        if not issues:
            report.add(None, "FOUR_FILE_BATCH", category="FILE_DISCOVERY")
        source_files = _file_mapping(manifest)
        business_metrics["source_files"] = source_files
        for item in manifest:
            item["FILE_FINGERPRINT"] = E.fingerprint_file(spark, item["SOURCE_PATH"])
            if item["LOGICAL_DATASET"]:
                by_dataset.setdefault(item["LOGICAL_DATASET"], []).append(item)
        business_metrics["source_metadata"] = manifest
        stage = "CREDENTIALS"
        credentials_attempted = True
        snowflake_options = get_snowflake_options(dbutils)
        stage = "IDEMPOTENCY"
        history = L.read_pipeline_history(spark, snowflake_options, anio, mes)
        run_type = L.classify_run(history, force_reprocess)
        business_metrics["run_type"] = run_type
        report.add(
            "ECOMMERCE_FACT",
            "PERIOD_NOT_PREVIOUSLY_SUCCEEDED",
            int(run_type == "DUPLICATE_PERIOD"),
            category="DUPLICATE_PERIOD",
            description="A successful period requires FORCE_REPROCESS=true.",
        )
        stage = "EXTRACTION"
        for dataset, definitions in C.SOURCE_DEFINITIONS.items():
            items = by_dataset.get(dataset, [])
            if len(items) != 1:
                continue
            item = items[0]
            try:
                headers = E.read_csv_header(spark, item["SOURCE_PATH"])
            except Exception:
                report.add(dataset, "READABLE_UTF8_CSV_HEADER", 1, category="SCHEMA", source=item["ORIGINAL_FILE_NAME"])
                continue
            normalized = V.validate_headers(headers, definitions[0], report, dataset, item["ORIGINAL_FILE_NAME"])
            if normalized is None:
                continue
            raw = E.read_source_strings(spark, item["SOURCE_PATH"], normalized)
            frame, row_count = V.validate_and_cast_source(
                raw, dataset, report, item["ORIGINAL_FILE_NAME"], cache_frames
            )
            if dataset == "ECOMMERCE_FACT":
                source_rows = row_count
                business_metrics["fact_metrics"]["annual_source_rows"] = row_count
            else:
                prefix = {"EDP_CATALOG": "edp", "WATERS_CATALOG": "waters", "STORE_CATALOG": "store"}[dataset]
                business_metrics["catalog_metrics"][prefix + "_rows"] = row_count
            if frame is not None:
                typed[dataset] = frame
        stage = "VALIDATION"
        for dataset, prefix in [("EDP_CATALOG", "edp"), ("WATERS_CATALOG", "waters"), ("STORE_CATALOG", "store")]:
            if dataset in typed:
                business_metrics["catalog_metrics"][prefix + "_duplicate_keys"] = V.validate_catalog_duplicates(
                    typed[dataset], dataset, report, by_dataset[dataset][0]["ORIGINAL_FILE_NAME"]
                )
        if {"EDP_CATALOG", "WATERS_CATALOG"} <= typed.keys():
            business_metrics["catalog_metrics"]["cross_catalog_collisions"] = V.validate_cross_catalogs(
                typed["EDP_CATALOG"], typed["WATERS_CATALOG"], report
            )
        if "ECOMMERCE_FACT" in typed:
            catalogs_available = {"EDP_CATALOG", "WATERS_CATALOG"} <= typed.keys()
            report.add(
                "ECOMMERCE_FACT",
                "BU_DERIVATION_CATALOGS_AVAILABLE",
                int(not catalogs_available),
                category="REFERENTIAL",
                source=by_dataset["ECOMMERCE_FACT"][0]["ORIGINAL_FILE_NAME"],
            )
            if catalogs_available:
                typed["ECOMMERCE_FACT"], business_metrics["bu_derivation"] = V.infer_fact_bu(
                    typed["ECOMMERCE_FACT"],
                    typed["EDP_CATALOG"],
                    typed["WATERS_CATALOG"],
                    report,
                    by_dataset["ECOMMERCE_FACT"][0]["ORIGINAL_FILE_NAME"],
                )
        monthly = transformed = None
        if "ECOMMERCE_FACT" in typed:
            fact_file = by_dataset["ECOMMERCE_FACT"][0]
            monthly, detection = V.detect_fact_period(
                typed["ECOMMERCE_FACT"], anio, mes, report, fact_file["ORIGINAL_FILE_NAME"]
            )
            monthly = monthly.cache()
            cache_frames.append(monthly)
            requested_period_rows = detection["REQUESTED_PERIOD_ROWS"]
            business_metrics["period_detection"] = detection
            business_metrics["fact_metrics"].update(
                requested_period_rows=requested_period_rows, unknown_bu_rows=detection["UNKNOWN_BU_ROWS"]
            )
            business_metrics["fact_metrics"]["duplicate_business_keys"] = V.validate_fact_duplicates(
                monthly, report, fact_file["ORIGINAL_FILE_NAME"]
            )
            units, amount = V.validate_negative_values(monthly, report, fact_file["ORIGINAL_FILE_NAME"])
            business_metrics["fact_metrics"].update(negative_unit_rows=units, negative_amount_rows=amount)
            if {"EDP_CATALOG", "WATERS_CATALOG", "STORE_CATALOG"} <= typed.keys():
                coverage = V.validate_references(
                    monthly,
                    typed["EDP_CATALOG"],
                    typed["WATERS_CATALOG"],
                    typed["STORE_CATALOG"],
                    report,
                    fact_file["ORIGINAL_FILE_NAME"],
                )
                business_metrics["coverage_metrics"] = coverage
                business_metrics["fact_metrics"].update(
                    unmatched_product_rows=coverage["UNMATCHED_PRODUCT_ROWS"],
                    unmatched_store_rows=coverage["UNMATCHED_STORE_ROWS"],
                )
        business_metrics["files_validated"] = sum(
            not any(
                r["VALIDATION_STATUS"] == "FAIL" and r["SOURCE_FILE_NAME"] == item["ORIGINAL_FILE_NAME"]
                for r in report.records
            )
            for dataset, items in by_dataset.items()
            for item in items
            if dataset in typed and len(items) == 1
        )
        for item in manifest:
            if item["FILE_FINGERPRINT"]:
                report.add(
                    item["LOGICAL_DATASET"],
                    "SOURCE_FINGERPRINT_UNCHANGED",
                    int(E.fingerprint_file(spark, item["SOURCE_PATH"]) != item["FILE_FINGERPRINT"]),
                    category="FILE_DISCOVERY",
                    source=item["ORIGINAL_FILE_NAME"],
                )
        timestamp = utc_now()
        if not report.failed and monthly is not None:
            transformed = V.enrich_fact(monthly, typed["EDP_CATALOG"], typed["WATERS_CATALOG"], typed["STORE_CATALOG"])
            transformed = V.add_operational_metadata(
                transformed, by_dataset["ECOMMERCE_FACT"][0], anio, mes, timestamp=timestamp
            ).cache()
            cache_frames.append(transformed)
            transformed_rows = transformed.count()
            business_metrics["fact_metrics"]["transformed_rows"] = transformed_rows
            report.add(
                "ECOMMERCE_FACT",
                "ENRICHMENT_ROW_PRESERVATION",
                abs(transformed_rows - requested_period_rows),
                category="REFERENTIAL",
            )
        business_metrics["validation_metrics"] = report.metrics()
        report.log(logger)
        L.persist_validation_results(spark, report)
        if report.failed:
            raise V.ValidationError(
                "The complete four-file batch failed validation; see persisted findings.", report.error_type
            )
        if transformed is None:
            raise V.ValidationError("A complete typed batch is required.", "SCHEMA_VALIDATION_ERROR")
        stage = "DELTA"
        business_metrics["delta_metrics"]["target_rows_before"] = L.delta_period_metrics(
            spark, C.FACT_CURATED_TABLE, anio, mes
        )["rows"]
        snowflake_catalogs = {}
        for dataset, table, bu in [
            ("EDP_CATALOG", C.CATALOG_EDP_TABLE, "EDP"),
            ("WATERS_CATALOG", C.CATALOG_WATERS_TABLE, "WATERS"),
            ("STORE_CATALOG", C.CATALOG_STORE_TABLE, None),
        ]:
            snapshot = V.add_operational_metadata(
                typed[dataset], by_dataset[dataset][0], anio, mes, catalog_bu=bu, timestamp=timestamp
            )
            L.load_catalog_snapshot(spark, table, snapshot)
            # Copy full Delta history so older monthly facts retain their joins.
            snowflake_catalogs[dataset] = spark.table(table)
        staged = V.add_operational_metadata(monthly, by_dataset["ECOMMERCE_FACT"][0], anio, mes, timestamp=timestamp)
        L.load_monthly_delta(spark, C.FACT_STAGING_TABLE, staged, anio, mes)
        staging_rows = V.period_filter(spark.table(C.FACT_STAGING_TABLE), anio, mes).count()
        L.load_monthly_delta(spark, C.FACT_CURATED_TABLE, transformed, anio, mes)
        delta = V.period_filter(spark.table(C.FACT_CURATED_TABLE), anio, mes)
        stage = "RECONCILIATION"
        business_metrics["delta_metrics"].update(L.reconcile_source_to_delta(monthly, transformed, delta))
        if staging_rows != business_metrics["fact_metrics"]["requested_period_rows"]:
            raise PipelineError("Monthly staging count differs from validated source.", "RECONCILIATION_ERROR")
        stage = "SNOWFLAKE"
        sf_metrics = L.load_snowflake_period(
            spark,
            snowflake_options,
            delta,
            anio,
            mes,
            run_context.run_id,
            observed_metrics=business_metrics["snowflake_metrics"],
            catalog_frames=snowflake_catalogs,
        )
        business_metrics["snowflake_metrics"].update(sf_metrics)
        target_rows_before, target_rows_after = sf_metrics["target_rows_before"], sf_metrics["target_rows_after"]
        stage = "RECONCILIATION"
        business_metrics["snowflake_metrics"].update(
            L.reconcile_delta_to_snowflake(spark, snowflake_options, delta, anio, mes, snowflake_catalogs)
        )
        business_metrics["files_loaded"] = C.EXPECTED_FILE_COUNT
        stage = "FILE_MOVEMENT"
        # Recheck fingerprints immediately before touching current; reject changed inputs.
        for item in manifest:
            if E.fingerprint_file(spark, item["SOURCE_PATH"]) != item["FILE_FINGERPRINT"]:
                raise PipelineError("A source fingerprint changed during execution.", "DBFS_MOVE_ERROR")
        E.move_batch_to_current(dbutils, manifest, business_metrics)
        final_status = "SUCCEEDED"
    except Exception as exc:
        original_exception = exc
        final_status = "VALIDATION_FAILED" if isinstance(exc, V.ValidationError) else "FAILED"
        error_type = classify_exception(exc, stage)
        error_message = (
            str(exc)
            if isinstance(exc, PipelineError)
            else f"{type(exc).__name__} during {stage}; sensitive error details omitted."
        )
        business_metrics["failed_stage"] = stage
        logger.event(stage, final_status, ERROR_TYPE=error_type, ERROR_MESSAGE=error_message)
        if isinstance(exc, V.ValidationError) and not report.records:
            report.add(None, error_type, 1, category="PERIOD", description=error_message)
        try:
            L.persist_validation_results(spark, report)
        except Exception:
            business_metrics["validation_persistence_failed"] = True
            logger.event("VALIDATION_PERSISTENCE", "FAILED")
        # A technical failure before discovery still belongs to the submitted incoming batch.
        if not discovery_completed:
            try:
                manifest = E.list_incoming_files(dbutils, run_context.run_id)
                E.validate_file_batch(manifest, report.anio)
                source_files = _file_mapping(manifest)
                business_metrics["files_discovered"] = len(manifest)
                business_metrics["source_files"] = source_files
            except Exception:
                business_metrics["cleanup_discovery_failed"] = True
        try:
            cleanup = E.delete_submitted_batch(dbutils, manifest)
            business_metrics["files_deleted_after_failure"] = cleanup["deleted"]
            business_metrics["cleanup_metrics"].update(
                incoming_files_deleted=cleanup["deleted"], incoming_files_remaining=cleanup["remaining"]
            )
            business_metrics["cleanup_failed_files"] = cleanup["failed_files"]
            logger.event(
                "CLEANUP", "FAILED" if cleanup["failed_files"] else "SUCCEEDED", FILES_DELETED=cleanup["deleted"]
            )
        except Exception:
            business_metrics["cleanup_failed"] = True
            logger.event("CLEANUP", "FAILED", ERROR_TYPE="DBFS_DELETE_ERROR")
    finally:
        business_metrics["validation_metrics"] = report.metrics()
        if not credentials_attempted:
            try:
                snowflake_options = get_snowflake_options(dbutils)
            except Exception:
                snowflake_options = None
        if target_rows_before is None:
            target_rows_before = business_metrics["snowflake_metrics"].get("target_rows_before")
        record = run_context.build_record(
            status=final_status,
            source_name=C.PIPELINE_SOURCE_NAME,
            source_file=metrics_to_json(source_files),
            target_name=C.TARGET_SNOWFLAKE_TABLE,
            period_start_date=period_start_date,
            period_end_date=period_end_date,
            source_rows=source_rows,
            staging_rows=staging_rows,
            transformed_rows=transformed_rows,
            target_rows_before=target_rows_before,
            target_rows_after=target_rows_after,
            business_metrics=business_metrics,
            error_type=error_type,
            error_message=error_message,
        )
        tracking_written = append_run_record_safely(
            spark, snowflake_options, C.PIPELINE_RUN_HISTORY_TABLE, record, logger
        )
        for frame in reversed(cache_frames):
            try:
                frame.unpersist()
            except Exception:
                continue
        logger.event("PIPELINE", final_status, TRACKING_WRITTEN=tracking_written)
    if original_exception is not None:
        raise original_exception
    if final_status == "SUCCEEDED" and not tracking_written:
        raise RuntimeError("Business load succeeded, but the PIPELINE_RUNS tracking record could not be written.")
    return record


def run_from_widgets(spark=None, dbutils=None):
    spark, dbutils = _runtime(spark, dbutils)
    for name, default in [("ANIO", ""), ("MES", "")]:
        if get_widget_value(dbutils, name, None) is None:
            dbutils.widgets.text(name, default)
    if get_widget_value(dbutils, "FORCE_REPROCESS", None) is None:
        dbutils.widgets.dropdown("FORCE_REPROCESS", "false", ["false", "true"])
    return run_pipeline(
        get_widget_value(dbutils, "ANIO"),
        get_widget_value(dbutils, "MES"),
        get_widget_value(dbutils, "FORCE_REPROCESS", "false"),
        spark=spark,
        dbutils=dbutils,
    )


def run_qa_validation(
    anio, mes, run_id=None, logical_dataset=None, bu=None, id_interno=None, id_store=None, *, spark=None, dbutils=None
):
    """Read-only QA from persisted Delta and Snowflake; incoming is never accessed."""
    anio, mes, _ = V.validate_parameters(anio, mes)
    spark, dbutils = _runtime(spark, dbutils)
    L.verify_hive_objects(spark)
    options = get_snowflake_options(dbutils)
    if bu is not None:
        bu = C.BU_NORMALIZATION.get(str(bu).strip().upper())
        if bu is None:
            raise ValueError("Unsupported QA BU")
    if logical_dataset is not None and logical_dataset not in C.SOURCE_DEFINITIONS:
        raise ValueError("Unsupported logical dataset")
    for value in [id_interno, id_store]:
        if value is not None and (isinstance(value, bool) or V.parse_numeric(value, "int64")[1] != "VALID"):
            raise ValueError("QA IDs must be valid integers")
    id_interno = int(V.parse_numeric(id_interno, "int64")[0]) if id_interno is not None else None
    id_store = int(V.parse_numeric(id_store, "int64")[0]) if id_store is not None else None
    source = V.period_filter(spark.table(C.FACT_STAGING_TABLE), anio, mes)
    source = V.apply_qa_filters(source, run_id=run_id)
    run_ids = source.select("RUN_ID").distinct()
    effective_run_id = run_id
    if not effective_run_id:
        ids = run_ids.limit(2).collect()
        effective_run_id = ids[0][0] if len(ids) == 1 else None
    catalogs = [
        spark.table(table).join(run_ids, "RUN_ID", "inner")
        for table in (C.CATALOG_EDP_TABLE, C.CATALOG_WATERS_TABLE, C.CATALOG_STORE_TABLE)
    ]
    edp, waters, stores = catalogs
    filters = dict(run_id=run_id, bu=bu, id_interno=id_interno, id_store=id_store)
    source = V.apply_qa_filters(source, **filters)
    delta = V.apply_qa_filters(V.period_filter(spark.table(C.FACT_CURATED_TABLE), anio, mes), **filters)
    if id_interno is not None:
        edp, waters = [frame.where(F.col("ID_INTERNO") == id_interno) for frame in (edp, waters)]
    if id_store is not None:
        stores = stores.where(F.col("ID_STORE") == id_store)
    expected_delta = V.join_star_fact(source, edp, waters, stores).drop("PRODUCT_MATCHED", "STORE_MATCHED")
    star_delta = V.star_fact(delta)
    sf = L.snowflake_read(
        spark,
        options,
        f"SELECT {','.join(star_delta.columns)} FROM {C.TARGET_SNOWFLAKE_TABLE} WHERE {V.period_predicate(anio, mes)}",
    )
    sf = V.apply_qa_filters(sf, **filters)
    history = L.read_pipeline_history(spark, options, anio, mes, effective_run_id)
    findings = L.qa_validation_results(spark, anio, mes, effective_run_id, logical_dataset)
    if mes == "ALL" and not effective_run_id:
        findings = findings.join(run_ids, "RUN_ID", "inner")
    duplicate_detail = V.qa_catalog_duplicate_detail(edp, waters, stores)
    if logical_dataset:
        duplicate_detail = duplicate_detail.where(F.col("LOGICAL_DATASET") == logical_dataset)
    collisions = V.qa_cross_catalog_collisions(edp, waters)
    coverage = V.versioned_coverage_metrics(source, edp, waters, stores)
    period_report = V.ValidationReport(spark, effective_run_id or "QA", anio, mes)
    _, detection = V.detect_fact_period(
        source.select(*C.FACT_EXPECTED_COLUMNS, "NORMALIZED_BU"), anio, mes, period_report
    )
    fact_duplicates = V.duplicate_groups(
        source,
        ["TIME_ID", "ID_STORE", "ID_INTERNO", "NORMALIZED_BU"],
        C.FACT_EXPECTED_COLUMNS + ["NORMALIZED_BU"],
        ["UNIDADES", "MONTO"],
    )
    negatives = source.agg(
        F.sum(F.when(F.col("UNIDADES") < 0, 1).otherwise(0)).alias("NEGATIVE_UNIT_ROWS"),
        F.sum(F.when(F.col("MONTO") < 0, 1).otherwise(0)).alias("NEGATIVE_AMOUNT_ROWS"),
    ).fillna(0)
    join_check = spark.createDataFrame(
        [
            (
                coverage["FACT_ROWS"],
                coverage["ROWS_AFTER_PRODUCT_JOIN"],
                coverage["ROWS_AFTER_STORE_JOIN"],
                "PASS"
                if coverage["FACT_ROWS"] == coverage["ROWS_AFTER_PRODUCT_JOIN"] == coverage["ROWS_AFTER_STORE_JOIN"]
                else "FAIL",
            )
        ],
        "ROWS_BEFORE long, ROWS_AFTER_PRODUCT_JOIN long, ROWS_AFTER_STORE_JOIN long, STATUS string",
    )
    source_delta = L.qa_reconciliation(spark, expected_delta, delta, "SOURCE_DELTA")
    delta_sf = L.qa_reconciliation(spark, star_delta, sf, "DELTA_SNOWFLAKE")
    catalog_qa, remote_catalogs = L.qa_snowflake_catalogs(
        spark,
        options,
        {"EDP_CATALOG": edp, "WATERS_CATALOG": waters, "STORE_CATALOG": stores},
        id_interno=id_interno,
        id_store=id_store,
    )
    star_qa = L.qa_snowflake_relationships(spark, sf, remote_catalogs)
    joined_data = V.join_star_fact(
        sf, remote_catalogs["EDP_CATALOG"], remote_catalogs["WATERS_CATALOG"], remote_catalogs["STORE_CATALOG"]
    )
    failed = (
        (mes != "ALL" and not effective_run_id)
        or source.count() == 0
        or period_report.failed
        or source_delta.first().STATUS == "FAIL"
        or delta_sf.first().STATUS == "FAIL"
        or catalog_qa.where("STATUS = 'FAIL'").limit(1).count() > 0
        or star_qa.first().STATUS == "FAIL"
        or any(coverage[k] for k in ["UNMATCHED_PRODUCT_ROWS", "UNMATCHED_STORE_ROWS", "UNKNOWN_BU_ROWS"])
        or coverage["FACT_ROWS"] != coverage["ROWS_AFTER_PRODUCT_JOIN"]
        or coverage["FACT_ROWS"] != coverage["ROWS_AFTER_STORE_JOIN"]
        or duplicate_detail.limit(1).count() > 0
        or (C.CROSS_CATALOG_COLLISION_SEVERITY == "ERROR" and collisions.limit(1).count() > 0)
        or (C.FACT_BUSINESS_KEY_DUPLICATE_SEVERITY == "ERROR" and fact_duplicates.limit(1).count() > 0)
        or findings.where(F.col("VALIDATION_STATUS") == "FAIL").limit(1).count() > 0
        or source.select("RUN_ID")
        .distinct()
        .join(history.where(F.col("STATUS") == "SUCCEEDED").select("RUN_ID").distinct(), "RUN_ID", "left_anti")
        .limit(1)
        .count()
        > 0
    )
    warnings = (
        findings.where(F.col("VALIDATION_STATUS") == "WARNING").limit(1).count() > 0
        or any(negatives.first())
        or duplicate_detail.limit(1).count() > 0
        or collisions.limit(1).count() > 0
        or fact_duplicates.limit(1).count() > 0
    )
    status = "FAIL" if failed else "PASS_WITH_WARNINGS" if warnings else "PASS"
    cover_frame = spark.createDataFrame([coverage])
    return {
        "qa_pipeline_history": history,
        "qa_validation_results": findings,
        "qa_period_detection": spark.createDataFrame([(metrics_to_json(detection),)], "PERIOD_METRICS_JSON string"),
        "qa_catalog_duplicate_summary": duplicate_detail.groupBy("LOGICAL_DATASET", "KEY_COLUMN", "CLASSIFICATION").agg(
            F.count("*").alias("DUPLICATE_KEYS"), F.sum("N").alias("AFFECTED_ROWS")
        ),
        "qa_catalog_duplicate_detail": duplicate_detail,
        "qa_cross_catalog_collisions": collisions,
        "qa_product_catalog_coverage": cover_frame.select(
            "FACT_ROWS", "MATCHED_PRODUCT_ROWS", "UNMATCHED_PRODUCT_ROWS", "UNKNOWN_BU_ROWS", "PRODUCT_MATCH_PERCENTAGE"
        ),
        "qa_store_catalog_coverage": cover_frame.select(
            "FACT_ROWS", "MATCHED_STORE_ROWS", "UNMATCHED_STORE_ROWS", "STORE_MATCH_PERCENTAGE"
        ),
        "qa_fact_duplicate_summary": fact_duplicates,
        "qa_negative_values": negatives,
        "qa_join_multiplication_check": join_check,
        "qa_source_delta_reconciliation": source_delta,
        "qa_delta_snowflake_reconciliation": delta_sf,
        "qa_snowflake_catalog_reconciliation": catalog_qa,
        "qa_snowflake_star_relationships": star_qa,
        "qa_joined_ecommerce_data": joined_data,
        "qa_final_status": spark.createDataFrame(
            [(anio, None if mes == "ALL" else mes, effective_run_id, status)],
            "ANIO int, MES int, RUN_ID string, STATUS string",
        ),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E-commerce month/year ETL for Databricks")
    parser.add_argument("--anio", "--ANIO", dest="anio", required=True)
    parser.add_argument("--mes", "--MES", dest="mes", required=True)
    parser.add_argument("--force-reprocess", "--FORCE_REPROCESS", dest="force_reprocess", default="false")
    args = parser.parse_args()
    run_pipeline(args.anio, args.mes, args.force_reprocess)
