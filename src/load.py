"""Delta persistence and single-session transactional Snowflake period replacement."""

import base64
import contextlib
import re
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

from conf import settings as C
from conf.credentials import SNOWFLAKE_SOURCE_NAME
from src.run_tracking import PipelineError, read_snowflake_metrics
from src.transform import period_filter


def verify_hive_objects(spark):
    # These are Hive Metastore objects, never objects in a caller's Unity catalog.
    spark.sql("USE CATALOG hive_metastore")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if not spark.catalog.databaseExists(C.SCHEMA_NAME):
        raise PipelineError(
            "Required Hive schema ecomm_dph is missing; run the one-time initialization.", "HIVE_LOAD_ERROR"
        )


def _assert_delta(spark, table):
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    if detail.format.lower() != "delta":
        raise PipelineError("An existing pipeline table is not a Delta table.", "DELTA_LOAD_ERROR")


def ensure_delta_table(spark, table, frame, partitions=None):
    if not spark.catalog.tableExists(table):
        writer = frame.limit(0).write.format("delta").mode("errorifexists")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.saveAsTable(table)
    _assert_delta(spark, table)


def persist_validation_results(spark, report):
    if report.persisted or not report.records:
        return
    frame = report.dataframe()
    ensure_delta_table(spark, C.VALIDATION_RESULTS_TABLE, frame)
    frame.write.format("delta").mode("append").saveAsTable(C.VALIDATION_RESULTS_TABLE)
    report.persisted = True


def load_catalog_snapshot(spark, table, frame):
    """Deactivate old active rows and insert the new snapshot in ONE Delta MERGE.

    A single sentinel matches old active rows; real rows match only their own run.
    This also makes repeating the same RUN_ID safe without dropping catalog rows.
    """
    from delta.tables import DeltaTable

    snapshot = (
        frame.withColumn("IS_ACTIVE", F.lit(True))
        .withColumn("VALID_FROM_UTC", F.col("LOAD_TIMESTAMP_UTC"))
        .withColumn("VALID_TO_UTC", F.lit(None).cast("timestamp"))
    )
    ensure_delta_table(spark, table, snapshot)
    run_id = frame.select("RUN_ID").first()[0]
    primary = "ID_STORE" if "ID_STORE" in frame.columns else "ID_INTERNO"
    real = snapshot.withColumn("_DEACTIVATE", F.lit(False))
    sentinel = snapshot.limit(1).select(*[F.lit(None).cast(f.dataType).alias(f.name) for f in snapshot.schema])
    staged = real.unionByName(sentinel.withColumn("_DEACTIVATE", F.lit(True)))
    escaped_run = run_id.replace("'", "''")
    condition = (
        f"(s._DEACTIVATE = true AND t.IS_ACTIVE = true AND t.RUN_ID <> '{escaped_run}') OR "
        f"(s._DEACTIVATE = false AND t.RUN_ID = s.RUN_ID AND t.{primary} = s.{primary})"
    )
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(staged.alias("s"), condition)
        .whenMatchedUpdate(
            condition="s._DEACTIVATE = true",
            set={"IS_ACTIVE": "false", "VALID_TO_UTC": F.lit(frame.select("LOAD_TIMESTAMP_UTC").first()[0])},
        )
        .whenNotMatchedInsert(condition="s._DEACTIVATE = false", values={c: "s.`" + c + "`" for c in snapshot.columns})
        .execute()
    )
    active = spark.table(table).where(F.col("IS_ACTIVE"))
    if active.where(F.col("RUN_ID") != run_id).limit(1).count() or active.count() != frame.count():
        raise PipelineError("Catalog active snapshot verification failed.", "DELTA_LOAD_ERROR")


def load_monthly_delta(spark, table, frame, anio, mes):
    if (
        frame.where(F.col("ANIO").isNull() | F.col("MES").isNull() | (F.col("ANIO") != anio) | (F.col("MES") != mes))
        .limit(1)
        .count()
    ):
        raise PipelineError("Delta input contains rows outside the requested period.", "DELTA_LOAD_ERROR")
    if not frame.limit(1).count():
        raise PipelineError("Refusing an empty monthly replacement.", "DELTA_LOAD_ERROR")
    ensure_delta_table(spark, table, frame, ["ANIO", "MES"])
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ANIO = {int(anio)} AND MES = {int(mes)}")
        .saveAsTable(table)
    )


def frame_metrics(frame):
    row = frame.agg(
        F.count("*").alias("ROWS"), F.coalesce(F.sum(F.col("MONTO").cast("decimal(38,6)")), F.lit(0)).alias("AMOUNT")
    ).first()
    return {"rows": int(row.ROWS), "amount": Decimal(str(row.AMOUNT))}


def delta_period_metrics(spark, table, anio, mes):
    return (
        frame_metrics(period_filter(spark.table(table), anio, mes))
        if spark.catalog.tableExists(table)
        else {"rows": 0, "amount": Decimal(0)}
    )


def compare_metrics(source, target):
    difference = abs(Decimal(str(source["amount"])) - Decimal(str(target["amount"])))
    return abs(source["rows"] - target["rows"]) <= C.ROW_COUNT_TOLERANCE and difference <= Decimal(
        str(C.MONTO_TOLERANCE)
    )


def multiset_difference(left, right):
    if set(left.columns) != set(right.columns):
        raise PipelineError("Reconciliation column sets differ.", "RECONCILIATION_ERROR")
    columns = left.columns
    aligned = right.select(*[F.col(c).cast(left.schema[c].dataType).alias(c) for c in columns])
    return left.select(*columns).exceptAll(aligned).count() + aligned.exceptAll(left.select(*columns)).count()


def reconcile_source_to_delta(source, transformed, persisted):
    expected, transformed_metrics, target = frame_metrics(source), frame_metrics(transformed), frame_metrics(persisted)
    if (
        not compare_metrics(expected, transformed_metrics)
        or not compare_metrics(expected, target)
        or multiset_difference(transformed, persisted)
    ):
        raise PipelineError("Source, transformed and Delta monthly records do not reconcile.", "RECONCILIATION_ERROR")
    return {
        "source_amount": expected["amount"],
        "target_amount": target["amount"],
        "amount_difference": target["amount"] - expected["amount"],
        "target_rows_after": target["rows"],
        "reconciliation_status": "PASS",
    }


def sql_identifier(identifier):
    if not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2}", identifier):
        raise ValueError("Unsafe SQL identifier")
    return identifier


def _sql_type(kind):
    if isinstance(kind, T.LongType):
        return "NUMBER(19,0)"
    if isinstance(kind, (T.IntegerType, T.ShortType, T.ByteType)):
        return "NUMBER(10,0)"
    if isinstance(kind, (T.DoubleType, T.FloatType)):
        return "DOUBLE"
    if isinstance(kind, T.DecimalType):
        return f"NUMBER({kind.precision},{kind.scale})"
    if isinstance(kind, T.TimestampType):
        return "TIMESTAMP_NTZ"
    if isinstance(kind, T.DateType):
        return "DATE"
    if isinstance(kind, T.BooleanType):
        return "BOOLEAN"
    if isinstance(kind, T.StringType):
        return "VARCHAR"
    raise ValueError("Unsupported Snowflake column type")


@contextlib.contextmanager
def snowflake_connection(options):
    import snowflake.connector

    conn = None
    try:
        host = options["sfURL"]
        conn = snowflake.connector.connect(
            account=host.removesuffix(".snowflakecomputing.com"),
            user=options["sfUser"],
            private_key=base64.b64decode(options["pem_private_key"], validate=True),
            warehouse=options["sfWarehouse"],
            database=options["sfDatabase"],
            schema=options["sfSchema"],
            role=options["sfRole"],
            autocommit=True,
            session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": C.PIPELINE_NAME},
            login_timeout=60,
            network_timeout=120,
        )
    except Exception:
        raise PipelineError("Snowflake authenticated connection failed.", "SNOWFLAKE_CONNECTION_ERROR") from None
    try:
        yield conn
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()


def _cursor_metrics(cursor, table, predicate):
    cursor.execute(
        f"SELECT COUNT(*), COALESCE(SUM(CAST(MONTO AS NUMBER(38,6))),0) FROM {sql_identifier(table)} WHERE {predicate}"
    )
    count, amount = cursor.fetchone()
    return {"rows": int(count), "amount": Decimal(str(amount))}


def read_snowflake_period_metrics(spark, options, anio, mes):
    result = read_snowflake_metrics(
        spark,
        options,
        f"SELECT COUNT(*) AS ROW_COUNT, COALESCE(SUM(CAST(MONTO AS NUMBER(38,6))),0) AS AMOUNT "
        f"FROM {C.TARGET_SNOWFLAKE_TABLE} WHERE ANIO={int(anio)} AND MES={int(mes)}",
    )
    return {"rows": int(result["ROW_COUNT"]), "amount": Decimal(str(result["AMOUNT"]))}


def snowflake_read(spark, options, query):
    return spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**options).option("query", query).load()


def read_pipeline_history(spark, options, anio, mes, run_id=None):
    import calendar
    import datetime as dt

    start, end = dt.date(anio, mes, 1), dt.date(anio, mes, calendar.monthrange(anio, mes)[1])
    query = (
        f"SELECT * FROM {C.PIPELINE_RUN_HISTORY_TABLE} WHERE PIPELINE_NAME='{C.PIPELINE_NAME}' "
        f"AND PERIOD_START_DATE='{start.isoformat()}' AND PERIOD_END_DATE='{end.isoformat()}'"
    )
    if run_id:
        import uuid

        query += f" AND RUN_ID='{uuid.UUID(str(run_id))}'"
    return snowflake_read(spark, options, query)


def classify_run(history, force_reprocess):
    statuses = history.select("STATUS")
    success = statuses.where(F.col("STATUS") == "SUCCEEDED").limit(1).count() > 0
    failed = statuses.where(F.col("STATUS").isin("FAILED", "VALIDATION_FAILED")).limit(1).count() > 0
    if success:
        return "REPROCESS" if force_reprocess else "DUPLICATE_PERIOD"
    return "RETRY" if failed else "FIRST_LOAD"


def _snowflake_multiset_difference(cursor, target, staging, columns, predicate):
    cols = ",".join(sql_identifier(c) for c in columns)
    left = f"SELECT {cols},COUNT(*) AS __MULTIPLICITY FROM {target} WHERE {predicate} GROUP BY {cols}"
    right = f"SELECT {cols},COUNT(*) AS __MULTIPLICITY FROM {staging} GROUP BY {cols}"
    # MINUS compares grouped multiplicities as well as values, including NULLs.
    cursor.execute(f"SELECT COUNT(*) FROM (({left} MINUS {right}) UNION ALL ({right} MINUS {left}))")
    return int(cursor.fetchone()[0])


def load_snowflake_period(spark, options, frame, anio, mes, run_id, observed_metrics=None):
    """Stage with Spark, then DELETE/INSERT/reconcile/COMMIT on ONE native session."""
    import uuid

    if (
        frame.where(F.col("ANIO").isNull() | F.col("MES").isNull() | (F.col("ANIO") != anio) | (F.col("MES") != mes))
        .limit(1)
        .count()
    ):
        raise PipelineError("Snowflake input contains rows outside the requested period.", "SNOWFLAKE_LOAD_ERROR")
    expected = frame_metrics(frame)
    if expected["rows"] == 0:
        raise PipelineError("Refusing an empty Snowflake period replacement.", "SNOWFLAKE_LOAD_ERROR")
    target = sql_identifier(C.TARGET_SNOWFLAKE_TABLE)
    staging = sql_identifier(target + "_STAGE_" + uuid.UUID(run_id).hex.upper())
    columns = [sql_identifier(c) for c in frame.columns]
    cols = ",".join(columns)
    ddl = ",".join(f"{sql_identifier(f.name)} {_sql_type(f.dataType)}" for f in frame.schema)
    predicate = f"ANIO={int(anio)} AND MES={int(mes)}"
    cleanup_status = "NOT_CREATED"
    with snowflake_connection(options) as conn:
        cursor = conn.cursor()
        stage_created, in_transaction = False, False
        try:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {target} ({ddl})")
            cursor.execute(f"SELECT {cols} FROM {target} WHERE 1=0")
            before = _cursor_metrics(cursor, target, predicate)
            if observed_metrics is not None:
                observed_metrics.update(target_rows_before=before["rows"], target_amount_before=before["amount"])
            cursor.execute(f"CREATE TRANSIENT TABLE {staging} ({ddl})")
            stage_created = True
            (
                frame.write.format(SNOWFLAKE_SOURCE_NAME)
                .options(**options)
                .option("dbtable", staging)
                .option("column_mapping", "name")
                .mode("append")
                .save()
            )
            staged = _cursor_metrics(cursor, staging, "1=1")
            if not compare_metrics(expected, staged):
                raise PipelineError("Snowflake staging metrics do not reconcile.", "RECONCILIATION_ERROR")
            stage_frame = snowflake_read(spark, options, f"SELECT {cols} FROM {staging}")
            if multiset_difference(frame, stage_frame):
                raise PipelineError("Snowflake staging contains unexpected records.", "RECONCILIATION_ERROR")
            cursor.execute("BEGIN")
            in_transaction = True
            cursor.execute(f"DELETE FROM {target} WHERE {predicate}")
            cursor.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}")
            after = _cursor_metrics(cursor, target, predicate)
            if not compare_metrics(expected, after) or _snowflake_multiset_difference(
                cursor, target, staging, columns, predicate
            ):
                raise PipelineError("Snowflake target records do not reconcile; rolling back.", "RECONCILIATION_ERROR")
            cursor.execute("COMMIT")
            in_transaction = False
        except Exception as exc:
            if in_transaction:
                with contextlib.suppress(Exception):
                    cursor.execute("ROLLBACK")
            if isinstance(exc, PipelineError):
                raise
            raise PipelineError(
                "Snowflake monthly load failed; rollback was attempted if a transaction was open.",
                "SNOWFLAKE_LOAD_ERROR",
            ) from None
        finally:
            if stage_created:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {staging}")
                    cleanup_status = "DROPPED"
                except Exception:
                    cleanup_status = "CLEANUP_REQUIRED"
            if observed_metrics is not None:
                observed_metrics.update(
                    staging_cleanup_status=cleanup_status,
                    staging_table=staging if cleanup_status == "CLEANUP_REQUIRED" else None,
                )
            with contextlib.suppress(Exception):
                cursor.close()
    return {
        "target_rows_before": before["rows"],
        "target_rows_after": after["rows"],
        "target_amount_before": before["amount"],
        "source_amount": expected["amount"],
        "target_amount": after["amount"],
        "amount_difference": after["amount"] - expected["amount"],
        "reconciliation_status": "PASS",
        "staging_cleanup_status": cleanup_status,
        "staging_table": staging if cleanup_status == "CLEANUP_REQUIRED" else None,
    }


def reconcile_delta_to_snowflake(spark, options, delta, anio, mes):
    target = snowflake_read(
        spark,
        options,
        f"SELECT {','.join(delta.columns)} FROM {C.TARGET_SNOWFLAKE_TABLE} WHERE ANIO={int(anio)} AND MES={int(mes)}",
    )
    left, right = frame_metrics(delta), frame_metrics(target)
    if not compare_metrics(left, right) or multiset_difference(delta, target):
        raise PipelineError("Committed Delta and Snowflake monthly records differ.", "RECONCILIATION_ERROR")
    return {
        "target_rows_after": right["rows"],
        "source_amount": left["amount"],
        "target_amount": right["amount"],
        "amount_difference": right["amount"] - left["amount"],
        "reconciliation_status": "PASS",
    }


def qa_validation_results(spark, anio, mes, run_id=None, logical_dataset=None):
    frame = period_filter(spark.table(C.VALIDATION_RESULTS_TABLE), anio, mes)
    if run_id:
        frame = frame.where(F.col("RUN_ID") == run_id)
    if logical_dataset:
        frame = frame.where(F.col("LOGICAL_DATASET") == logical_dataset)
    return frame


def qa_reconciliation(spark, left, right, name):
    a, b = frame_metrics(left), frame_metrics(right)
    differences = multiset_difference(left, right)
    return spark.createDataFrame(
        [
            (
                name,
                a["rows"],
                b["rows"],
                str(a["amount"]),
                str(b["amount"]),
                differences,
                "PASS" if compare_metrics(a, b) and differences == 0 else "FAIL",
            )
        ],
        "CHECK_NAME string, SOURCE_ROWS long, TARGET_ROWS long, SOURCE_AMOUNT string, TARGET_AMOUNT string, DIFFERENT_RECORDS long, STATUS string",
    )
