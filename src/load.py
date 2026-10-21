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
from src.transform import period_bounds, period_filter, period_predicate


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


def _delta_legacy_schema(actual, expected):
    old = {f.name: f.dataType for f in actual}
    new = {f.name: f.dataType for f in expected}
    return (
        not (set(new) - set(old))
        and (set(old) - set(new)) <= {"SKU", "BU", "PRODUCT_SKU"}
        and all(
            old[name] == kind
            or (name == "UNIDADES" and isinstance(old[name], T.DoubleType) and isinstance(kind, T.LongType))
            for name, kind in new.items()
        )
    )


def ensure_delta_table(spark, table, frame, partitions=None):
    exists = spark.catalog.tableExists(table)
    mode, migrate = "errorifexists", False
    if exists:
        _assert_delta(spark, table)
        actual = spark.table(table)
        if {f.name: f.dataType for f in actual.schema} == {f.name: f.dataType for f in frame.schema}:
            return
        business = {
            C.CATALOG_EDP_TABLE,
            C.CATALOG_WATERS_TABLE,
            C.CATALOG_STORE_TABLE,
            C.FACT_STAGING_TABLE,
            C.FACT_CURATED_TABLE,
        }
        if table not in business or not _delta_legacy_schema(actual.schema, frame.schema) or actual.limit(1).count():
            raise PipelineError(
                "Delta table schema differs from the approved contract; populated tables require an explicit migration.",
                "DELTA_LOAD_ERROR",
            )
        mode, migrate = "overwrite", True
    writer = frame.limit(0).write.format("delta").mode(mode)
    if migrate:
        writer = writer.option("overwriteSchema", "true")
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
        frame.where(
            F.col("ANIO").isNull()
            | F.col("MES").isNull()
            | ~F.col("MES").between(1, 12)
            | ~F.expr(period_predicate(anio, mes))
        )
        .limit(1)
        .count()
    ):
        raise PipelineError("Delta input contains rows outside the requested period.", "DELTA_LOAD_ERROR")
    if not frame.limit(1).count():
        raise PipelineError("Refusing an empty period replacement.", "DELTA_LOAD_ERROR")
    ensure_delta_table(spark, table, frame, ["ANIO", "MES"])
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", period_predicate(anio, mes, spaced=True))
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
        f"FROM {C.TARGET_SNOWFLAKE_TABLE} WHERE {period_predicate(anio, mes)}",
    )
    return {"rows": int(result["ROW_COUNT"]), "amount": Decimal(str(result["AMOUNT"]))}


def snowflake_read(spark, options, query):
    return spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**options).option("query", query).load()


def read_pipeline_history(spark, options, anio, mes, run_id=None):
    start, end = period_bounds(anio, mes)
    query = (
        f"SELECT * FROM {C.PIPELINE_RUN_HISTORY_TABLE} WHERE PIPELINE_NAME='{C.PIPELINE_NAME}' "
        f"AND PERIOD_START_DATE<='{end.isoformat()}' AND PERIOD_END_DATE>='{start.isoformat()}'"
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


def _snowflake_contract_type_matches(field, actual):
    kind, precision, scale = actual
    kind = kind.upper()
    if isinstance(field.dataType, (T.LongType, T.IntegerType, T.ShortType, T.ByteType)):
        digits = 19 if isinstance(field.dataType, T.LongType) else 10
        return (
            kind in {"NUMBER", "FIXED", "DECIMAL", "NUMERIC"} and int(scale or 0) == 0 and int(precision or 0) >= digits
        )
    if isinstance(field.dataType, (T.DoubleType, T.FloatType)):
        return kind in {"FLOAT", "DOUBLE", "REAL"}
    if isinstance(field.dataType, T.StringType):
        return kind in {"TEXT", "VARCHAR", "STRING"}
    return kind == _sql_type(field.dataType)


def ensure_snowflake_table(cursor, table, frame):
    """Create/validate a business table; upgrade only known, empty legacy schemas."""
    table = sql_identifier(table)
    database, schema, name = table.split(".")
    ddl = ",".join(f"{sql_identifier(f.name)} {_sql_type(f.dataType)}" for f in frame.schema)
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({ddl})")
    cursor.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
        f"FROM {database}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA='{schema.upper()}' AND TABLE_NAME='{name.upper()}' ORDER BY ORDINAL_POSITION"
    )
    actual = {row[0].upper(): (row[1], row[2], row[3]) for row in cursor.fetchall()}

    def compatible():
        return set(actual) == set(frame.columns) and all(
            _snowflake_contract_type_matches(field, actual[field.name]) for field in frame.schema
        )

    migrated = False
    if not compatible():
        business = set(C.SNOWFLAKE_CATALOG_TABLES.values()) | {C.TARGET_SNOWFLAKE_TABLE}
        legacy = (
            not (set(frame.columns) - set(actual))
            and (set(actual) - set(frame.columns)) <= {"SKU", "BU", "PRODUCT_SKU"}
            and all(
                _snowflake_contract_type_matches(field, actual[field.name])
                or (
                    field.name == "UNIDADES"
                    and isinstance(field.dataType, T.LongType)
                    and actual[field.name][0].upper() in {"FLOAT", "DOUBLE", "REAL"}
                )
                for field in frame.schema
            )
        )
        if table in business and legacy and _cursor_count(cursor, table) == 0:
            # DDL is outside the data transaction. Preserve grants; never replace populated objects.
            cursor.execute(f"CREATE OR REPLACE TABLE {table} ({ddl}) COPY GRANTS")
            migrated = True
        else:
            raise PipelineError(
                f"Existing Snowflake table {table} has an incompatible schema. Populated or unknown schemas require an explicit migration.",
                "SNOWFLAKE_LOAD_ERROR",
            )
    return migrated


def _cursor_count(cursor, table, predicate="1=1"):
    cursor.execute(f"SELECT COUNT(*) FROM {sql_identifier(table)} WHERE {predicate}")
    return int(cursor.fetchone()[0])


def _validate_catalog_history(frame, dataset, run_id):
    primary = "ID_STORE" if dataset == "STORE_CATALOG" else "ID_INTERNO"
    required = set(C.SOURCE_DEFINITIONS[dataset][0]) | {"RUN_ID", "IS_ACTIVE", "VALID_FROM_UTC", "VALID_TO_UTC"}
    if not required.issubset(frame.columns):
        raise PipelineError("Snowflake catalogs require the complete Delta snapshot schema.", "SNOWFLAKE_LOAD_ERROR")
    if (
        frame.where(
            F.col("RUN_ID").isNull()
            | (F.trim(F.col("RUN_ID")) == "")
            | F.col(primary).isNull()
            | F.col("IS_ACTIVE").isNull()
            | F.col("VALID_FROM_UTC").isNull()
        )
        .limit(1)
        .count()
    ):
        raise PipelineError("Catalog snapshot keys or validity metadata are missing.", "SNOWFLAKE_LOAD_ERROR")
    keys = [primary] if dataset == "STORE_CATALOG" else ["ID_INTERNO", "UPC", "ID_IMPORT"]
    for key in keys:
        if frame.groupBy("RUN_ID", key).count().where(F.col("count") > 1).limit(1).count():
            raise PipelineError(
                f"Catalog history contains duplicate versioned dimension keys: {dataset}.{key}.",
                "SNOWFLAKE_LOAD_ERROR",
            )
    active_runs = [row[0] for row in frame.where("IS_ACTIVE = true").select("RUN_ID").distinct().limit(2).collect()]
    if active_runs != [run_id]:
        raise PipelineError("The active dimension snapshot does not match this run.", "SNOWFLAKE_LOAD_ERROR")


def _cursor_star_relationships(cursor):
    """Check the entire fact so a dimension update cannot orphan older months."""
    fact = sql_identifier(C.TARGET_SNOWFLAKE_TABLE)
    edp = sql_identifier(C.SNOWFLAKE_CATALOG_TABLES["EDP_CATALOG"])
    waters = sql_identifier(C.SNOWFLAKE_CATALOG_TABLES["WATERS_CATALOG"])
    stores = sql_identifier(C.SNOWFLAKE_CATALOG_TABLES["STORE_CATALOG"])
    before = _cursor_count(cursor, fact)
    cursor.execute(
        "SELECT COUNT(*) AS JOINED_ROWS, "
        "COALESCE(SUM(IFF((f.NORMALIZED_BU='EDP' AND e.ID_INTERNO IS NOT NULL) OR "
        "(f.NORMALIZED_BU='WATERS' AND w.ID_INTERNO IS NOT NULL),0,1)),0) AS MISSING_PRODUCTS, "
        "COALESCE(SUM(IFF(s.ID_STORE IS NULL,1,0)),0) AS MISSING_STORES "
        f"FROM {fact} f LEFT JOIN {edp} e ON f.RUN_ID=e.RUN_ID AND f.ID_INTERNO=e.ID_INTERNO AND f.NORMALIZED_BU='EDP' "
        f"LEFT JOIN {waters} w ON f.RUN_ID=w.RUN_ID AND f.ID_INTERNO=w.ID_INTERNO AND f.NORMALIZED_BU='WATERS' "
        f"LEFT JOIN {stores} s ON f.RUN_ID=s.RUN_ID AND f.ID_STORE=s.ID_STORE"
    )
    joined, missing_products, missing_stores = (int(value) for value in cursor.fetchone())
    if joined != before or missing_products or missing_stores:
        raise PipelineError(
            "Snowflake star joins multiply facts or leave missing historical dimension keys.", "RECONCILIATION_ERROR"
        )
    return {
        "fact_rows": before,
        "joined_rows": joined,
        "missing_product_rows": missing_products,
        "missing_store_rows": missing_stores,
        "status": "PASS",
    }


def load_snowflake_period(spark, options, frame, anio, mes, run_id, observed_metrics=None, catalog_frames=None):
    """Stage three dimension histories and one thin fact; commit all four together."""
    import uuid

    from src.transform import star_fact

    if catalog_frames is None or set(catalog_frames) != set(C.SNOWFLAKE_CATALOG_TABLES):
        raise PipelineError(
            "All three catalog histories are required for the Snowflake star load.", "SNOWFLAKE_LOAD_ERROR"
        )
    frame = star_fact(frame)
    if (
        frame.where(
            F.col("ANIO").isNull()
            | F.col("MES").isNull()
            | ~F.col("MES").between(1, 12)
            | ~F.expr(period_predicate(anio, mes))
            | F.col("RUN_ID").isNull()
            | (F.col("RUN_ID") != run_id)
        )
        .limit(1)
        .count()
    ):
        raise PipelineError("Snowflake fact input contains a different period or run.", "SNOWFLAKE_LOAD_ERROR")
    expected = frame_metrics(frame)
    if expected["rows"] == 0:
        raise PipelineError("Refusing an empty Snowflake period replacement.", "SNOWFLAKE_LOAD_ERROR")
    for dataset, catalog in catalog_frames.items():
        _validate_catalog_history(catalog, dataset, run_id)
    payloads = {**catalog_frames, "ECOMMERCE_FACT": frame}
    targets = {**C.SNOWFLAKE_CATALOG_TABLES, "ECOMMERCE_FACT": C.TARGET_SNOWFLAKE_TABLE}
    suffix = uuid.UUID(run_id).hex.upper()
    stages = {dataset: sql_identifier(table + "_STAGE_" + suffix) for dataset, table in targets.items()}
    predicate = period_predicate(anio, mes)
    counts = {dataset: catalog.count() for dataset, catalog in catalog_frames.items()}
    counts["ECOMMERCE_FACT"] = expected["rows"]
    catalog_metrics, cleanup, created = {}, {}, []
    schema_updates = []
    with snowflake_connection(options) as conn:
        cursor = conn.cursor()
        in_transaction = False
        try:
            # All DDL is outside the explicit transaction. Only DML follows BEGIN.
            for dataset, payload in payloads.items():
                if ensure_snowflake_table(cursor, targets[dataset], payload):
                    schema_updates.append(targets[dataset])
                    if observed_metrics is not None:
                        observed_metrics["empty_schema_updates"] = list(schema_updates)
            before = _cursor_metrics(cursor, C.TARGET_SNOWFLAKE_TABLE, predicate)
            if observed_metrics is not None:
                observed_metrics.update(target_rows_before=before["rows"], target_amount_before=before["amount"])
            for dataset, payload in payloads.items():
                ddl = ",".join(f"{sql_identifier(f.name)} {_sql_type(f.dataType)}" for f in payload.schema)
                cursor.execute(f"CREATE TRANSIENT TABLE {stages[dataset]} ({ddl})")
                created.append(dataset)
                (
                    payload.write.format(SNOWFLAKE_SOURCE_NAME)
                    .options(**options)
                    .option("dbtable", stages[dataset])
                    .option("column_mapping", "name")
                    .mode("append")
                    .save()
                )
                if _cursor_count(cursor, stages[dataset]) != counts[dataset]:
                    raise PipelineError(
                        "Snowflake staging row count differs from validated input.", "RECONCILIATION_ERROR"
                    )
                columns = ",".join(sql_identifier(c) for c in payload.columns)
                staged_frame = snowflake_read(spark, options, f"SELECT {columns} FROM {stages[dataset]}")
                if multiset_difference(payload, staged_frame):
                    raise PipelineError("Snowflake staging contains unexpected records.", "RECONCILIATION_ERROR")
            staged_metrics = _cursor_metrics(cursor, stages["ECOMMERCE_FACT"], "1=1")
            if not compare_metrics(expected, staged_metrics):
                raise PipelineError("Snowflake fact staging metrics do not reconcile.", "RECONCILIATION_ERROR")
            cursor.execute("BEGIN")
            in_transaction = True
            for dataset, catalog in catalog_frames.items():
                target, stage = targets[dataset], stages[dataset]
                primary = "ID_STORE" if dataset == "STORE_CATALOG" else "ID_INTERNO"
                columns = [sql_identifier(c) for c in catalog.columns]
                updates = ",".join(f"t.{c}=s.{c}" for c in columns if c not in {"RUN_ID", primary})
                cursor.execute(
                    f"MERGE INTO {target} t USING {stage} s ON t.RUN_ID=s.RUN_ID AND t.{primary}=s.{primary} "
                    f"WHEN MATCHED THEN UPDATE SET {updates} "
                    f"WHEN NOT MATCHED THEN INSERT ({','.join(columns)}) VALUES ({','.join('s.' + c for c in columns)})"
                )
                actual_count = _cursor_count(cursor, target)
                if actual_count != counts[dataset] or _snowflake_multiset_difference(
                    cursor, target, stage, columns, "1=1"
                ):
                    raise PipelineError(
                        "Snowflake dimension history differs from its Delta history.", "RECONCILIATION_ERROR"
                    )
                catalog_metrics[dataset] = {
                    "source_rows": counts[dataset],
                    "target_rows": actual_count,
                    "status": "PASS",
                }
            target, stage = targets["ECOMMERCE_FACT"], stages["ECOMMERCE_FACT"]
            columns = [sql_identifier(c) for c in frame.columns]
            cols = ",".join(columns)
            cursor.execute(f"DELETE FROM {target} WHERE {predicate}")
            cursor.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}")
            after = _cursor_metrics(cursor, target, predicate)
            if not compare_metrics(expected, after) or _snowflake_multiset_difference(
                cursor, target, stage, columns, predicate
            ):
                raise PipelineError("Snowflake fact records do not reconcile; rolling back.", "RECONCILIATION_ERROR")
            relationships = _cursor_star_relationships(cursor)
            cursor.execute("COMMIT")
            in_transaction = False
        except Exception as exc:
            if in_transaction:
                with contextlib.suppress(Exception):
                    cursor.execute("ROLLBACK")
            if isinstance(exc, PipelineError):
                raise
            raise PipelineError(
                "Snowflake star load failed; rollback was attempted if a transaction was open.", "SNOWFLAKE_LOAD_ERROR"
            ) from None
        finally:
            for dataset in created:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {stages[dataset]}")
                    cleanup[dataset] = "DROPPED"
                except Exception:
                    cleanup[dataset] = "CLEANUP_REQUIRED"
            if observed_metrics is not None:
                observed_metrics.update(
                    catalog_metrics=catalog_metrics,
                    staging_cleanup=cleanup,
                    remaining_staging_tables=[stages[d] for d in created if cleanup[d] == "CLEANUP_REQUIRED"],
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
        "empty_schema_updates": schema_updates,
        "catalog_metrics": catalog_metrics,
        "star_relationships": relationships,
        "staging_cleanup": cleanup,
        "remaining_staging_tables": [stages[d] for d in created if cleanup[d] == "CLEANUP_REQUIRED"],
    }


def reconcile_delta_to_snowflake(spark, options, delta, anio, mes, catalog_frames=None):
    from src.transform import star_fact

    expected = star_fact(delta)
    target = snowflake_read(
        spark,
        options,
        f"SELECT {','.join(expected.columns)} FROM {C.TARGET_SNOWFLAKE_TABLE} WHERE {period_predicate(anio, mes)}",
    )
    left, right = frame_metrics(expected), frame_metrics(target)
    if not compare_metrics(left, right) or multiset_difference(expected, target):
        raise PipelineError("Committed Delta and Snowflake monthly facts differ.", "RECONCILIATION_ERROR")
    for dataset, catalog in (catalog_frames or {}).items():
        remote = snowflake_read(
            spark, options, f"SELECT {','.join(catalog.columns)} FROM {C.SNOWFLAKE_CATALOG_TABLES[dataset]}"
        )
        if multiset_difference(catalog, remote):
            raise PipelineError("Committed Delta and Snowflake dimensions differ.", "RECONCILIATION_ERROR")
    return {
        "target_rows_after": right["rows"],
        "source_amount": left["amount"],
        "target_amount": right["amount"],
        "amount_difference": right["amount"] - left["amount"],
        "reconciliation_status": "PASS",
    }


def qa_snowflake_catalogs(spark, options, catalogs, id_interno=None, id_store=None):
    from functools import reduce

    frames, remotes = [], {}
    for dataset, expected in catalogs.items():
        remote = snowflake_read(
            spark, options, f"SELECT {','.join(expected.columns)} FROM {C.SNOWFLAKE_CATALOG_TABLES[dataset]}"
        )
        ids = expected.select("RUN_ID").distinct()
        remote = remote.join(ids, "RUN_ID", "inner")
        key, value = ("ID_STORE", id_store) if dataset == "STORE_CATALOG" else ("ID_INTERNO", id_interno)
        if value is not None:
            remote = remote.where(F.col(key) == value)
        differences = multiset_difference(expected, remote)
        rows, target_rows = expected.count(), remote.count()
        frames.append(
            spark.createDataFrame(
                [(dataset, rows, target_rows, differences, "PASS" if differences == 0 else "FAIL")],
                "LOGICAL_DATASET string, DELTA_ROWS long, SNOWFLAKE_ROWS long, DIFFERENT_RECORDS long, STATUS string",
            )
        )
        remotes[dataset] = remote
    return reduce(lambda a, b: a.unionByName(b), frames), remotes


def qa_snowflake_relationships(spark, fact, catalogs):
    from src.transform import versioned_coverage_metrics

    result = versioned_coverage_metrics(
        fact, catalogs["EDP_CATALOG"], catalogs["WATERS_CATALOG"], catalogs["STORE_CATALOG"]
    )
    good = result["FACT_ROWS"] == result["ROWS_AFTER_PRODUCT_JOIN"] == result["ROWS_AFTER_STORE_JOIN"] and all(
        result[k] == 0 for k in ["UNMATCHED_PRODUCT_ROWS", "UNMATCHED_STORE_ROWS", "UNKNOWN_BU_ROWS"]
    )
    return spark.createDataFrame([{**result, "STATUS": "PASS" if good else "FAIL"}])


def qa_validation_results(spark, anio, mes, run_id=None, logical_dataset=None):
    frame = spark.table(C.VALIDATION_RESULTS_TABLE).where(F.col("ANIO") == anio)
    if mes != "ALL":
        frame = frame.where((F.col("MES") == mes) | F.col("MES").isNull())
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
