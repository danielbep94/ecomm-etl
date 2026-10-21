"""Explicit maintenance for the configured test tables; never imported by the ETL."""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from conf import settings as C
from src import load as L
from src import transform as V
from src.run_tracking import PipelineError


def empty_business_tables(spark):
    """Build reset schemas through the same projections used by the normal load."""
    kinds = {"int64": T.LongType(), "float64": T.DoubleType(), "object": T.StringType()}
    sources = {
        dataset: spark.createDataFrame([], T.StructType([T.StructField(c, kinds[types[c]]) for c in columns]))
        for dataset, (columns, types, _) in C.SOURCE_DEFINITIONS.items()
    }
    metadata = dict.fromkeys(["RUN_ID", "ORIGINAL_FILE_NAME", "SOURCE_PATH", "FILE_FINGERPRINT", "FILE_YEAR"])
    catalogs = {}
    for dataset in C.SNOWFLAKE_CATALOG_TABLES:
        bu = C.EXPECTED_STATIC_FILES[dataset].get("catalog_bu")
        frame = V.add_operational_metadata(
            sources[dataset], {**metadata, "LOGICAL_DATASET": dataset}, None, None, catalog_bu=bu
        )
        catalogs[dataset] = L.catalog_snapshot(frame)
    fact = V.star_fact(
        V.add_operational_metadata(
            sources["ECOMMERCE_FACT"].withColumn("NORMALIZED_BU", F.lit(None).cast("string")),
            {**metadata, "LOGICAL_DATASET": "ECOMMERCE_FACT"},
            None,
            None,
        )
    )
    curated = V.enrich_fact(fact, sources["EDP_CATALOG"], sources["WATERS_CATALOG"], sources["STORE_CATALOG"])
    delta = [
        (C.CATALOG_EDP_TABLE, catalogs["EDP_CATALOG"], None),
        (C.CATALOG_WATERS_TABLE, catalogs["WATERS_CATALOG"], None),
        (C.CATALOG_STORE_TABLE, catalogs["STORE_CATALOG"], None),
        (C.FACT_STAGING_TABLE, fact, ["ANIO", "MES"]),
        (C.FACT_CURATED_TABLE, curated, ["ANIO", "MES"]),
    ]
    snowflake = {C.SNOWFLAKE_CATALOG_TABLES[dataset]: frame for dataset, frame in catalogs.items()}
    snowflake[C.TARGET_SNOWFLAKE_TABLE] = fact
    return delta, snowflake


def reset_business_test_tables(spark, options, logger):
    """Explicit test maintenance: empty/recreate the nine configured business tables.

    Destructive and separate from ETL; stop every writer first. Tracking, validation
    history and source files are outside this operation. Completed DDL/overwrites
    cannot be rolled back across stores; a failed reset must be rerun to completion.
    """
    L.verify_hive_objects(spark)
    delta, snowflake = empty_business_tables(spark)
    names = [L.sql_identifier(table) for table, _, _ in delta] + [L.sql_identifier(table) for table in snowflake]

    def canonical(name):
        return ("hive_metastore." + name if name.count(".") == 1 else name).upper()

    protected = {canonical(C.VALIDATION_RESULTS_TABLE), canonical(C.PIPELINE_RUN_HISTORY_TABLE)}
    targets = {canonical(name) for name in names}
    if len(targets) != 9 or protected & targets:
        raise PipelineError(
            "Reset targets must be nine distinct business tables, excluding shared history.", "TEST_RESET_ERROR"
        )
    # Detect an unexpected provider before resetting either system.
    for table, _, _ in delta:
        if spark.catalog.tableExists(table):
            L._assert_delta(spark, table)
    completed, target, reset_started = [], "SNOWFLAKE_CONNECTION", False
    try:
        with L.snowflake_connection(options) as conn:
            cursor = conn.cursor()
            try:
                for target, frame in snowflake.items():
                    logger.event("TEST_RESET", "RESETTING", TABLE_NAME=target, SYSTEM="SNOWFLAKE")
                    ddl = ",".join(f"{L.sql_identifier(f.name)} {L._sql_type(f.dataType)}" for f in frame.schema)
                    reset_started = True
                    cursor.execute(f"CREATE OR REPLACE TABLE {target} ({ddl}) COPY GRANTS")
                    L.ensure_snowflake_table(cursor, target, frame)
                    if L._cursor_count(cursor, target):
                        raise PipelineError("Reset target is not empty; stop other writers.", "TEST_RESET_ERROR")
                    completed.append({"SYSTEM": "SNOWFLAKE", "TABLE_NAME": target, "ROWS_AFTER": 0})
                    logger.event("TEST_RESET", "SUCCEEDED", TABLE_NAME=target, SYSTEM="SNOWFLAKE", ROWS_AFTER=0)
            finally:
                cursor.close()
        for target, frame, partitions in delta:
            logger.event("TEST_RESET", "RESETTING", TABLE_NAME=target, SYSTEM="DELTA")
            writer = (
                frame.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("partitionOverwriteMode", "static")
            )
            if partitions:
                writer = writer.partitionBy(*partitions)
            writer.saveAsTable(target)
            L.ensure_delta_table(spark, target, frame, partitions)
            if spark.table(target).limit(1).count():
                raise PipelineError("Reset target is not empty; stop other writers.", "TEST_RESET_ERROR")
            completed.append({"SYSTEM": "DELTA", "TABLE_NAME": target, "ROWS_AFTER": 0})
            logger.event("TEST_RESET", "SUCCEEDED", TABLE_NAME=target, SYSTEM="DELTA", ROWS_AFTER=0)
    except Exception as exc:
        logger.event(
            "TEST_RESET",
            "FAILED",
            TABLE_NAME=target,
            COMPLETED_TABLES=len(completed),
            ERROR_TYPE=getattr(exc, "error_type", type(exc).__name__),
            ERROR_MESSAGE=str(exc),
        )
        if not reset_started:
            detail = str(exc) if isinstance(exc, PipelineError) else "Check the reset log for the connection error."
            raise PipelineError(
                f"Test reset aborted before changing any table. {detail}",
                getattr(exc, "error_type", "TEST_RESET_ERROR"),
            ) from None
        raise PipelineError(
            f"Test reset stopped at {target} after {len(completed)} verified tables; completed resets are not rolled back. Stop writers and rerun reset.",
            "TEST_RESET_ERROR",
        ) from None
    return completed
