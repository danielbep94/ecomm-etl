"""Pure Spark validation, transformations and persisted-data QA; no table writes."""

import math
import re
from decimal import Decimal, InvalidOperation
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T

from conf import settings as C
from src.run_tracking import PipelineError, metrics_to_json, utc_now


class ValidationError(PipelineError):
    pass


VALIDATION_SCHEMA = T.StructType(
    [
        T.StructField("RUN_ID", T.StringType()),
        T.StructField("PIPELINE_NAME", T.StringType()),
        T.StructField("ANIO", T.IntegerType()),
        T.StructField("MES", T.IntegerType()),
        *[
            T.StructField(name, T.StringType())
            for name in (
                "LOGICAL_DATASET",
                "SOURCE_FILE_NAME",
                "VALIDATION_CATEGORY",
                "VALIDATION_RULE",
                "VALIDATION_STATUS",
                "SEVERITY",
                "COLUMN_NAME",
                "KEY_VALUE",
            )
        ],
        T.StructField("AFFECTED_ROWS", T.LongType()),
        T.StructField("ERROR_DESCRIPTION", T.StringType()),
        T.StructField("SAMPLE_RECORDS_JSON", T.StringType()),
        T.StructField("VALIDATION_TIMESTAMP_UTC", T.TimestampType()),
    ]
)
ERROR_TYPES = {
    "FILE_DISCOVERY": "FILE_DISCOVERY_ERROR",
    "FILE_NAME": "FILE_NAME_ERROR",
    "SCHEMA": "SCHEMA_VALIDATION_ERROR",
    "TYPE": "TYPE_VALIDATION_ERROR",
    "MANDATORY": "MANDATORY_FIELD_VALIDATION_ERROR",
    "DUPLICATE": "DUPLICATE_VALIDATION_ERROR",
    "CROSS_CATALOG": "CROSS_CATALOG_VALIDATION_ERROR",
    "REFERENTIAL": "REFERENTIAL_VALIDATION_ERROR",
    "PERIOD": "PERIOD_VALIDATION_ERROR",
    "DUPLICATE_PERIOD": "DUPLICATE_PERIOD_VALIDATION_ERROR",
}


class ValidationReport:
    def __init__(self, spark, run_id, anio=None, mes=None):
        self.spark, self.run_id, self.anio, self.mes = spark, run_id, anio, mes
        self.records, self.detail_frames = [], []
        self.persisted = False

    def add(
        self,
        dataset,
        rule,
        affected=0,
        *,
        category="MANDATORY",
        severity="ERROR",
        column=None,
        description=None,
        sample=None,
        source=None,
    ):
        if severity not in {"ERROR", "WARNING"}:
            raise ValueError("Validation severity must be ERROR or WARNING")
        self.persisted = False
        record = {
            "RUN_ID": self.run_id,
            "PIPELINE_NAME": C.PIPELINE_NAME,
            "ANIO": self.anio,
            "MES": self.mes,
            "LOGICAL_DATASET": dataset,
            "SOURCE_FILE_NAME": source,
            "VALIDATION_CATEGORY": category,
            "VALIDATION_RULE": rule,
            "VALIDATION_STATUS": "PASS" if affected == 0 else "FAIL" if severity == "ERROR" else "WARNING",
            "SEVERITY": severity,
            "COLUMN_NAME": column,
            "KEY_VALUE": None,
            "AFFECTED_ROWS": int(affected),
            "ERROR_DESCRIPTION": description,
            "SAMPLE_RECORDS_JSON": metrics_to_json(sample or []),
            "VALIDATION_TIMESTAMP_UTC": utc_now(),
        }
        self.records.append(record)

    def add_detail(self, frame, keys, *, dataset, rule, category, source=None, severity="ERROR"):
        """Keep every offending key distributed; only bounded samples reach the driver."""
        values = {
            "RUN_ID": self.run_id,
            "PIPELINE_NAME": C.PIPELINE_NAME,
            "ANIO": self.anio,
            "MES": self.mes,
            "LOGICAL_DATASET": dataset,
            "SOURCE_FILE_NAME": source,
            "VALIDATION_CATEGORY": category + "_DETAIL",
            "VALIDATION_RULE": rule,
            "VALIDATION_STATUS": "FAIL" if severity == "ERROR" else "WARNING",
            "SEVERITY": severity,
            "COLUMN_NAME": "+".join(keys),
        }
        key = F.to_json(F.struct(*[F.col(c) for c in keys]), {"ignoreNullFields": "false"})
        columns = []
        for field in VALIDATION_SCHEMA:
            name = field.name
            if name == "KEY_VALUE":
                expr = key
            elif name == "AFFECTED_ROWS":
                expr = F.col("N")
            elif name == "ERROR_DESCRIPTION":
                expr = F.col("CLASSIFICATION") if "CLASSIFICATION" in frame.columns else F.lit(rule)
            elif name == "SAMPLE_RECORDS_JSON":
                expr = F.concat(F.lit("["), key, F.lit("]"))
            elif name == "VALIDATION_TIMESTAMP_UTC":
                expr = F.lit(utc_now())
            else:
                expr = F.lit(values.get(name))
            columns.append(expr.cast(field.dataType).alias(name))
        self.detail_frames.append(frame.select(*columns))
        self.persisted = False

    @property
    def failed(self):
        return any(r["VALIDATION_STATUS"] == "FAIL" for r in self.records)

    @property
    def error_type(self):
        failed = [r for r in self.records if r["VALIDATION_STATUS"] == "FAIL"]
        if any(r["VALIDATION_CATEGORY"] == "DUPLICATE_PERIOD" for r in failed):
            return ERROR_TYPES["DUPLICATE_PERIOD"]
        if any(r["VALIDATION_CATEGORY"] == "REFERENTIAL" and "MULTIPLICATION" in r["VALIDATION_RULE"] for r in failed):
            return ERROR_TYPES["REFERENTIAL"]
        return ERROR_TYPES.get(failed[0]["VALIDATION_CATEGORY"], "SCHEMA_VALIDATION_ERROR") if failed else None

    def metrics(self):
        return {
            "total_rules": len(self.records),
            "passed_rules": sum(r["VALIDATION_STATUS"] == "PASS" for r in self.records),
            "warning_rules": sum(r["VALIDATION_STATUS"] == "WARNING" for r in self.records),
            "failed_rules": sum(r["VALIDATION_STATUS"] == "FAIL" for r in self.records),
        }

    def dataframe(self):
        frames = [self.spark.createDataFrame(self.records, VALIDATION_SCHEMA)] + self.detail_frames
        return reduce(lambda a, b: a.unionByName(b), frames)

    def log(self, logger):
        for row in self.records:
            logger.event(
                "VALIDATION",
                row["VALIDATION_STATUS"],
                row["LOGICAL_DATASET"],
                row["SOURCE_FILE_NAME"],
                RULE=row["VALIDATION_RULE"],
                AFFECTED_ROWS=row["AFFECTED_ROWS"],
                SEVERITY=row["SEVERITY"],
            )


def validate_parameters(anio, mes, force_reprocess=False):
    if isinstance(anio, bool) or not re.fullmatch(r"[1-9]\d{3}", str(anio).strip()):
        raise ValidationError("ANIO must be a four-digit integer.", "PERIOD_VALIDATION_ERROR")
    if isinstance(mes, bool) or not re.fullmatch(r"\d{1,2}", str(mes).strip()) or not 1 <= int(mes) <= 12:
        raise ValidationError("MES must be an integer between 1 and 12.", "PERIOD_VALIDATION_ERROR")
    if isinstance(force_reprocess, bool):
        force = force_reprocess
    elif str(force_reprocess).strip().lower() in {"true", "false"}:
        force = str(force_reprocess).strip().lower() == "true"
    else:
        raise ValidationError("FORCE_REPROCESS must be true or false.", "PERIOD_VALIDATION_ERROR")
    return int(anio), int(mes), force


def validate_headers(headers, expected, report, dataset, source):
    normalized = [name.lstrip("\ufeff").strip() if i == 0 else name.strip() for i, name in enumerate(headers)]
    duplicates = sorted({c for c in normalized if normalized.count(c) > 1})
    missing, unexpected = sorted(set(expected) - set(normalized)), sorted(set(normalized) - set(expected))
    report.add(dataset, "EMPTY_HEADER_NAMES", normalized.count(""), category="SCHEMA", source=source)
    report.add(dataset, "DUPLICATE_HEADER_NAMES", len(duplicates), category="SCHEMA", source=source, sample=duplicates)
    report.add(dataset, "MISSING_COLUMNS", len(missing), category="SCHEMA", source=source, sample=missing)
    report.add(dataset, "UNEXPECTED_COLUMNS", len(unexpected), category="SCHEMA", source=source, sample=unexpected)
    valid = not (duplicates or missing or unexpected or "" in normalized)
    report.add(
        dataset,
        "HEADER_ORDER",
        int(valid and normalized != expected),
        category="SCHEMA",
        severity=C.HEADER_ORDER_SEVERITY,
        source=source,
    )
    report.add(
        dataset,
        "HEADER_WHITESPACE",
        sum(a != b for a, b in zip(headers, normalized)),
        category="SCHEMA",
        severity="WARNING",
        source=source,
    )
    return normalized if valid else None


def parse_numeric(value, kind):
    """Canonical string and status, avoiding lossy double casts for 64-bit IDs."""
    if value is None or str(value).strip().upper() in C.MISSING_VALUES:
        return None, "MISSING"
    text = str(value).strip()
    if len(text) > 1024 or not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", text):
        return None, "INVALID"
    try:
        number = Decimal(text)
        if kind == "int64":
            if not -(2**63) <= number < 2**63 or number != number.to_integral_value():
                return None, "INVALID"
            return str(int(number)), "VALID"
        number = float(number)
        return (repr(number), "VALID") if math.isfinite(number) else (None, "INVALID")
    except (InvalidOperation, ValueError, OverflowError):
        return None, "INVALID"


def _sample(frame, columns):
    cols = list(dict.fromkeys(c for c in columns if c in frame.columns))
    return [row.asDict() for row in frame.select(*cols).limit(C.VALIDATION_SAMPLE_LIMIT).collect()] if cols else []


def _count(frame):
    return frame.count()


def validate_and_cast_source(raw, dataset, report, source, cache_frames):
    columns, types, mandatory = C.SOURCE_DEFINITIONS[dataset]
    working = raw
    numeric_columns = [c for c in columns if types[c] != "object"]
    parse_schema = T.StructType([T.StructField("value", T.StringType()), T.StructField("status", T.StringType())])
    for column in numeric_columns:
        kind = types[column]

        def parser(value, expected_kind=kind):
            return parse_numeric(value, expected_kind)

        working = working.withColumn("_PARSE_" + column, F.udf(parser, parse_schema)(F.col(column)))
    working = working.cache()
    cache_frames.append(working)
    expressions = [
        F.count("*").alias("ROWS"),
        F.sum(F.when(F.col("_CSV_ERROR").isNotNull(), 1).otherwise(0)).alias("CSV"),
    ]
    for column in columns:
        missing = F.col(column).isNull() | F.upper(F.trim(F.col(column))).isin(*C.MISSING_VALUES)
        if column in mandatory:
            expressions.append(F.sum(F.when(missing, 1).otherwise(0)).alias("MISSING_" + column))
        if column in numeric_columns:
            invalid = F.col("_PARSE_" + column + ".status") == "INVALID"
            expressions.append(F.sum(F.when(invalid, 1).otherwise(0)).alias("INVALID_" + column))
    stats = working.agg(*expressions).first().asDict()
    report.add(dataset, "EMPTY_SOURCE", int(stats["ROWS"] == 0), category="MANDATORY", source=source)
    report.add(dataset, "CSV_RECORD_SHAPE", stats["CSV"] or 0, category="SCHEMA", source=source)
    bad_types = bool(stats["CSV"])
    for column in columns:
        if column in mandatory:
            n = stats["MISSING_" + column] or 0
            condition = F.col(column).isNull() | F.upper(F.trim(F.col(column))).isin(*C.MISSING_VALUES)
            report.add(
                dataset,
                "MANDATORY_" + column,
                n,
                category="MANDATORY",
                column=column,
                source=source,
                sample=_sample(working.where(condition), [column]) if n else None,
            )
        if column in numeric_columns:
            n = stats["INVALID_" + column] or 0
            report.add(
                dataset,
                "TYPE_" + column,
                n,
                category="TYPE",
                column=column,
                source=source,
                sample=_sample(working.where(F.col("_PARSE_" + column + ".status") == "INVALID"), [column])
                if n
                else None,
            )
            bad_types |= bool(n)
    if bad_types:
        return None, stats["ROWS"]
    converted = []
    for column in columns:
        if types[column] == "object":
            expr = F.when(F.upper(F.trim(F.col(column))).isin(*C.MISSING_VALUES), F.lit(None)).otherwise(
                F.trim(F.col(column))
            )
        else:
            expr = F.col("_PARSE_" + column + ".value").cast("long" if types[column] == "int64" else "double")
        converted.append(expr.alias(column))
    typed = working.select(*converted).cache()
    cache_frames.append(typed)
    for column in numeric_columns:
        if types[column] == "int64":
            n = typed.where(F.col(column) < 0).count()
            report.add(dataset, "NONNEGATIVE_" + column, n, column=column, source=source)
    if "FACTOR_CONVERSION" in columns:
        report.add(
            dataset,
            "POSITIVE_FACTOR_CONVERSION",
            typed.where(F.col("FACTOR_CONVERSION") <= 0).count(),
            column="FACTOR_CONVERSION",
            source=source,
        )
    return typed, stats["ROWS"]


def duplicate_groups(frame, keys, all_columns, comparison_columns=None):
    attrs = [c for c in all_columns if c not in keys]
    business = [c for c in (comparison_columns or attrs) if c not in keys]

    def variants(cols):
        return (
            F.countDistinct(F.to_json(F.struct(*[F.col(c) for c in cols]), {"ignoreNullFields": "false"}))
            if cols
            else F.lit(1)
        )

    return (
        frame.groupBy(*keys)
        .agg(
            F.count("*").alias("N"),
            variants(attrs).alias("FULL_VARIANTS"),
            variants(business).alias("BUSINESS_VARIANTS"),
        )
        .where(F.col("N") > 1)
        .withColumn(
            "CLASSIFICATION",
            F.when(F.col("FULL_VARIANTS") == 1, "EXACT_DUPLICATE")
            .when(F.col("BUSINESS_VARIANTS") == 1, "DUPLICATE_KEY_SAME_ATTRIBUTES")
            .otherwise("DUPLICATE_KEY_CONFLICTING_ATTRIBUTES"),
        )
    )


def validate_catalog_duplicates(frame, dataset, report, source=None):
    columns = C.SOURCE_DEFINITIONS[dataset][0]
    key_sets = (
        [["ID_STORE"]]
        if dataset == "STORE_CATALOG"
        else [["ID_INTERNO"], ["UPC"], ["ID_IMPORT"], ["SKU"], ["ID_INTERNO", "SKU"]]
    )
    primary_count = 0
    for keys in key_sets:
        detail = duplicate_groups(
            frame, keys, columns, C.PRODUCT_COMPARISON_COLUMNS if dataset != "STORE_CATALOG" else None
        )
        stats = detail.agg(F.count("*").alias("KEYS"), F.sum("N").alias("ROWS")).first()
        rule = "DUPLICATE_" + "_".join(keys)
        report.add(
            dataset,
            rule,
            stats.ROWS or 0,
            category="DUPLICATE",
            severity=C.CATALOG_DUPLICATE_SEVERITY,
            source=source,
            column="+".join(keys),
            sample=_sample(detail, keys + ["N", "CLASSIFICATION"]) if stats.KEYS else None,
        )
        if stats.KEYS:
            report.add_detail(
                detail,
                keys,
                dataset=dataset,
                rule=rule,
                category="DUPLICATE",
                source=source,
                severity=C.CATALOG_DUPLICATE_SEVERITY,
            )
        if len(keys) == 1 and keys[0] in {"ID_STORE", "ID_INTERNO"}:
            primary_count = stats.KEYS
    return primary_count


def cross_catalog_collisions(edp, waters, key):
    left, right = edp.alias("e"), waters.alias("w")
    same = reduce(
        lambda a, b: a & b, [F.col("e." + c).eqNullSafe(F.col("w." + c)) for c in C.PRODUCT_COMPARISON_COLUMNS]
    )
    return (
        left.join(right, F.col("e." + key) == F.col("w." + key), "inner")
        .select(
            F.col("e." + key).alias(key),
            F.when(same, "CROSS_CATALOG_SAME_PRODUCT")
            .otherwise("CROSS_CATALOG_CONFLICTING_PRODUCT")
            .alias("CLASSIFICATION"),
        )
        .groupBy(key, "CLASSIFICATION")
        .agg(F.count("*").alias("N"))
    )


def validate_cross_catalogs(edp, waters, report):
    total = 0
    for key in ["ID_INTERNO", "UPC", "ID_IMPORT", "SKU"]:
        detail = cross_catalog_collisions(edp, waters, key)
        count = detail.count()
        total += count
        report.add(
            "PRODUCT_CATALOGS",
            "CROSS_CATALOG_" + key,
            count,
            category="CROSS_CATALOG",
            severity=C.CROSS_CATALOG_COLLISION_SEVERITY,
            column=key,
            sample=_sample(detail, [key, "CLASSIFICATION", "N"]) if count else None,
        )
        if count:
            report.add_detail(
                detail,
                [key],
                dataset="PRODUCT_CATALOGS",
                rule="CROSS_CATALOG_" + key,
                category="CROSS_CATALOG",
                severity=C.CROSS_CATALOG_COLLISION_SEVERITY,
            )
    return total


def normalize_bu(frame):
    mapping = F.create_map(*[F.lit(v) for pair in C.BU_NORMALIZATION.items() for v in pair])
    return frame.withColumn("NORMALIZED_BU", mapping[F.upper(F.trim(F.col("BU")))])


def detect_fact_period(frame, anio, mes, report, source=None):
    text = F.col("TIME_ID").cast("string")
    length = F.length(text)
    iso = F.concat(
        F.substring(text, 1, 4),
        F.lit("-"),
        F.substring(text, 5, 2),
        F.lit("-"),
        F.when(length == 6, F.lit("01")).otherwise(F.substring(text, 7, 2)),
    )
    dated = (
        normalize_bu(frame)
        .withColumn("_TIME_TEXT", text)
        .withColumn("_TIME_ISO", F.when(length.isin(6, 8), iso))
        .withColumn("_TIME_DATE", F.expr("try_cast(_TIME_ISO as date)"))
        .withColumn("ANIO", F.year("_TIME_DATE"))
        .withColumn("MES", F.month("_TIME_DATE"))
    )
    lengths = [
        r[0] for r in dated.where(F.col("_TIME_DATE").isNotNull()).select(F.length("_TIME_TEXT")).distinct().collect()
    ]
    invalid = dated.where(F.col("_TIME_DATE").isNull()).count()
    wrong_year = dated.where(F.col("ANIO") != anio).count()
    unknown = dated.where(F.col("NORMALIZED_BU").isNull()).count()
    report.add(
        "ECOMMERCE_FACT",
        "TIME_ID_FORMAT",
        invalid,
        category="PERIOD",
        source=source,
        sample=_sample(dated.where(F.col("_TIME_DATE").isNull()), ["TIME_ID"]) if invalid else None,
    )
    report.add("ECOMMERCE_FACT", "CONSISTENT_TIME_ID_FORMAT", int(len(lengths) > 1), category="PERIOD", source=source)
    report.add("ECOMMERCE_FACT", "ANNUAL_SOURCE_YEAR", wrong_year, category="PERIOD", source=source)
    report.add(
        "ECOMMERCE_FACT",
        "KNOWN_BU",
        unknown,
        category="REFERENTIAL",
        source=source,
        sample=_sample(dated.where(F.col("NORMALIZED_BU").isNull()), ["BU"]) if unknown else None,
    )
    monthly = dated.where((F.col("ANIO") == anio) & (F.col("MES") == mes)).drop("_TIME_TEXT", "_TIME_ISO", "_TIME_DATE")
    count = monthly.count()
    report.add("ECOMMERCE_FACT", "REQUESTED_PERIOD_EXISTS", int(count == 0), category="PERIOD", source=source)
    bounds = (
        frame.agg(
            F.min("TIME_ID").alias("MIN_TIME_ID"),
            F.max("TIME_ID").alias("MAX_TIME_ID"),
            F.count("*").alias("SOURCE_ANNUAL_ROWS"),
        )
        .first()
        .asDict()
    )
    return monthly, {
        **bounds,
        "REQUESTED_PERIOD_ROWS": count,
        "OTHER_PERIOD_ROWS": bounds["SOURCE_ANNUAL_ROWS"] - count - invalid,
        "INVALID_TIME_ID_ROWS": invalid,
        "UNKNOWN_BU_ROWS": unknown,
        "DETECTED_FORMAT": "YYYYMM" if lengths == [6] else "YYYYMMDD" if lengths == [8] else None,
        "DETECTED_YEARS": [
            r[0] for r in dated.select("ANIO").distinct().where("ANIO IS NOT NULL").orderBy("ANIO").collect()
        ],
        "DETECTED_MONTHS": [
            r[0] for r in dated.select("MES").distinct().where("MES IS NOT NULL").orderBy("MES").collect()
        ],
    }


def validate_fact_duplicates(monthly, report, source=None):
    exact = monthly.groupBy(*C.FACT_EXPECTED_COLUMNS).agg(F.count("*").alias("N")).where(F.col("N") > 1)
    n_exact = exact.agg(F.sum("N")).first()[0] or 0
    report.add(
        "ECOMMERCE_FACT",
        "EXACT_FACT_DUPLICATE",
        n_exact,
        category="DUPLICATE",
        severity=C.FACT_EXACT_DUPLICATE_SEVERITY,
        source=source,
    )
    keys = ["TIME_ID", "ID_STORE", "ID_INTERNO", "NORMALIZED_BU"]
    detail = duplicate_groups(monthly, keys, C.FACT_EXPECTED_COLUMNS + ["NORMALIZED_BU"], ["UNIDADES", "MONTO"])
    detail = detail.withColumn(
        "CLASSIFICATION",
        F.when(F.col("BUSINESS_VARIANTS") > 1, "POTENTIAL_AGGREGATED_DUPLICATE").otherwise(F.col("CLASSIFICATION")),
    )
    n = detail.count()
    report.add(
        "ECOMMERCE_FACT",
        "DUPLICATE_BUSINESS_KEY",
        n,
        category="DUPLICATE",
        severity=C.FACT_BUSINESS_KEY_DUPLICATE_SEVERITY,
        source=source,
    )
    if n:
        report.add_detail(
            detail,
            keys,
            dataset="ECOMMERCE_FACT",
            rule="DUPLICATE_BUSINESS_KEY",
            category="DUPLICATE",
            source=source,
            severity=C.FACT_BUSINESS_KEY_DUPLICATE_SEVERITY,
        )
    return n


def validate_negative_values(monthly, report, source=None):
    stats = monthly.agg(
        F.sum(F.when(F.col("UNIDADES") < 0, 1).otherwise(0)).alias("units"),
        F.sum(F.when(F.col("MONTO") < 0, 1).otherwise(0)).alias("amount"),
    ).first()
    units, amount = stats.units or 0, stats.amount or 0
    report.add(
        "ECOMMERCE_FACT",
        "NEGATIVE_UNITS",
        units,
        category="MANDATORY",
        severity=C.NEGATIVE_UNITS_SEVERITY,
        source=source,
    )
    report.add(
        "ECOMMERCE_FACT",
        "NEGATIVE_AMOUNT",
        amount,
        category="MANDATORY",
        severity=C.NEGATIVE_AMOUNT_SEVERITY,
        source=source,
    )
    return units, amount


def product_keys(edp, waters):
    return (
        edp.select("ID_INTERNO")
        .withColumn("NORMALIZED_BU", F.lit("EDP"))
        .unionByName(waters.select("ID_INTERNO").withColumn("NORMALIZED_BU", F.lit("WATERS")))
    )


def coverage_metrics(fact, edp, waters, stores):
    products = product_keys(edp, waters)
    keys = ["ID_INTERNO", "NORMALIZED_BU"]
    n = fact.count()
    matched_product = fact.join(products, keys, "left_semi").count()
    matched_store = fact.join(stores.select("ID_STORE"), "ID_STORE", "left_semi").count()
    after_product = fact.join(products, keys, "left").count()
    after_store = fact.join(stores.select("ID_STORE"), "ID_STORE", "left").count()
    return {
        "FACT_ROWS": n,
        "MATCHED_PRODUCT_ROWS": matched_product,
        "UNMATCHED_PRODUCT_ROWS": n - matched_product,
        "MATCHED_STORE_ROWS": matched_store,
        "UNMATCHED_STORE_ROWS": n - matched_store,
        "UNKNOWN_BU_ROWS": fact.where(F.col("NORMALIZED_BU").isNull()).count(),
        "ROWS_BEFORE_PRODUCT_JOIN": n,
        "ROWS_AFTER_PRODUCT_JOIN": after_product,
        "ROWS_BEFORE_STORE_JOIN": n,
        "ROWS_AFTER_STORE_JOIN": after_store,
        "PRODUCT_MATCH_PERCENTAGE": 100.0 * matched_product / n if n else 0.0,
        "STORE_MATCH_PERCENTAGE": 100.0 * matched_store / n if n else 0.0,
    }


def validate_references(fact, edp, waters, stores, report, source=None):
    metrics = coverage_metrics(fact, edp, waters, stores)
    for rule in ["UNMATCHED_PRODUCT_ROWS", "UNMATCHED_STORE_ROWS"]:
        report.add("ECOMMERCE_FACT", rule, metrics[rule], category="REFERENTIAL", source=source)
    for rule, missing, keys in [
        (
            "UNMATCHED_PRODUCT_ROWS",
            fact.join(product_keys(edp, waters), ["ID_INTERNO", "NORMALIZED_BU"], "left_anti"),
            ["ID_INTERNO", "NORMALIZED_BU"],
        ),
        ("UNMATCHED_STORE_ROWS", fact.join(stores.select("ID_STORE"), "ID_STORE", "left_anti"), ["ID_STORE"]),
    ]:
        if metrics[rule]:
            report.add_detail(
                missing.groupBy(*keys).agg(F.count("*").alias("N")),
                keys,
                dataset="ECOMMERCE_FACT",
                rule=rule,
                category="REFERENTIAL",
                source=source,
            )
    for kind in ["PRODUCT", "STORE"]:
        report.add(
            "ECOMMERCE_FACT",
            kind + "_JOIN_MULTIPLICATION",
            abs(metrics["ROWS_AFTER_" + kind + "_JOIN"] - metrics["FACT_ROWS"]),
            category="REFERENTIAL",
            source=source,
        )
    return metrics


def enrich_fact(monthly, edp, waters, stores):
    product_cols = [c for c in C.EDP_EXPECTED_COLUMNS if c != "ID_INTERNO"]
    products = (
        edp.select(*C.EDP_EXPECTED_COLUMNS)
        .withColumn("NORMALIZED_BU", F.lit("EDP"))
        .unionByName(waters.select(*C.WATERS_EXPECTED_COLUMNS).withColumn("NORMALIZED_BU", F.lit("WATERS")))
    )
    p = products.select("ID_INTERNO", "NORMALIZED_BU", *[F.col(c).alias("PRODUCT_" + c) for c in product_cols])
    s = stores.select("ID_STORE", *[F.col(c).alias("STORE_" + c) for c in C.STORE_EXPECTED_COLUMNS if c != "ID_STORE"])
    return monthly.join(p, ["ID_INTERNO", "NORMALIZED_BU"], "left").join(s, "ID_STORE", "left")


def add_operational_metadata(frame, metadata, anio, mes, catalog_bu=None, timestamp=None):
    values = {
        "RUN_ID": metadata["RUN_ID"],
        "LOGICAL_DATASET": metadata["LOGICAL_DATASET"],
        "SOURCE_FILE_NAME": metadata["ORIGINAL_FILE_NAME"],
        "SOURCE_FILE_PATH": metadata["SOURCE_PATH"],
        "SOURCE_FILE_FINGERPRINT": metadata["FILE_FINGERPRINT"],
        "SOURCE_FILE_YEAR": metadata["FILE_YEAR"],
        "LOAD_TIMESTAMP_UTC": timestamp or utc_now(),
        "ANIO": anio,
        "MES": mes,
    }
    if catalog_bu:
        values["CATALOG_BU"] = catalog_bu
    for key, value in values.items():
        kind = (
            "int"
            if key in {"ANIO", "MES", "SOURCE_FILE_YEAR"}
            else "timestamp"
            if key == "LOAD_TIMESTAMP_UTC"
            else "string"
        )
        frame = frame.withColumn(key, F.lit(value).cast(kind))
    return frame


def period_filter(frame, anio, mes):
    return frame.where((F.col("ANIO") == anio) & (F.col("MES") == mes))


def apply_qa_filters(frame, run_id=None, logical_dataset=None, bu=None, id_interno=None, id_store=None):
    for column, value in [
        ("RUN_ID", run_id),
        ("LOGICAL_DATASET", logical_dataset),
        ("NORMALIZED_BU", bu),
        ("ID_INTERNO", id_interno),
        ("ID_STORE", id_store),
    ]:
        if value is not None and column in frame.columns:
            frame = frame.where(F.col(column) == value)
    return frame


def qa_catalog_duplicate_detail(edp, waters, stores):
    frames = []
    for dataset, frame, keys in [
        ("EDP_CATALOG", edp, ["ID_INTERNO", "UPC", "ID_IMPORT", "SKU"]),
        ("WATERS_CATALOG", waters, ["ID_INTERNO", "UPC", "ID_IMPORT", "SKU"]),
        ("STORE_CATALOG", stores, ["ID_STORE"]),
    ]:
        key_sets = [[k] for k in keys] + ([["ID_INTERNO", "SKU"]] if dataset != "STORE_CATALOG" else [])
        for key_set in key_sets:
            grouped = duplicate_groups(frame, key_set, C.SOURCE_DEFINITIONS[dataset][0])
            frames.append(
                grouped.select(
                    F.lit(dataset).alias("LOGICAL_DATASET"),
                    F.lit("+".join(key_set)).alias("KEY_COLUMN"),
                    F.to_json(F.struct(*key_set)).alias("KEY_VALUE"),
                    "N",
                    "CLASSIFICATION",
                )
            )
    return reduce(lambda a, b: a.unionByName(b), frames)


def qa_cross_catalog_collisions(edp, waters):
    return reduce(
        lambda a, b: a.unionByName(b),
        [
            cross_catalog_collisions(edp, waters, key).select(
                F.lit(key).alias("KEY_COLUMN"), F.col(key).cast("string").alias("KEY_VALUE"), "CLASSIFICATION", "N"
            )
            for key in ["ID_INTERNO", "UPC", "ID_IMPORT", "SKU"]
        ],
    )
