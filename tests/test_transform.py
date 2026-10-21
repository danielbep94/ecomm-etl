import pytest
from pyspark.sql import functions as F

from conf import settings as C
from src import transform as V


@pytest.mark.parametrize(
    "value,kind,status",
    [
        ("9223372036854775807", "int64", "VALID"),
        ("9223372036854775808", "int64", "INVALID"),
        ("-9223372036854775808", "int64", "VALID"),
        ("-9223372036854775809", "int64", "INVALID"),
        ("1.000000000000000000001", "int64", "INVALID"),
        ("1.0", "int64", "VALID"),
        ("1e3", "int64", "VALID"),
        ("1.5", "int64", "INVALID"),
        ("oops", "float64", "INVALID"),
        ("NaN", "float64", "INVALID"),
        ("inf", "float64", "INVALID"),
        ("1e9999", "float64", "INVALID"),
        ("-0.25", "float64", "VALID"),
        ("NULL", "int64", "MISSING"),
        (None, "float64", "MISSING"),
    ],
)
def test_numeric_validation(value, kind, status):
    assert V.parse_numeric(value, kind)[1] == status


@pytest.mark.parametrize("args", [(True, 1, False), (25, 1, False), (2025, 13, False), (2025, 1, "yes")])
def test_bad_parameters(args):
    with pytest.raises(V.ValidationError):
        V.validate_parameters(*args)


def test_safe_boolean_conversion():
    assert V.validate_parameters("2025", "07", "false") == (2025, 7, False)
    assert V.validate_parameters(2026, 1, "TRUE") == (2026, 1, True)


def test_headers(spark):
    report = V.ValidationReport(spark, "r", 2025, 7)
    assert V.validate_headers(["\ufeff B ", "A"], ["A", "B"], report, "X", "file") == ["B", "A"]
    assert not report.failed
    assert V.validate_headers(["A", "A", "OTHER"], ["A", "B"], report, "X", "file") is None
    assert report.failed


def test_mandatory_missing_and_invalid_numeric_separate(spark):
    frame = spark.createDataFrame(
        [("NA", "Online", "Chain", "Digital", None), ("oops", "Online", "Chain", "Digital", None)],
        "ID_STORE string, TYPE string, CADENA string, CANAL string, _CSV_ERROR string",
    )
    report, cache = V.ValidationReport(spark, "r", 2025, 7), []
    try:
        typed, count = V.validate_and_cast_source(frame, "STORE_CATALOG", report, "stores.csv", cache)
        assert typed is None and count == 2
        rules = {r["VALIDATION_RULE"]: r["AFFECTED_ROWS"] for r in report.records}
        assert rules["MANDATORY_ID_STORE"] == 1
        assert rules["TYPE_ID_STORE"] == 1
    finally:
        for f in cache:
            f.unpersist()


def test_duplicate_classifications(spark, catalogs, product_rows):
    edp, _, _ = catalogs
    frame = spark.createDataFrame(
        [
            product_rows(),
            product_rows(),
            product_rows(2, 22, description="A"),
            product_rows(2, 22, description="B"),
            product_rows(3, 33, name="A"),
            product_rows(3, 33, name="B"),
        ],
        edp.schema,
    )
    rows = V.duplicate_groups(frame, ["ID_INTERNO"], C.EDP_EXPECTED_COLUMNS, C.PRODUCT_COMPARISON_COLUMNS).collect()
    assert {r.ID_INTERNO: r.CLASSIFICATION for r in rows} == {
        1: "EXACT_DUPLICATE",
        2: "DUPLICATE_KEY_SAME_ATTRIBUTES",
        3: "DUPLICATE_KEY_CONFLICTING_ATTRIBUTES",
    }
    report = V.ValidationReport(spark, "r", 2025, 7)
    assert V.validate_catalog_duplicates(frame, "EDP_CATALOG", report) == 3
    assert report.failed
    assert report.dataframe().where("KEY_VALUE IS NOT NULL").count() > 0


def test_cross_catalog_collisions(spark, catalogs):
    edp, _, _ = catalogs
    assert V.cross_catalog_collisions(edp, edp, "ID_INTERNO").first().CLASSIFICATION == "CROSS_CATALOG_SAME_PRODUCT"
    changed = edp.withColumn("NOMBRE", F.lit("Different"))
    assert (
        V.cross_catalog_collisions(edp, changed, "ID_INTERNO").first().CLASSIFICATION
        == "CROSS_CATALOG_CONFLICTING_PRODUCT"
    )


def test_period_filtering_unknown_bu_and_mixed_dates(spark):
    frame = spark.createDataFrame(
        [
            (202507, 1, 1, 1.0, 10.0, " edp "),
            (202508, 1, 1, 1.0, 20.0, "PET"),
            (20250731, 1, 1, 1.0, 10.0, "unknown"),
            (20250230, 1, 1, 1.0, 10.0, "EDP"),
            (202607, 1, 1, 1.0, 10.0, "EDP"),
        ],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double, BU string",
    )
    report = V.ValidationReport(spark, "r", 2025, 7)
    monthly, metrics = V.detect_fact_period(frame, 2025, 7, report)
    assert monthly.count() == 2 and metrics["SOURCE_ANNUAL_ROWS"] == 5
    assert metrics["INVALID_TIME_ID_ROWS"] == 1 and metrics["UNKNOWN_BU_ROWS"] == 1
    assert {r["VALIDATION_RULE"] for r in report.records if r["VALIDATION_STATUS"] == "FAIL"} >= {
        "CONSISTENT_TIME_ID_FORMAT",
        "ANNUAL_SOURCE_YEAR",
        "KNOWN_BU",
        "TIME_ID_FORMAT",
    }


def test_missing_references_and_join_multiplication(spark, catalogs):
    edp, waters, stores = catalogs
    fact = spark.createDataFrame(
        [(202507, 1, 1, 1.0, 10.0, "EDP"), (202507, 999, 999, 1.0, 10.0, "EDP")],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double, BU string",
    )
    fact = V.normalize_bu(fact)
    report = V.ValidationReport(spark, "r", 2025, 7)
    metrics = V.validate_references(fact, edp.unionByName(edp), waters, stores, report)
    assert metrics["UNMATCHED_PRODUCT_ROWS"] == 1 and metrics["UNMATCHED_STORE_ROWS"] == 1
    assert metrics["ROWS_AFTER_PRODUCT_JOIN"] == 3
    assert report.error_type == "REFERENTIAL_VALIDATION_ERROR"
    assert report.dataframe().where("VALIDATION_CATEGORY='REFERENTIAL_DETAIL'").count() > 0


def test_fact_duplicates_and_negative_warnings(spark):
    fact = spark.createDataFrame(
        [(202507, 1, 1, -1.0, -10.0, "EDP"), (202507, 1, 1, -1.0, -10.0, "EDP")],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double, BU string",
    )
    fact = V.normalize_bu(fact)
    report = V.ValidationReport(spark, "r", 2025, 7)
    assert V.validate_fact_duplicates(fact, report) == 1
    assert V.validate_negative_values(fact, report) == (2, 2)
    assert report.metrics()["warning_rules"] == 2


def test_valid_explicit_types_preserve_exact_long_values(spark, catalogs):
    edp, _, _ = catalogs
    raw = edp.select(*[F.col(c).cast("string").alias(c) for c in C.EDP_EXPECTED_COLUMNS])
    raw = (
        raw.withColumn("UPC", F.lit("9223372036854775807"))
        .withColumn("ID_IMPORT", F.lit("1.001e3"))
        .withColumn("_CSV_ERROR", F.lit(None).cast("string"))
    )
    report, caches = V.ValidationReport(spark, "r", 2025, 7), []
    try:
        typed, count = V.validate_and_cast_source(raw, "EDP_CATALOG", report, "edp.csv", caches)
        assert count == 1 and not report.failed
        row = typed.first()
        assert row.UPC == 9223372036854775807 and row.ID_IMPORT == 1001
        assert typed.schema["UPC"].dataType.simpleString() == "bigint"
        assert typed.schema["FACTOR_CONVERSION"].dataType.simpleString() == "double"
    finally:
        for frame in caches:
            frame.unpersist()


@pytest.mark.parametrize("month", ["ALL", "all", " ALL "])
def test_annual_scope_parameters(month):
    assert V.validate_parameters(2026, month) == (2026, "ALL", False)
    assert str(V.period_bounds(2026, "ALL")[0]) == "2026-01-01"
    assert str(V.period_bounds(2026, "ALL")[1]) == "2026-12-31"


@pytest.mark.parametrize("month", [None, "", " ", 0, "YEAR"])
def test_empty_or_implicit_annual_scope_rejected(month):
    with pytest.raises(V.ValidationError, match="ALL"):
        V.validate_parameters(2026, month)


def test_annual_detection_and_metadata_preserve_all_twelve_months(spark):
    source = spark.createDataFrame(
        [(202600 + month, 1, 1, 1.0, 10.0, "EDP") for month in range(1, 13)],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double, BU string",
    )
    report = V.ValidationReport(spark, "annual", 2026, "ALL")
    annual, metrics = V.detect_fact_period(source, 2026, "ALL", report)
    metadata = {
        "RUN_ID": "annual",
        "LOGICAL_DATASET": "ECOMMERCE_FACT",
        "ORIGINAL_FILE_NAME": "fact.csv",
        "SOURCE_PATH": "file:/fact.csv",
        "FILE_FINGERPRINT": "a" * 64,
        "FILE_YEAR": 2026,
    }
    annotated = V.add_operational_metadata(annual, metadata, 2026, "ALL")
    assert sorted(row.MES for row in annotated.select("MES").collect()) == list(range(1, 13))
    assert metrics["LOAD_SCOPE"] == "YEAR" and metrics["REQUESTED_PERIOD_ROWS"] == 12 and not report.failed
    assert report.dataframe().first().MES is None


@pytest.mark.parametrize("dataset", ["EDP_CATALOG", "WATERS_CATALOG", "ECOMMERCE_FACT", "STORE_CATALOG"])
def test_approved_source_headers_are_exact(spark, dataset):
    expected = C.SOURCE_DEFINITIONS[dataset][0]
    report = V.ValidationReport(spark, "schema", 2026, "ALL")
    assert V.validate_headers(expected, expected, report, dataset, "source.csv") == expected
    assert not report.failed
    extra = "BU" if dataset == "ECOMMERCE_FACT" else "SKU"
    assert V.validate_headers(expected + [extra], expected, report, dataset, "source.csv") is None
    assert report.failed


@pytest.mark.parametrize("units,bad", [("17", False), ("17.5", False), ("-1", False), ("oops", True)])
def test_fact_units_follow_float_contract(spark, units, bad):
    raw = spark.createDataFrame(
        [("202601", "2197", "1", units, "109.42", None)],
        "TIME_ID string, ID_STORE string, ID_INTERNO string, UNIDADES string, MONTO string, _CSV_ERROR string",
    )
    report, cache = V.ValidationReport(spark, "units", 2026, "ALL"), []
    try:
        frame, count = V.validate_and_cast_source(raw, "ECOMMERCE_FACT", report, "fact.csv", cache)
        assert count == 1 and report.failed == bad
        if not bad:
            assert frame.schema["UNIDADES"].dataType.simpleString() == "double"
            assert frame.first().UNIDADES == float(units)
    finally:
        for frame in cache:
            frame.unpersist()


def test_nullable_measures_preserve_rows_and_identify_raw_placeholders_in_logs(spark):
    import json
    from unittest.mock import Mock

    raw = spark.createDataFrame(
        [
            ("202601", "2197", "1", " -   ", " -   ", None),
            ("202601", "2197", "2", None, "NULL", None),
            ("202601", "2197", "3", "17.5", "109.42", None),
        ],
        "TIME_ID string, ID_STORE string, ID_INTERNO string, UNIDADES string, MONTO string, _CSV_ERROR string",
    )
    report, cache = V.ValidationReport(spark, "nullable", 2026, "ALL"), []
    try:
        frame, count = V.validate_and_cast_source(raw, "ECOMMERCE_FACT", report, "fact.csv", cache)
        assert count == 3 and not report.failed
        rows = frame.orderBy("ID_INTERNO").collect()
        assert len(rows) == 3
        assert rows[0].UNIDADES is None and rows[0].MONTO is None
        assert rows[1].UNIDADES is None and rows[1].MONTO is None
        assert rows[2].UNIDADES == 17.5 and rows[2].MONTO == 109.42
        warnings = [r for r in report.records if r["VALIDATION_STATUS"] == "WARNING"]
        assert {r["VALIDATION_RULE"]: r["AFFECTED_ROWS"] for r in warnings} == {
            "NULLABLE_UNIDADES": 2,
            "NULLABLE_MONTO": 2,
        }
        logger = Mock()
        report.log(logger)
        logged = [call.kwargs for call in logger.event.call_args_list if call.kwargs["RULE"] == "NULLABLE_UNIDADES"][0]
        sample = json.loads(logged["SAMPLE_RECORDS_JSON"])[0]
        assert sample == {"TIME_ID": "202601", "ID_STORE": "2197", "ID_INTERNO": "1", "UNIDADES": " -   "}
        assert logged["COLUMN_NAME"] == "UNIDADES" and "NULL" in logged["DESCRIPTION"]
        assert V.parse_numeric(" -   ", "int64")[1] == "INVALID"  # IDs remain strict.
    finally:
        for item in cache:
            item.unpersist()


def test_nullable_conversion_factor_warns_and_invalid_factors_identify_products(spark, catalogs):
    import json

    edp, _, _ = catalogs
    raw = edp.select(*[F.col(c).cast("string").alias(c) for c in C.EDP_EXPECTED_COLUMNS])
    factors = spark.createDataFrame(
        [("1", None), ("2", " -   "), ("3", "0"), ("4", "-0.2"), ("5", "0.1")],
        "_ID string, _FACTOR string",
    )
    raw = (
        raw.crossJoin(factors)
        .withColumn("ID_INTERNO", F.col("_ID"))
        .withColumn("FACTOR_CONVERSION", F.col("_FACTOR"))
        .withColumn("_CSV_ERROR", F.lit(None).cast("string"))
    )
    report, cache = V.ValidationReport(spark, "factor", 2026, "ALL"), []
    try:
        frame, count = V.validate_and_cast_source(raw, "WATERS_CATALOG", report, "waters.csv", cache)
        assert count == 5 and report.failed
        assert frame.where("FACTOR_CONVERSION IS NULL").count() == 2
        warnings = [r for r in report.records if r["VALIDATION_STATUS"] == "WARNING"]
        assert len(warnings) == 1 and warnings[0]["VALIDATION_RULE"] == "NULLABLE_FACTOR_CONVERSION"
        assert warnings[0]["AFFECTED_ROWS"] == 2
        errors = [r for r in report.records if r["VALIDATION_STATUS"] == "FAIL"]
        assert len(errors) == 1 and errors[0]["VALIDATION_RULE"] == "POSITIVE_FACTOR_CONVERSION"
        assert errors[0]["AFFECTED_ROWS"] == 2
        samples = json.loads(errors[0]["SAMPLE_RECORDS_JSON"])
        assert {(r["ID_INTERNO"], r["FACTOR_CONVERSION"]) for r in samples} == {(3, 0.0), (4, -0.2)}
    finally:
        for item in cache:
            item.unpersist()


def test_missing_bu_is_derived_without_dropping_or_multiplying_fact_rows(spark, catalogs):
    edp, waters, _ = catalogs
    fact = spark.createDataFrame(
        [(202601, 1, 1, 1.0, 10.0), (202602, 1, 2, 2.0, 20.0)],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double",
    )
    report = V.ValidationReport(spark, "bu", 2026, "ALL")
    result, metrics = V.infer_fact_bu(fact, edp, waters, report)
    assert {r.ID_INTERNO: r.NORMALIZED_BU for r in result.collect()} == {1: "EDP", 2: "WATERS"}
    assert result.count() == fact.count() == 2 and not report.failed
    assert metrics["derived_rows"] == 2


def test_missing_bu_rejects_ambiguous_and_unknown_products(spark, catalogs):
    edp, waters, _ = catalogs
    fact = spark.createDataFrame(
        [(202601, 1, 1, 1.0, 10.0), (202602, 1, 99, 2.0, 20.0)],
        "TIME_ID long, ID_STORE long, ID_INTERNO long, UNIDADES double, MONTO double",
    )
    report = V.ValidationReport(spark, "bu", 2026, "ALL")
    result, metrics = V.infer_fact_bu(fact, edp, waters.unionByName(edp), report)
    assert result.count() == 2 and all(r.NORMALIZED_BU is None for r in result.collect())
    assert metrics["ambiguous_rows"] == metrics["unmatched_rows"] == 1 and report.failed
