"""Strict source discovery, streaming fingerprints, CSV extraction and DBFS cleanup."""

import csv
import datetime as dt
import re
from collections import Counter

from conf import settings as C
from src.run_tracking import PipelineError


def is_ignored_file(name):
    lower = name.lower()
    return name.startswith((".", "~", "_")) or lower.endswith((".tmp", ".partial"))


def _is_dir(item):
    return item.isDir() if callable(getattr(item, "isDir", None)) else item.path.endswith("/")


def path_exists(dbutils, path):
    """Distinguish a missing path from permissions or transport failures."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:
        message = str(exc).lower()
        if any(s in message for s in ("filenotfoundexception", "path_not_found", "no such file", "does not exist")):
            return False
        raise PipelineError("DBFS path inspection failed.", "FILE_DISCOVERY_ERROR") from None


def verify_runtime_directories(dbutils):
    for path in (C.INCOMING_PATH, C.CURRENT_PATH):
        if not path_exists(dbutils, path):
            if not dbutils.fs.mkdirs(path) or not path_exists(dbutils, path):
                raise PipelineError("Required DBFS directory could not be created.", "FILE_DISCOVERY_ERROR")


def list_incoming_files(dbutils, run_id, expected_year=None):
    files = []
    for item in dbutils.fs.ls(C.INCOMING_PATH):
        if _is_dir(item) or is_ignored_file(item.name):
            continue
        files.append(
            {
                "RUN_ID": run_id,
                "LOGICAL_DATASET": None,
                "ORIGINAL_FILE_NAME": item.name,
                "SOURCE_PATH": item.path,
                "MATCHED_PATTERN": None,
                "FILE_YEAR": None,
                "EXPECTED_YEAR": expected_year,
                "FILE_SIZE": int(item.size),
                "MODIFICATION_TIMESTAMP": dt.datetime.fromtimestamp(item.modificationTime / 1000, dt.timezone.utc),
                "FILE_FINGERPRINT": None,
                "DISCOVERY_STATUS": "PENDING",
                "DISCOVERY_MESSAGE": None,
            }
        )
    return sorted(files, key=lambda item: item["ORIGINAL_FILE_NAME"])


def validate_file_batch(files, anio):
    """Annotate the complete manifest; return every available discovery finding."""
    issues = []
    if len(files) != C.EXPECTED_FILE_COUNT:
        issues.append((None, "FILE_COUNT", len(files), "Exactly four business CSV files are required."))
    for item in files:
        item.setdefault("LOGICAL_DATASET", None)
        name = item["ORIGINAL_FILE_NAME"]
        matches = []
        match = re.fullmatch(C.FACT_FILE_PATTERN, name, re.IGNORECASE)
        if match:
            matches.append(("ECOMMERCE_FACT", C.FACT_FILE_PATTERN, int(match.group(1))))
        for logical, definition in C.EXPECTED_STATIC_FILES.items():
            if re.fullmatch(definition["pattern"], name, re.IGNORECASE):
                matches.append((logical, definition["pattern"], None))
        item["EXPECTED_YEAR"] = anio
        if len(matches) != 1:
            item.update(DISCOVERY_STATUS="FAIL", DISCOVERY_MESSAGE="Unexpected or ambiguous source file name.")
            issues.append((item, "FILE_NAME", 1, item["DISCOVERY_MESSAGE"]))
            continue
        logical, pattern, file_year = matches[0]
        item.update(
            LOGICAL_DATASET=logical,
            MATCHED_PATTERN=pattern,
            FILE_YEAR=file_year,
            DISCOVERY_STATUS="PASS",
            DISCOVERY_MESSAGE="Source identified.",
        )
        if file_year is not None and file_year != anio:
            item.update(DISCOVERY_STATUS="FAIL", DISCOVERY_MESSAGE="Fact file year differs from ANIO.")
            issues.append((item, "FILE_YEAR", 1, item["DISCOVERY_MESSAGE"]))
    counts = Counter(item["LOGICAL_DATASET"] for item in files if item["LOGICAL_DATASET"])
    for logical in C.SOURCE_DEFINITIONS:
        if counts[logical] != 1:
            issues.append(
                (
                    {"LOGICAL_DATASET": logical},
                    "DATASET_CARDINALITY",
                    counts[logical],
                    "Each required logical dataset must occur exactly once.",
                )
            )
    return issues


def _hadoop_stream(spark, path):
    jvm = spark.sparkContext._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return jvm, fs.open(jpath)


def fingerprint_file(spark, path):
    """Stream bytes through JVM SHA-256, without collecting a whole annual file."""
    jvm, stream = _hadoop_stream(spark, path)
    digest = jvm.java.security.MessageDigest.getInstance("SHA-256")
    reader = jvm.java.security.DigestInputStream(stream, digest)
    buffer = spark.sparkContext._gateway.new_array(jvm.byte, 1024 * 1024)
    try:
        while reader.read(buffer) != -1:
            pass
        return bytes(int(x) & 255 for x in digest.digest()).hex()
    finally:
        reader.close()


def read_csv_header(spark, path):
    jvm, stream = _hadoop_stream(spark, path)
    decoder = jvm.java.nio.charset.StandardCharsets.UTF_8.newDecoder()
    decoder.onMalformedInput(jvm.java.nio.charset.CodingErrorAction.REPORT)
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, decoder))
    try:
        line = reader.readLine()
    finally:
        reader.close()
    if line is None:
        return []
    # Only a leading BOM is stripped; unknown column names are never repaired.
    return next(
        csv.reader(
            [line.lstrip("\ufeff")], delimiter=C.CSV_OPTIONS["delimiter"], quotechar=C.CSV_OPTIONS["quote"], strict=True
        )
    )


def read_source_strings(spark, path, headers):
    """Explicit all-string schema, strict record width, no schema inference.

    Contract: comma-separated UTF-8, doubled quotes, one physical line per record.
    Invalid widths/quoting become error rows, never discarded or silently truncated.
    """
    from pyspark.sql import types as T

    width = len(headers)
    delimiter, quote = C.CSV_OPTIONS["delimiter"], C.CSV_OPTIONS["quote"]

    def parse_partition(records):
        import csv

        for row, index in records:
            if index == 0:
                continue
            try:
                parsed = next(csv.reader([row.value], delimiter=delimiter, quotechar=quote, strict=True))
                if len(parsed) != width:
                    yield tuple([None] * width + ["FIELD_COUNT_MISMATCH"])
                else:
                    yield tuple(parsed + [None])
            except (csv.Error, StopIteration):
                yield tuple([None] * width + ["MALFORMED_CSV_RECORD"])

    schema = T.StructType([T.StructField(name, T.StringType(), True) for name in headers + ["_CSV_ERROR"]])
    lines = spark.read.format("text").option("encoding", C.CSV_OPTIONS["encoding"]).load(path)
    return spark.createDataFrame(lines.rdd.zipWithIndex().mapPartitions(parse_partition), schema)


def _current_item(dbutils, source):
    return next((f for f in dbutils.fs.ls(C.INCOMING_PATH) if f.path == source["SOURCE_PATH"]), None)


def _check_unchanged(item, source):
    timestamp = int(source["MODIFICATION_TIMESTAMP"].timestamp() * 1000)
    if item.size != source["FILE_SIZE"] or abs(item.modificationTime - timestamp) > 1:
        raise PipelineError("Submitted source changed during processing; refusing file operation.", "DBFS_DELETE_ERROR")


def delete_submitted_batch(dbutils, files):
    """Best effort for ALL manifest entries; caller preserves the original error."""
    deleted, errors = 0, []
    for source in files:
        try:
            item = _current_item(dbutils, source)
            if item is None:
                continue
            _check_unchanged(item, source)
            if not dbutils.fs.rm(source["SOURCE_PATH"], False) or _current_item(dbutils, source) is not None:
                raise PipelineError("Submitted source deletion could not be verified.", "DBFS_DELETE_ERROR")
            deleted += 1
        except Exception:
            errors.append(source["ORIGINAL_FILE_NAME"])
    remaining = 0
    for source in files:
        try:
            exists = _current_item(dbutils, source) is not None
            if remaining is not None:
                remaining += int(exists)
        except Exception:
            remaining = None
            if source["ORIGINAL_FILE_NAME"] not in errors:
                errors.append(source["ORIGINAL_FILE_NAME"])
    return {"deleted": deleted, "remaining": remaining, "failed_files": errors}


def move_batch_to_current(dbutils, files, metrics):
    """Called only after both targets reconcile. No failure directories are used."""
    for source in files:
        try:
            item = _current_item(dbutils, source)
            if item is None:
                raise RuntimeError("Submitted source missing")
            _check_unchanged(item, source)
            target = f"{C.CURRENT_PATH}/{source['ORIGINAL_FILE_NAME']}"
            if path_exists(dbutils, target) and not dbutils.fs.rm(target, False):
                raise RuntimeError("Previous current version could not be removed")
            if not dbutils.fs.mv(source["SOURCE_PATH"], target, False):
                raise RuntimeError("Move returned false")
            if not path_exists(dbutils, target) or _current_item(dbutils, source) is not None:
                raise RuntimeError("Move verification failed")
            metrics["files_moved_to_current"] += 1
            metrics["cleanup_metrics"]["current_files_updated"] += 1
        except Exception:
            raise PipelineError("Moving the submitted batch to current failed.", "DBFS_MOVE_ERROR") from None
    if any(_current_item(dbutils, source) is not None for source in files):
        raise PipelineError("Submitted batch still exists in incoming after movement.", "DBFS_MOVE_ERROR")
    metrics["cleanup_metrics"]["incoming_files_remaining"] = 0
