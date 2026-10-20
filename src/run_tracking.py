"""Append-only terminal run tracking. No DDL, updates, merges, or write retries."""

import dataclasses
import datetime as dt
import decimal
import json
import math
import re
import uuid
from collections.abc import Mapping

_MAX_ERROR_LENGTH = 4000
UTC = dt.timezone.utc
SECRET_NAME = re.compile(r"password|passwd|token|secret|credential|private.?key|pem|authorization|sfuser", re.I)


class PipelineError(RuntimeError):
    def __init__(self, message, error_type="UNEXPECTED_ERROR"):
        super().__init__(sanitize_error_message(message))
        self.error_type = error_type


def utc_now():
    return dt.datetime.now(UTC)


def parse_optional_int(value):
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, bool) or not re.fullmatch(r"[+-]?\d+", str(value).strip()):
        raise ValueError("Expected an integer")
    return int(value)


def normalize_optional_long(value):
    result = parse_optional_int(value)
    if result is not None and not -(2**63) <= result < 2**63:
        raise ValueError("Integer outside signed 64-bit range")
    return result


def parse_utc_datetime(value):
    if value is None:
        return None
    parsed = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def sanitize_error_message(value):
    if value is None:
        return None
    text = str(value)
    text = re.sub(
        r"-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|$)",
        "[REDACTED PRIVATE KEY]",
        text,
        flags=re.S,
    )
    text = re.sub(r"(?i)(?:bearer\s+)[\w.\-+/=]+", "Bearer [REDACTED]", text)
    text = re.sub(
        r"""(?ix)(["']?(?:password|passwd|token|secret|pem_private_key|private_key|sfUser|authorization)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;|]+)""",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(r"(https?://)[^\s/@:]+:[^\s/@]+@", r"\1[REDACTED]@", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    return text[:_MAX_ERROR_LENGTH]


def metrics_to_json(metrics):
    def clean(value):
        if isinstance(value, Mapping):
            return {str(k): ("[REDACTED]" if SECRET_NAME.search(str(k)) else clean(v)) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [clean(v) for v in value]
        if isinstance(value, (dt.datetime, dt.date)):
            return value.isoformat()
        if isinstance(value, decimal.Decimal):
            return str(value) if value.is_finite() else None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        if isinstance(value, str):
            return sanitize_error_message(value)
        if value is None or isinstance(value, (int, float, bool)):
            return value
        return sanitize_error_message(str(value))

    return json.dumps(clean(metrics), ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def get_widget_value(dbutils, name, default=None):
    try:
        value = dbutils.widgets.get(name)
        return default if value is None or str(value).strip() == "" else value
    except Exception:
        return default


@dataclasses.dataclass
class RunContext:
    pipeline_name: str
    pipeline_version: str
    environment: str
    databricks_job_id: str | None = None
    databricks_job_run_id: str | None = None
    databricks_task_run_id: str | None = None
    databricks_task_name: str | None = None
    attempt_number: int = 0
    trigger_type: str = "INTERACTIVE"
    run_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    started_at_utc: dt.datetime = dataclasses.field(default_factory=utc_now)

    def build_record(
        self,
        *,
        status,
        source_name=None,
        source_file=None,
        target_name=None,
        period_start_date=None,
        period_end_date=None,
        source_rows=None,
        staging_rows=None,
        transformed_rows=None,
        target_rows_before=None,
        target_rows_after=None,
        business_metrics=None,
        error_type=None,
        error_message=None,
        completed_at_utc=None,
    ):
        if status not in {"SUCCEEDED", "VALIDATION_FAILED", "FAILED"}:
            raise ValueError("Only terminal statuses may be tracked")
        completed = parse_utc_datetime(completed_at_utc) or utc_now()
        started = parse_utc_datetime(self.started_at_utc)
        duration = decimal.Decimal(str(max(0, (completed - started).total_seconds()))).quantize(
            decimal.Decimal("0.001")
        )
        before, after = normalize_optional_long(target_rows_before), normalize_optional_long(target_rows_after)
        return {
            "RUN_ID": self.run_id,
            "PIPELINE_NAME": self.pipeline_name,
            "PIPELINE_VERSION": self.pipeline_version,
            "ENVIRONMENT": self.environment,
            "STATUS": status,
            "DATABRICKS_JOB_ID": self.databricks_job_id,
            "DATABRICKS_JOB_RUN_ID": self.databricks_job_run_id,
            "DATABRICKS_TASK_RUN_ID": self.databricks_task_run_id,
            "DATABRICKS_TASK_NAME": self.databricks_task_name,
            "ATTEMPT_NUMBER": normalize_optional_long(self.attempt_number),
            "TRIGGER_TYPE": self.trigger_type,
            "SOURCE_NAME": source_name,
            "SOURCE_FILE": source_file,
            "TARGET_NAME": target_name,
            "PERIOD_START_DATE": period_start_date,
            "PERIOD_END_DATE": period_end_date,
            "STARTED_AT_UTC": started,
            "COMPLETED_AT_UTC": completed,
            "DURATION_SECONDS": duration,
            "SOURCE_ROWS": normalize_optional_long(source_rows),
            "STAGING_ROWS": normalize_optional_long(staging_rows),
            "TRANSFORMED_ROWS": normalize_optional_long(transformed_rows),
            "TARGET_ROWS_BEFORE": before,
            "TARGET_ROWS_AFTER": after,
            "TARGET_ROW_DELTA": after - before if after is not None and before is not None else None,
            "BUSINESS_METRICS_JSON": metrics_to_json(business_metrics or {}),
            "ERROR_TYPE": error_type,
            "ERROR_MESSAGE": sanitize_error_message(error_message),
        }


def read_snowflake_metrics(spark, snowflake_options, query):
    from conf.credentials import SNOWFLAKE_SOURCE_NAME

    row = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option("query", query).load().first()
    return {} if row is None else row.asDict()


def append_run_record(spark, snowflake_options, table_name, record):
    from pyspark.sql import types as T

    from conf.credentials import SNOWFLAKE_SOURCE_NAME

    if not re.fullmatch(r"[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*", table_name):
        raise ValueError("Unsafe tracking table identifier")
    # Existence/column checks prevent the connector from auto-creating tracking.
    (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**snowflake_options)
        .option("query", f"SELECT {','.join(record)} FROM {table_name} WHERE 1=0")
        .load()
        .collect()
    )
    longs = {
        "ATTEMPT_NUMBER",
        "SOURCE_ROWS",
        "STAGING_ROWS",
        "TRANSFORMED_ROWS",
        "TARGET_ROWS_BEFORE",
        "TARGET_ROWS_AFTER",
        "TARGET_ROW_DELTA",
    }
    timestamps = {"STARTED_AT_UTC", "COMPLETED_AT_UTC"}
    dates = {"PERIOD_START_DATE", "PERIOD_END_DATE"}
    fields = [
        T.StructField(
            k,
            T.LongType()
            if k in longs
            else T.TimestampType()
            if k in timestamps
            else T.DateType()
            if k in dates
            else T.DecimalType(18, 3)
            if k == "DURATION_SECONDS"
            else T.StringType(),
            True,
        )
        for k in record
    ]
    frame = spark.createDataFrame([record], T.StructType(fields))
    (
        frame.write.format(SNOWFLAKE_SOURCE_NAME)
        .options(**snowflake_options)
        .option("dbtable", table_name)
        .option("column_mapping", "name")
        .option("column_mismatch_behavior", "ignore")  # CREATED_AT_UTC uses its Snowflake default.
        .mode("append")
        .save()
    )


def append_run_record_safely(spark, snowflake_options, table_name, record, logger):
    try:
        if not snowflake_options:
            raise ValueError("Snowflake options unavailable")
        append_run_record(spark, snowflake_options, table_name, record)
        logger.event("TRACKING", "WRITTEN")
        return True
    except Exception:
        # Do not log connector exception text or retry an ambiguously committed append.
        logger.event("TRACKING", "FAILED", ERROR_TYPE="TRACKING_WRITE_ERROR")
        return False
