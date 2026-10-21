"""Reusable append-only pipeline run tracking for Databricks and Snowflake."""

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional
from uuid import uuid4

_MAX_ERROR_LENGTH = 4000


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.startswith("{{"):
        return None
    try:
        return int(normalized)
    except (TypeError, ValueError):
        return None


def normalize_optional_long(value: object) -> Optional[int]:
    """Convert Snowflake NUMBER/Decimal counts into Spark LongType values."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Boolean values are not valid row counts.")
    try:
        numeric_value = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise TypeError(f"Row count is not numeric: {value!r}") from exc
    if not numeric_value.is_finite():
        raise ValueError(f"Row count must be finite: {value!r}")
    if numeric_value != numeric_value.to_integral_value():
        raise ValueError(f"Row count must be an integer: {value!r}")
    normalized = int(numeric_value)
    if not -(2**63) <= normalized <= (2**63 - 1):
        raise OverflowError(f"Row count exceeds Spark LongType: {value!r}")
    return normalized


def parse_utc_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.startswith("{{"):
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def sanitize_error_message(value: object) -> Optional[str]:
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
        r"""(?ix)(["']?(?:password|passwd|pwd|token|secret|pem_private_key|private[_-]?key|access[_-]?key|sfUser|authorization)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;|]+)""",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(r"(https?://)[^\s/@:]+:[^\s/@]+@", r"\1[REDACTED]@", text)
    text = re.sub(r"[\r\n\t]+", " ", text)
    return text[:_MAX_ERROR_LENGTH]


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def metrics_to_json(metrics: Optional[Dict[str, Any]]) -> Optional[str]:
    if not metrics:
        return None

    def clean(value):
        if isinstance(value, Mapping):
            return {
                str(key): (
                    "[REDACTED]"
                    if re.search(
                        r"password|passwd|pwd|token|secret|credential|private.?key|access.?key|pem|authorization|sfuser",
                        str(key),
                        re.I,
                    )
                    else clean(item)
                )
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [clean(item) for item in value]
        if isinstance(value, Decimal) and not value.is_finite():
            return None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        if isinstance(value, str):
            return sanitize_error_message(value)
        return value

    return json.dumps(
        clean(metrics), default=_json_default, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


@dataclass
class RunContext:
    pipeline_name: str
    environment: str
    pipeline_version: Optional[str] = None
    databricks_job_id: Optional[str] = None
    databricks_job_run_id: Optional[str] = None
    databricks_task_run_id: Optional[str] = None
    databricks_task_name: Optional[str] = None
    attempt_number: Optional[int] = None
    trigger_type: Optional[str] = None
    started_at_utc: datetime = field(default_factory=utc_now)
    run_id: str = field(default_factory=lambda: str(uuid4()))

    def build_record(
        self,
        *,
        status: str,
        source_name: Optional[str] = None,
        source_file: Optional[str] = None,
        target_name: Optional[str] = None,
        period_start_date: Optional[date] = None,
        period_end_date: Optional[date] = None,
        source_rows: Optional[int] = None,
        staging_rows: Optional[int] = None,
        transformed_rows: Optional[int] = None,
        target_rows_before: Optional[int] = None,
        target_rows_after: Optional[int] = None,
        business_metrics: Optional[Dict[str, Any]] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        completed_at_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        if status not in {"SUCCEEDED", "VALIDATION_FAILED", "FAILED"}:
            raise ValueError("Only terminal statuses may be tracked")
        completed_at = completed_at_utc or utc_now()
        duration_seconds = max(
            0.0,
            (completed_at - self.started_at_utc).total_seconds(),
        )
        source_rows = normalize_optional_long(source_rows)
        staging_rows = normalize_optional_long(staging_rows)
        transformed_rows = normalize_optional_long(transformed_rows)
        target_rows_before = normalize_optional_long(target_rows_before)
        target_rows_after = normalize_optional_long(target_rows_after)

        target_delta = None
        if target_rows_before is not None and target_rows_after is not None:
            target_delta = target_rows_after - target_rows_before

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
            "STARTED_AT_UTC": self.started_at_utc,
            "COMPLETED_AT_UTC": completed_at,
            "DURATION_SECONDS": duration_seconds,
            "SOURCE_ROWS": source_rows,
            "STAGING_ROWS": staging_rows,
            "TRANSFORMED_ROWS": transformed_rows,
            "TARGET_ROWS_BEFORE": target_rows_before,
            "TARGET_ROWS_AFTER": target_rows_after,
            "TARGET_ROW_DELTA": target_delta,
            "BUSINESS_METRICS_JSON": metrics_to_json(business_metrics),
            "ERROR_TYPE": sanitize_error_message(error_type),
            "ERROR_MESSAGE": sanitize_error_message(error_message),
            "CREATED_AT_UTC": completed_at,
        }


def read_snowflake_metrics(
    spark,
    snowflake_options: Dict[str, str],
    query: str,
) -> Dict[str, Any]:
    """Execute a SELECT through the connector and return one result row."""
    row = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("query", query)
        .load()
        .first()
    )
    return row.asDict(recursive=True) if row is not None else {}


def append_run_record(
    spark,
    snowflake_options: Dict[str, str],
    table_name: str,
    record: Dict[str, Any],
) -> None:
    """Append one terminal run-attempt record to the shared Snowflake table."""
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("RUN_ID", StringType(), False),
            StructField("PIPELINE_NAME", StringType(), False),
            StructField("PIPELINE_VERSION", StringType(), True),
            StructField("ENVIRONMENT", StringType(), False),
            StructField("STATUS", StringType(), False),
            StructField("DATABRICKS_JOB_ID", StringType(), True),
            StructField("DATABRICKS_JOB_RUN_ID", StringType(), True),
            StructField("DATABRICKS_TASK_RUN_ID", StringType(), True),
            StructField("DATABRICKS_TASK_NAME", StringType(), True),
            StructField("ATTEMPT_NUMBER", LongType(), True),
            StructField("TRIGGER_TYPE", StringType(), True),
            StructField("SOURCE_NAME", StringType(), True),
            StructField("SOURCE_FILE", StringType(), True),
            StructField("TARGET_NAME", StringType(), True),
            StructField("PERIOD_START_DATE", DateType(), True),
            StructField("PERIOD_END_DATE", DateType(), True),
            StructField("STARTED_AT_UTC", TimestampType(), False),
            StructField("COMPLETED_AT_UTC", TimestampType(), False),
            StructField("DURATION_SECONDS", DoubleType(), False),
            StructField("SOURCE_ROWS", LongType(), True),
            StructField("STAGING_ROWS", LongType(), True),
            StructField("TRANSFORMED_ROWS", LongType(), True),
            StructField("TARGET_ROWS_BEFORE", LongType(), True),
            StructField("TARGET_ROWS_AFTER", LongType(), True),
            StructField("TARGET_ROW_DELTA", LongType(), True),
            StructField("BUSINESS_METRICS_JSON", StringType(), True),
            StructField("ERROR_TYPE", StringType(), True),
            StructField("ERROR_MESSAGE", StringType(), True),
            StructField("CREATED_AT_UTC", TimestampType(), False),
        ]
    )
    if not re.fullmatch(r"[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*", table_name):
        raise ValueError("Unsafe tracking table identifier")
    # The shared table must already exist. Never let the connector create it.
    (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("query", f"SELECT {','.join(field.name for field in schema)} FROM {table_name} WHERE 1=0")
        .load()
        .collect()
    )
    dataframe = spark.createDataFrame([record], schema=schema)
    (
        dataframe.write.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("dbtable", table_name)
        .option("column_mapping", "name")
        .mode("append")
        .save()
    )


def append_run_record_safely(
    spark,
    snowflake_options: Dict[str, str],
    table_name: str,
    record: Dict[str, Any],
    logger,
) -> bool:
    """Write telemetry without changing the pipeline's business outcome."""
    try:
        append_run_record(
            spark=spark,
            snowflake_options=snowflake_options,
            table_name=table_name,
            record=record,
        )
        logger.info(
            "Pipeline telemetry recorded: run_id=%s status=%s",
            record["RUN_ID"],
            record["STATUS"],
        )
        return True
    except Exception as tracking_error:
        logger.error(
            "TRACKING_WRITE_FAILED | %s: %s",
            type(tracking_error).__name__,
            sanitize_error_message(tracking_error),
        )
        return False


class PipelineError(RuntimeError):
    def __init__(self, message, error_type="UNEXPECTED_ERROR"):
        super().__init__(sanitize_error_message(message))
        self.error_type = error_type
