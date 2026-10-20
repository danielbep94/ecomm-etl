import datetime as dt
import json
from unittest.mock import Mock

import pytest

from src import run_tracking as R


def test_terminal_record_derives_duration_delta_and_json():
    context = R.RunContext(
        pipeline_name="p",
        pipeline_version="1",
        environment="TEST",
        started_at_utc=dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
    )
    record = context.build_record(
        status="SUCCEEDED",
        target_rows_before=2,
        target_rows_after=5,
        completed_at_utc=dt.datetime(2025, 1, 1, 0, 0, 3, tzinfo=dt.timezone.utc),
        business_metrics={"sfUser": "private", "rows": 5},
    )
    assert record["DURATION_SECONDS"] == 3
    assert record["TARGET_ROW_DELTA"] == 3
    assert json.loads(record["BUSINESS_METRICS_JSON"]) == {"sfUser": "[REDACTED]", "rows": 5}
    assert R.RunContext(pipeline_name="p", pipeline_version="1", environment="TEST").run_id != context.run_id
    with pytest.raises(ValueError):
        context.build_record(status="RUNNING")


@pytest.mark.parametrize(
    "text",
    [
        "password=supersecret",
        '"pem_private_key": "supersecret"',
        "Bearer supersecret",
        "-----BEGIN PRIVATE KEY-----\nsupersecret\n-----END PRIVATE KEY-----",
        "https://person:supersecret@example.org/",
    ],
)
def test_sanitization(text):
    assert "supersecret" not in R.sanitize_error_message(text)
    assert len(R.sanitize_error_message("x" * 5000)) == 4000


def test_append_only_one_write_and_no_retry(monkeypatch):
    append = Mock(side_effect=RuntimeError("password=supersecret"))
    monkeypatch.setattr(R, "append_run_record", append)
    logger = Mock()
    assert not R.append_run_record_safely(Mock(), {"configured": True}, "DB.SCHEMA.PIPELINE_RUNS", {}, logger)
    append.assert_called_once()
    assert "supersecret" not in str(logger.mock_calls)


def test_append_uses_append_mode_and_checks_existing_table(spark):
    record = R.RunContext(pipeline_name="p", pipeline_version="1", environment="TEST").build_record(status="FAILED")

    class FakeWriter:
        def __init__(self):
            self.modes = []

        def format(self, value):
            return self

        def options(self, **kw):
            return self

        def option(self, *args):
            return self

        def mode(self, value):
            self.modes.append(value)
            return self

        def save(self):
            return None

    writer = FakeWriter()
    driver = Mock()
    driver.read.format.return_value.options.return_value.option.return_value.load.return_value.collect.return_value = []
    driver.createDataFrame.return_value.write = writer
    R.append_run_record(driver, {"test": "value"}, "PRD_MDP.MDP_STG.PIPELINE_RUNS", record)
    assert writer.modes == ["append"]
    query = driver.read.format.return_value.options.return_value.option.call_args.args[1]
    assert query.startswith("SELECT ") and "WHERE 1=0" in query


@pytest.mark.parametrize("value,expected_error", [(True, TypeError), ("1.5", ValueError), (str(2**63), OverflowError)])
def test_long_validation(value, expected_error):
    with pytest.raises(expected_error):
        R.normalize_optional_long(value)


def test_credentials_use_scope_and_never_expose_key_on_error():
    import base64

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    from conf.credentials import SNOWFLAKE_SECRET_SCOPE, get_snowflake_options

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    dbutils = Mock()
    dbutils.secrets.get.side_effect = ["test-user", pem]
    options = get_snowflake_options(dbutils)
    assert options["sfUser"] == "test-user"
    assert serialization.load_der_private_key(base64.b64decode(options["pem_private_key"]), None).key_size == 2048
    assert all(c.kwargs["scope"] == SNOWFLAKE_SECRET_SCOPE for c in dbutils.secrets.get.call_args_list)
    dbutils.secrets.get.side_effect = RuntimeError("private_key=private-test-value")
    with pytest.raises(RuntimeError) as captured:
        get_snowflake_options(dbutils)
    assert "private-test-value" not in str(captured.value)


def test_supplied_context_schema_and_decimal_normalization():
    from decimal import Decimal

    context = R.RunContext(pipeline_name="p", environment="TEST", pipeline_version="1")
    record = context.build_record(
        status="SUCCEEDED",
        source_rows=Decimal("10.0"),
        target_rows_before=Decimal("2"),
        target_rows_after=Decimal("5.00"),
    )
    assert record["ENVIRONMENT"] == "TEST" and record["PIPELINE_VERSION"] == "1"
    assert record["SOURCE_ROWS"] == 10 and record["TARGET_ROW_DELTA"] == 3
    assert isinstance(record["DURATION_SECONDS"], float)
    assert record["CREATED_AT_UTC"] == record["COMPLETED_AT_UTC"]
    assert R.parse_optional_int("{{job.run_id}}") is None
    assert R.parse_optional_int("not available") is None
    assert R.parse_utc_datetime("not available") is None
    assert R.metrics_to_json({}) is None


def test_supplied_tracking_record_matches_real_spark_schema(spark):
    record = R.RunContext(pipeline_name="p", environment="TEST").build_record(status="SUCCEEDED")
    driver = Mock()
    driver.createDataFrame.side_effect = spark.createDataFrame

    class StopWrite(Exception):
        pass

    # Capture the real frame, avoiding any remote connector write.
    frames = []

    def frame(rows, schema):
        result = spark.createDataFrame(rows, schema)
        frames.append(result)
        raise StopWrite

    driver.createDataFrame.side_effect = frame
    with pytest.raises(StopWrite):
        R.append_run_record(driver, {}, "PRD_MDP.MDP_STG.PIPELINE_RUNS", record)
    assert frames[0].first().RUN_ID == record["RUN_ID"]
    assert frames[0].schema["DURATION_SECONDS"].dataType.simpleString() == "double"
    assert frames[0].schema["CREATED_AT_UTC"].dataType.simpleString() == "timestamp"


def test_standard_tracking_logs_keep_pipeline_context(capsys):
    import logging

    from logs.logger import BusinessLogger

    logger = logging.getLogger("tracking-context-test")
    logger.handlers = [logging.StreamHandler()]
    logger.propagate = False
    logger.setLevel(logging.INFO)
    adapter = BusinessLogger(logger, {"RUN_ID": "run-1", "ANIO": 2025, "MES": 7})
    adapter.info("Pipeline telemetry recorded: run_id=%s status=%s", "run-1", "SUCCEEDED")
    message = capsys.readouterr().err
    assert "RUN_ID=run-1" in message and "ANIO=2025" in message and "STAGE=TRACKING" in message
