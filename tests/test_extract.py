import datetime as dt
import hashlib
from types import SimpleNamespace

import pytest

from conf import settings as C
from src import extract as E


def manifest():
    names = [C.FACT_FILE_TEMPLATE.format(ANIO=2025)] + [
        d["expected_file_name"] for d in C.EXPECTED_STATIC_FILES.values()
    ]
    return [
        {
            "ORIGINAL_FILE_NAME": name,
            "SOURCE_PATH": C.INCOMING_PATH + "/" + name,
            "LOGICAL_DATASET": None,
            "RUN_ID": "r",
            "FILE_SIZE": 4,
            "MODIFICATION_TIMESTAMP": dt.datetime.fromtimestamp(1, dt.timezone.utc),
        }
        for name in names
    ]


class FakeFS:
    def __init__(self, files):
        self.files = {
            f["SOURCE_PATH"]: SimpleNamespace(
                path=f["SOURCE_PATH"], name=f["ORIGINAL_FILE_NAME"], size=4, modificationTime=1000
            )
            for f in files
        }

    def ls(self, path):
        if path in self.files:
            return [self.files[path]]
        if path in (C.INCOMING_PATH, C.CURRENT_PATH):
            return [f for p, f in self.files.items() if p.rsplit("/", 1)[0] == path]
        raise FileNotFoundError("No such file")

    def rm(self, path, recursive=False):
        return self.files.pop(path, None) is not None

    def mv(self, src, dest, recursive=False):
        item = self.files.pop(src)
        self.files[dest] = SimpleNamespace(
            path=dest, name=item.name, size=item.size, modificationTime=item.modificationTime
        )
        return True


def test_discovery_case_insensitive_and_physical_name_preserved():
    files = manifest()
    files[0]["ORIGINAL_FILE_NAME"] = files[0]["ORIGINAL_FILE_NAME"].lower().replace(".csv", ".CSV")
    assert E.validate_file_batch(files, 2025) == []
    assert files[0]["LOGICAL_DATASET"] == "ECOMMERCE_FACT"
    assert files[0]["ORIGINAL_FILE_NAME"].endswith(".CSV")


@pytest.mark.parametrize("mutation", ["missing", "unexpected", "wrong_year", "duplicate"])
def test_invalid_batch(mutation):
    files = manifest()
    if mutation == "missing":
        files.pop()
    if mutation == "unexpected":
        files.append({"ORIGINAL_FILE_NAME": "unexpected.csv"})
    if mutation == "wrong_year":
        files[0]["ORIGINAL_FILE_NAME"] = C.FACT_FILE_TEMPLATE.format(ANIO=2026)
    if mutation == "duplicate":
        files[1]["ORIGINAL_FILE_NAME"] = files[0]["ORIGINAL_FILE_NAME"]
    assert E.validate_file_batch(files, 2025)


def test_system_files_and_directories_ignored():
    files = manifest()
    fs = FakeFS(files)
    for name in ["_SUCCESS", ".hidden", "~draft.csv", "x.tmp", "x.partial"]:
        fs.files[C.INCOMING_PATH + "/" + name] = SimpleNamespace(
            path=C.INCOMING_PATH + "/" + name, name=name, size=0, modificationTime=1000
        )
    fs.files[C.INCOMING_PATH + "/dir/"] = SimpleNamespace(
        path=C.INCOMING_PATH + "/dir/", name="dir/", size=0, modificationTime=1000
    )
    assert len(E.list_incoming_files(SimpleNamespace(fs=fs), "r", 2025)) == 4


def test_failure_deletes_complete_batch_and_success_moves():
    files = manifest()
    dbutils = SimpleNamespace(fs=FakeFS(files))
    result = E.delete_submitted_batch(dbutils, files)
    assert result == {"deleted": 4, "remaining": 0, "failed_files": []}
    dbutils = SimpleNamespace(fs=FakeFS(files))
    metrics = {"files_moved_to_current": 0, "cleanup_metrics": {"current_files_updated": 0}}
    E.move_batch_to_current(dbutils, files, metrics)
    assert metrics["files_moved_to_current"] == 4
    assert dbutils.fs.ls(C.INCOMING_PATH) == []
    assert len(dbutils.fs.ls(C.CURRENT_PATH)) == 4


def test_changed_file_not_deleted():
    files = manifest()
    fs = FakeFS(files)
    fs.files[files[0]["SOURCE_PATH"]].size = 99
    result = E.delete_submitted_batch(SimpleNamespace(fs=fs), files)
    assert result["deleted"] == 3 and result["remaining"] == 1


def test_strict_csv_width_and_streamed_fingerprint(spark, tmp_path):
    path = tmp_path / "input.csv"
    content = '\ufeff A ,B\n"hello, world",2\nextra,3,4\n"unfinished,5\n'
    path.write_text(content, encoding="utf-8")
    assert E.fingerprint_file(spark, path.as_uri()) == hashlib.sha256(content.encode()).hexdigest()
    assert E.read_csv_header(spark, path.as_uri()) == [" A ", "B"]
    records = E.read_source_strings(spark, str(path), ["A", "B"]).collect()
    assert records[0].A == "hello, world"
    assert records[1]._CSV_ERROR == "FIELD_COUNT_MISMATCH"
    assert records[2]._CSV_ERROR == "MALFORMED_CSV_RECORD"


def test_cleanup_keeps_partial_metrics_on_permission_failure():
    files = manifest()

    class FailedFS(FakeFS):
        failed = False

        def ls(self, path):
            if self.failed:
                raise PermissionError("Access denied")
            return super().ls(path)

        def rm(self, path, recursive=False):
            result = super().rm(path, recursive)
            self.failed = True
            return result

    result = E.delete_submitted_batch(SimpleNamespace(fs=FailedFS(files)), files)
    # Deletion is not counted as verified if its follow-up inspection fails.
    assert result["remaining"] is None
    assert len(result["failed_files"]) == 4
