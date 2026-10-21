"""Shared Snowflake connection doubles for loader and test-maintenance checks."""

from decimal import Decimal

from src import load as L


class Cursor:
    def __init__(self, payloads, fail=None, incompatible=False):
        self.commands, self.result, self.fail = [], None, fail
        self.payloads, self.incompatible = payloads, incompatible

    def execute(self, sql):
        self.commands.append(sql)
        if self.fail and self.fail != "relationships" and sql.startswith(self.fail):
            raise RuntimeError("secret=private")
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            table = next(table for table in self.payloads if "TABLE_NAME='" + table.split(".")[-1].upper() + "'" in sql)
            rows = []
            for field in self.payloads[table].schema:
                kind = L._sql_type(field.dataType)
                rows.append(
                    (
                        field.name,
                        "NUMBER"
                        if kind.startswith("NUMBER")
                        else "TEXT"
                        if kind == "VARCHAR"
                        else "FLOAT"
                        if kind == "DOUBLE"
                        else kind,
                        38,
                        0,
                    )
                )
            self.result = rows[:-1] if self.incompatible else rows
        elif sql.startswith("SELECT COUNT(*) FROM (("):
            self.result = (0,)
        elif sql.startswith("SELECT COUNT(*) AS JOINED_ROWS"):
            self.result = (2, 1 if self.fail == "relationships" else 0, 0)
        elif sql.startswith("SELECT COUNT(*),"):
            self.result = (2, Decimal("7.5"))
        elif sql.startswith("SELECT COUNT(*) FROM"):
            self.result = (2,)
        return self

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result

    def close(self):
        pass
