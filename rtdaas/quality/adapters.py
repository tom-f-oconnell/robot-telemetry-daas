"""Table adapters that give the rule engine a uniform view of pandas, SQLite and Spark tables."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pandas as pd

from rtdaas.quality.schemas import sqlite_table_name

log = logging.getLogger(__name__)


class TableAdapter(Protocol):
    """Minimal interface the engine needs from a storage backend."""

    def frame(
        self,
        table_name: str,
        partition_column: str | None,
        partition_value: str | None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Return the (optionally partition-filtered) table as a pandas frame.

        ``columns`` names the ones the caller will read (struct children in flattened form, like
        ``psml_x``). An adapter may return more, or fewer if some do not exist.
        """

    def sql(self, query: str) -> float | None:
        """Run a SQL query (table placeholders already substituted) and return its single numeric result."""

    def columns(self, table_name: str) -> dict[str, str]:
        """Column name -> type name (backend-specific type strings)."""

    def resolve_table(self, table_name: str) -> str:
        """Backend-specific identifier to substitute for ``{table}`` in SQL checks."""


def _scalar(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if np.isnan(f) else f


def _partition_mask(frame: pd.DataFrame, column: str, value: str) -> np.ndarray:
    """Rows whose ``column`` falls on calendar day ``value`` (``YYYY-MM-DD``, UTC)."""
    ts = pd.to_datetime(
        frame[column], utc=True, format="ISO8601" if frame[column].dtype == object else None
    )
    return (ts.dt.strftime("%Y-%m-%d") == value).to_numpy()


def frame_to_sqlite_ready(frame: pd.DataFrame) -> pd.DataFrame:
    """Make a frame SQLite-insertable: lists/dicts -> JSON text, tz timestamps -> ISO strings."""
    out = frame.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_datetime64_any_dtype(s.dtype):
            s = pd.to_datetime(s, utc=True)
            out[col] = s.dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00").where(s.notna(), None)
        elif s.dtype == object:
            if s.map(lambda v: isinstance(v, (list, tuple, dict, np.ndarray))).any():
                out[col] = s.map(
                    lambda v: (
                        json.dumps(list(v) if isinstance(v, (tuple, np.ndarray)) else v)
                        if isinstance(v, (list, tuple, dict, np.ndarray))
                        else v
                    )
                )
    return out


class PandasAdapter:
    """Tables held in memory as pandas frames; SQL runs on an in-memory SQLite copy."""

    def __init__(self, tables: dict[str, pd.DataFrame]):
        self.tables = dict(tables)
        self._conn: sqlite3.Connection | None = None

    def frame(
        self,
        table_name: str,
        partition_column: str | None = None,
        partition_value: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        if table_name not in self.tables:
            raise KeyError(f"unknown table {table_name!r}")
        df = self.tables[table_name]
        if partition_column and partition_value and partition_column in df.columns and len(df):
            df = df.loc[_partition_mask(df, partition_column, partition_value)]
        return df

    def columns(self, table_name: str) -> dict[str, str]:
        return {c: str(t) for c, t in self.frame(table_name).dtypes.items()}

    def resolve_table(self, table_name: str) -> str:
        return sqlite_table_name(table_name)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(":memory:")
            for name, df in self.tables.items():
                frame_to_sqlite_ready(df).to_sql(sqlite_table_name(name), self._conn, index=False)
        return self._conn

    def sql(self, query: str) -> float | None:
        row = self._connection().execute(query).fetchone()
        return _scalar(row[0] if row else None)


class SqliteAdapter:
    """The local SQLite mirror (spec §3.3): ``telemetry.sessions`` -> ``telemetry__sessions``."""

    def __init__(self, path: str | Path | sqlite3.Connection):
        if isinstance(path, sqlite3.Connection):
            self.conn = path
            self.path = None
        else:
            self.path = Path(path)
            self.conn = sqlite3.connect(str(self.path))

    def table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (sqlite_table_name(table_name),),
        ).fetchone()
        return row is not None

    def frame(
        self,
        table_name: str,
        partition_column: str | None = None,
        partition_value: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        name = sqlite_table_name(table_name)
        if not self.table_exists(table_name):
            raise KeyError(f"table {name!r} not found in {self.path}")
        if partition_column and partition_value:
            q = f'SELECT * FROM "{name}" WHERE date("{partition_column}") = ?'
            return pd.read_sql_query(q, self.conn, params=(partition_value,))
        return pd.read_sql_query(f'SELECT * FROM "{name}"', self.conn)

    def columns(self, table_name: str) -> dict[str, str]:
        name = sqlite_table_name(table_name)
        rows = self.conn.execute(f'PRAGMA table_info("{name}")').fetchall()
        if not rows:
            raise KeyError(f"table {name!r} not found in {self.path}")
        return {r[1]: (r[2] or "").lower() for r in rows}

    def resolve_table(self, table_name: str) -> str:
        return f'"{sqlite_table_name(table_name)}"'

    def sql(self, query: str) -> float | None:
        row = self.conn.execute(query).fetchone()
        return _scalar(row[0] if row else None)


class SparkAdapter:
    """Iceberg tables through a SparkSession (``lake.<namespace>.<table>``). pyspark is imported lazily."""

    def __init__(self, spark: Any, catalog: str = "lake", row_limit: int | None = 2_000_000):
        self.spark = spark
        self.catalog = catalog
        self.row_limit = row_limit

    def resolve_table(self, table_name: str) -> str:
        return f"{self.catalog}.{table_name}"

    def _df(
        self,
        table_name: str,
        partition_column: str | None = None,
        partition_value: str | None = None,
    ):
        from pyspark.sql import functions as F  # noqa: N812

        df = self.spark.table(self.resolve_table(table_name))
        if partition_column and partition_value:
            df = df.where(F.to_date(F.col(partition_column)) == F.lit(partition_value))
        return df

    def frame(
        self,
        table_name: str,
        partition_column: str | None = None,
        partition_value: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        df = self._df(table_name, partition_column, partition_value)
        if columns is not None:
            df = self._project(df, columns)
        if self.row_limit:
            df = df.limit(self.row_limit)
        pdf = df.toPandas()
        # Flatten struct columns to <col>_<field> so the same checks work as on the SQLite mirror.
        for col in list(pdf.columns):
            if len(pdf) and isinstance(pdf[col].iloc[0], dict):
                expanded = pd.DataFrame(pdf[col].tolist()).add_prefix(f"{col}_")
                pdf = pd.concat([pdf.drop(columns=[col]), expanded], axis=1)
        return pdf

    @staticmethod
    def _project(df: Any, columns: list[str]) -> Any:
        """Select ``columns``, reading ``psml_x`` from ``psml.x``.

        Whole wide tables do not fit: procedures.kinematics at 284k rows of 76 doubles ran 1 GB
        executors out of heap in ``toPandas``. One column stays selected so that row counts work
        when the checks read none, and names that do not exist are left for the checks to report.
        """
        from pyspark.sql import functions as F  # noqa: N812

        children = {
            f"{field.name}_{child}": f"{field.name}.{child}"
            for field in df.schema.fields
            if hasattr(field.dataType, "fieldNames")
            for child in field.dataType.fieldNames()
        }
        selected = [
            F.col(c) if c in df.columns else F.col(children[c]).alias(c)
            for c in columns
            if c in df.columns or c in children
        ]
        return df.select(*selected) if selected else df.select(df.columns[0])

    def columns(self, table_name: str) -> dict[str, str]:
        df = self.spark.table(self.resolve_table(table_name))
        return {f.name: f.dataType.simpleString() for f in df.schema.fields}

    def sql(self, query: str) -> float | None:
        rows = self.spark.sql(query).collect()
        return _scalar(rows[0][0] if rows else None)
