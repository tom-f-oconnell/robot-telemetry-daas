"""Write pandas frames into the SQLite mirror of the lakehouse (spec §3.3).

Conventions: ``telemetry.sessions`` -> table ``telemetry__sessions``; arrays stored as JSON
text; struct columns arrive already flattened (``psml_x``); timestamps stored as ISO-8601 UTC
strings ``YYYY-MM-DDTHH:MM:SS.ffffff+00:00``.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from rtdaas.quality.schemas import SPEC_SCHEMAS, flattened_columns, sqlite_table_name

log = logging.getLogger(__name__)

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"

# Indices created after writes (table -> list of column tuples).
INDICES: dict[str, list[tuple[str, ...]]] = {
    "telemetry.tool_pose_raw": [("robot_id", "ts"), ("robot_id", "arm", "ts")],
    "telemetry.joint_states_raw": [("robot_id", "ts")],
    "telemetry.events_raw": [("robot_id", "ts"), ("code",)],
    "telemetry.heartbeat_raw": [("robot_id", "ts")],
    "telemetry.kpi_1m": [("robot_id", "arm", "window_start")],
    "telemetry.sessions": [("session_id",), ("robot_id", "start_ts")],
    "procedures.trials": [("trial_id",), ("task",)],
    "procedures.kinematics": [("trial_id", "frame_idx")],
    "procedures.gestures": [("trial_id", "start_frame"), ("task", "gesture")],
    "procedures.frame_features": [("trial_id", "frame_idx")],
    "dq.check_results": [("run_id",), ("table_name",)],
}

JOBS_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    target TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    status TEXT NOT NULL,
    dag_run_id TEXT
)
"""

CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS catalog (
    name TEXT PRIMARY KEY,
    owner TEXT,
    description TEXT,
    freshness_target_hours REAL,
    row_count INTEGER,
    last_updated TEXT,
    freshness_ok INTEGER,
    contract TEXT,
    columns_json TEXT
)
"""


def format_ts(value) -> str | None:
    """Timestamp-like -> ISO-8601 UTC string (None stays None)."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    t = pd.Timestamp(value)
    if pd.isna(t):
        return None
    t = t.tz_convert("UTC") if t.tzinfo is not None else t.tz_localize("UTC")
    return t.strftime(TS_FORMAT)


def now_iso() -> str:
    return format_ts(datetime.now(UTC)) or ""


def _is_listlike(v) -> bool:
    return isinstance(v, (list, tuple, dict, np.ndarray))


def to_sqlite_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce a frame into SQLite-friendly dtypes (JSON text for arrays, ISO strings for timestamps)."""
    out = frame.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_datetime64_any_dtype(s.dtype):
            s = pd.to_datetime(s, utc=True)
            out[col] = s.dt.strftime(TS_FORMAT).astype(object).where(s.notna(), None)
        elif s.dtype == object or pd.api.types.is_string_dtype(s.dtype):
            if len(s) and s.map(_is_listlike).any():
                out[col] = s.map(
                    lambda v: (
                        json.dumps(
                            v.tolist()
                            if isinstance(v, np.ndarray)
                            else (list(v) if isinstance(v, tuple) else v)
                        )
                        if _is_listlike(v)
                        else v
                    )
                ).astype(object)
            elif len(s) and s.map(lambda v: isinstance(v, (pd.Timestamp, datetime))).any():
                out[col] = s.map(
                    lambda v: format_ts(v) if isinstance(v, (pd.Timestamp, datetime)) else v
                ).astype(object)
        elif pd.api.types.is_bool_dtype(s.dtype):
            out[col] = s.astype("int64")
    return out


class SqliteStore:
    """Thin wrapper over a SQLite connection implementing the §3.3 mirror conventions."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(JOBS_DDL)
        self.conn.execute(CATALOG_DDL)
        self.conn.commit()

    # ------------------------------------------------------------------ writes
    def write(self, table_name: str, frame: pd.DataFrame, mode: str = "replace") -> int:
        """Write ``frame`` to the mirror table for ``table_name`` (``replace`` or ``append``)."""
        if mode not in ("replace", "append"):
            raise ValueError("mode must be 'replace' or 'append'")
        name = sqlite_table_name(table_name)
        frame = self._conform(table_name, frame)
        sql_frame = to_sqlite_frame(frame)
        if len(sql_frame) == 0 and mode == "replace":
            self.conn.execute(f'DROP TABLE IF EXISTS "{name}"')
            cols = ", ".join(f'"{c}"' for c in sql_frame.columns) or '"_empty" INTEGER'
            self.conn.execute(f'CREATE TABLE "{name}" ({cols})')
        else:
            sql_frame.to_sql(name, self.conn, if_exists=mode, index=False, chunksize=5000)
        self._create_indices(table_name)
        self.conn.commit()
        log.debug("wrote %d rows to %s (%s)", len(sql_frame), name, mode)
        return len(sql_frame)

    def _conform(self, table_name: str, frame: pd.DataFrame) -> pd.DataFrame:
        """Add missing spec columns (as NULL) and order columns spec-first, extras last."""
        if table_name not in SPEC_SCHEMAS:
            return frame
        expected = list(flattened_columns(table_name).keys())
        out = frame.copy()
        for c in expected:
            if c not in out.columns:
                out[c] = None
        extras = [c for c in out.columns if c not in expected]
        return out[expected + extras]

    def _create_indices(self, table_name: str) -> None:
        name = sqlite_table_name(table_name)
        existing = {r[1] for r in self.conn.execute(f'PRAGMA table_info("{name}")').fetchall()}
        for cols in INDICES.get(table_name, []):
            if not all(c in existing for c in cols):
                continue
            idx = f"ix_{name}_{'_'.join(cols)}"
            col_sql = ", ".join(f'"{c}"' for c in cols)
            self.conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{name}" ({col_sql})')

    def write_catalog(self, entries: Iterable[dict]) -> int:
        """Replace the catalog, table included, so a mirror from an older schema still takes it."""
        rows = [
            (
                e["name"],
                e.get("owner"),
                e.get("description"),
                e.get("freshness_target_hours"),
                e.get("row_count"),
                e.get("last_updated"),
                int(bool(e.get("freshness_ok", True))),
                e.get("contract"),
                json.dumps(e.get("columns", [])),
            )
            for e in entries
        ]
        self.conn.execute("DROP TABLE IF EXISTS catalog")
        self.conn.executescript(CATALOG_DDL)
        self.conn.executemany(
            "INSERT OR REPLACE INTO catalog (name, owner, description, freshness_target_hours, row_count, last_updated, freshness_ok, contract, columns_json) VALUES (?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.conn.commit()
        return len(rows)

    def record_job(
        self,
        job_id: str,
        kind: str,
        target: str,
        status: str = "queued",
        dag_run_id: str | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO jobs (job_id, kind, target, requested_at, status, dag_run_id) VALUES (?,?,?,?,?,?)",
            (job_id, kind, target, now_iso(), status, dag_run_id),
        )
        self.conn.commit()

    # ------------------------------------------------------------------ reads
    def table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (sqlite_table_name(table_name),),
        ).fetchone()
        return row is not None

    def read(self, table_name: str, where: str | None = None, params: tuple = ()) -> pd.DataFrame:
        name = sqlite_table_name(table_name)
        q = f'SELECT * FROM "{name}"' + (f" WHERE {where}" if where else "")
        return pd.read_sql_query(q, self.conn, params=params)

    def row_count(self, table_name: str) -> int:
        if not self.table_exists(table_name):
            return 0
        return int(
            self.conn.execute(f'SELECT COUNT(*) FROM "{sqlite_table_name(table_name)}"').fetchone()[
                0
            ]
        )

    def max_value(self, table_name: str, column: str):
        if not self.table_exists(table_name):
            return None
        cols = {
            r[1]
            for r in self.conn.execute(
                f'PRAGMA table_info("{sqlite_table_name(table_name)}")'
            ).fetchall()
        }
        if column not in cols:
            return None
        return self.conn.execute(
            f'SELECT MAX("{column}") FROM "{sqlite_table_name(table_name)}"'
        ).fetchone()[0]

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> SqliteStore:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
