"""SQLite backend over the local mirror written by ``rtdaas.local`` (spec §3.3).

Table names use ``__`` instead of ``.``; arrays are JSON text; timestamps are ISO-8601 UTC
strings, which sort lexicographically so range filters are plain comparisons.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from rtdaas.config import Settings
from rtdaas.serving.backends._sqlbase import SqlBackendBase

log = logging.getLogger(__name__)

JOBS_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY, kind TEXT NOT NULL, target TEXT NOT NULL,
    requested_at TEXT NOT NULL, status TEXT NOT NULL, dag_run_id TEXT
)
"""


class SqliteBackend(SqlBackendBase):
    name = "sqlite"

    def __init__(self, path: str | Path, settings: Settings | None = None, create: bool = True):
        super().__init__(settings)
        self.path = Path(path)
        if not self.path.exists() and not create:
            raise FileNotFoundError(f"SQLite mirror not found: {self.path}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(JOBS_DDL)
        self._conn.commit()
        self._tables: set[str] | None = None

    # ------------------------------------------------------------------ hooks
    def table(self, name: str) -> str:
        return '"' + name.replace(".", "__") + '"'

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()

    def table_exists(self, name: str) -> bool:
        phys = name.replace(".", "__")
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?", (phys,)
            ).fetchone()
        return row is not None

    def close(self) -> None:
        with self._lock:
            self._conn.close()
