"""DuckDB backend: the production serving path over Iceberg (and optionally Elasticsearch).

Two modes, chosen at construction:

1. **Iceberg REST catalog** (DuckDB >= 1.2 with the ``iceberg`` extension): the lakehouse is
   attached as catalog ``lake`` and every query reads the live Iceberg tables::

       ATTACH '<warehouse>' AS lake (TYPE ICEBERG, ENDPOINT '<ICEBERG_REST_URI>', ...)

   DuckDB caches an attached catalog's tables and snapshots, so ``lake`` is re-attached once it
   is older than ``ICEBERG_REFRESH_S``; otherwise tables created or commits made after startup
   would never show up.

   Tables resolve to ``lake.telemetry.sessions`` etc.  Timestamps are native, arrays are native
   lists and ``procedures.kinematics`` struct columns are read as ``psml.x``.

2. **No catalog**: when it cannot be attached (DuckDB without the ``iceberg`` extension, REST
   endpoint unreachable), the lake tables are reported as absent and only the ``jobs`` table and
   the Elasticsearch-backed endpoints answer.

When ``ES_URL`` responds, ``events`` and ``gestures`` are served from Elasticsearch, which gives
free-text search; otherwise they fall back to SQL ``LIKE``.
"""

from __future__ import annotations

import logging
import threading
import time

from rtdaas.config import Settings
from rtdaas.serving.backends._sqlbase import SqlBackendBase
from rtdaas.serving.es import ElasticsearchClient

log = logging.getLogger(__name__)

ICEBERG_REFRESH_S = 30.0


class DuckDBBackend(SqlBackendBase):
    name = "duckdb"

    def __init__(self, settings: Settings | None = None):
        super().__init__(settings)
        try:
            import duckdb  # noqa: F401
        except ImportError as exc:  # pragma: no cover - depends on environment
            raise RuntimeError(
                "SERVING_BACKEND=duckdb requires the 'duckdb' package (pip install rtdaas[api])"
            ) from exc
        import duckdb

        self._lock = threading.Lock()
        self._conn = duckdb.connect(self.settings.serving_duckdb_path)
        self.mode = "unattached"  # until the Iceberg catalog is attached
        self._attach_iceberg()
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS jobs (
                   job_id VARCHAR PRIMARY KEY, kind VARCHAR, target VARCHAR,
                   requested_at VARCHAR, status VARCHAR, dag_run_id VARCHAR)"""
        )
        self.es = ElasticsearchClient(self.settings.es_url) if self.settings.es_url else None
        if self.es and not self.es.ping():
            log.info(
                "Elasticsearch not reachable at %s; events/gestures served from SQL",
                self.settings.es_url,
            )
            self.es = None

    # ------------------------------------------------------------------ setup
    def _attach_iceberg(self) -> None:
        s = self.settings
        try:
            self._conn.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
            self._conn.execute(
                f"""CREATE OR REPLACE SECRET lake_s3 (
                        TYPE S3, KEY_ID '{s.s3_access_key}', SECRET '{s.s3_secret_key}',
                        ENDPOINT '{s.s3_endpoint.replace("http://", "").replace("https://", "")}',
                        URL_STYLE 'path', USE_SSL {"true" if s.s3_endpoint.startswith("https") else "false"})"""
            )
            self._conn.execute(self._attach_sql())
            self._attached_at = time.monotonic()
            self.mode = "iceberg"
            log.info("attached Iceberg REST catalog %s as 'lake'", s.iceberg_rest_uri)
        except Exception as exc:
            log.warning("Iceberg catalog unavailable (%s); lake tables will be absent", exc)

    def _attach_sql(self) -> str:
        s = self.settings
        return f"""ATTACH '{s.iceberg_warehouse.replace("s3a://", "s3://")}' AS lake (
                    TYPE ICEBERG, ENDPOINT '{s.iceberg_rest_uri}', AUTHORIZATION_TYPE 'none')"""

    def _refresh_lake(self) -> None:
        """Re-attach ``lake`` when stale (see module docstring). Call with ``self._lock`` held."""
        if self.mode != "iceberg" or time.monotonic() - self._attached_at < ICEBERG_REFRESH_S:
            return
        try:
            self._conn.execute("DETACH DATABASE IF EXISTS lake")
            self._conn.execute(self._attach_sql())
            self._attached_at = time.monotonic()
        except Exception as exc:  # keep the old timestamp so the next query retries
            log.warning("re-attaching Iceberg catalog failed: %s", exc)

    # ------------------------------------------------------------------ hooks
    def table(self, name: str) -> str:
        if name == "jobs":
            return "jobs"
        if name == "catalog":
            return "lake.dq.catalog" if self.mode == "iceberg" else "dq.catalog"
        return f"lake.{name}" if self.mode == "iceberg" else name

    def ts_param(self) -> str:
        return "CAST(? AS TIMESTAMP)"

    def arm_col(self, arm: str, field: str) -> str:
        return f"{arm}.{field}"

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            self._refresh_lake()
            cur = self._conn.execute(sql, list(params))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._refresh_lake()
            self._conn.execute(sql, list(params))

    def table_exists(self, name: str) -> bool:
        if name == "jobs":
            return True
        try:
            with self._lock:
                self._refresh_lake()
                self._conn.execute(f"SELECT 1 FROM {self.table(name)} LIMIT 0")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ ES-backed search
    def events(self, q=None, robot_id=None, severity_min=None, start=None, end=None, limit=200):
        if self.es is not None:
            hits = self.es.search_events(
                q=q, robot_id=robot_id, severity_min=severity_min, start=start, end=end, limit=limit
            )
            if hits is not None:
                return hits
        return super().events(q, robot_id, severity_min, start, end, limit)

    def gestures(self, task=None, gesture=None, skill_level=None, limit=500):
        if self.es is not None:
            hits = self.es.search_gestures(
                task=task, gesture=gesture, skill_level=skill_level, limit=limit
            )
            if hits is not None:
                return hits
        return super().gestures(task, gesture, skill_level, limit)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
