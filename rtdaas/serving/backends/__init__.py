"""Query backends for the data API.

``SqliteBackend`` reads the local mirror written by ``rtdaas.local`` (dev, tests, demo);
``DuckDBBackend`` reads Iceberg tables (production) and optionally Elasticsearch.
"""

from __future__ import annotations

from rtdaas.config import Settings
from rtdaas.serving.backends.base import Backend


def make_backend(settings: Settings) -> Backend:
    """Instantiate the backend named by ``settings.serving_backend``."""
    kind = (settings.serving_backend or "sqlite").lower()
    if kind == "sqlite":
        from rtdaas.serving.backends.sqlite import SqliteBackend

        return SqliteBackend(settings.serving_sqlite_path, settings=settings)
    if kind == "duckdb":
        from rtdaas.serving.backends.duckdb import DuckDBBackend

        return DuckDBBackend(settings)
    raise ValueError(f"unknown SERVING_BACKEND {kind!r}; expected 'sqlite' or 'duckdb'")


__all__ = ["Backend", "make_backend"]
