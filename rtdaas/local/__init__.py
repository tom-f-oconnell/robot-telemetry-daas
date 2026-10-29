"""Serial, dependency-light pipeline that mirrors the Spark jobs into SQLite (spec §3.3, §4.7)."""

from rtdaas.local.sqlite_store import SqliteStore

__all__ = ["SqliteStore"]
