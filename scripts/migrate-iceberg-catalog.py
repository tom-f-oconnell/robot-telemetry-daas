#!/usr/bin/env python3
"""Move Iceberg tables from the old SQLite catalog file to the current catalog.

The catalog only stores where each table's metadata file is, so moving it loses no data, but a
catalog that starts empty hides every table. Registers each table the file lists. Standard library
only, and it does not change the file.

Stop whatever commits to the old catalog first, and start the new one before anything else:

    docker stop rtdaas-streaming rtdaas-airflow-scheduler rtdaas-iceberg-rest
    docker cp rtdaas-iceberg-rest:/home/iceberg /tmp/old-catalog
    docker compose --project-directory infra/compose --profile core up -d iceberg-rest
    python3 scripts/migrate-iceberg-catalog.py /tmp/old-catalog/iceberg_rest.db http://localhost:8181

Then `make demo` starts the rest. On Kubernetes, port-forward the catalog and read the file from the
old PVC, `iceberg-rest-data`.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request


def _request(url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data, headers), timeout=60) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        text = exc.read().decode(errors="replace")
        try:
            return exc.code, json.loads(text)
        except ValueError:
            return exc.code, {"error": {"message": text[:300]}}


def migrate(sqlite_path: str, catalog_url: str) -> int:
    """Register every table in the file with the catalog. Returns how many failed."""
    catalog_url = catalog_url.rstrip("/")
    with sqlite3.connect(sqlite_path) as db:
        rows = db.execute(
            "SELECT table_namespace, table_name, metadata_location FROM iceberg_tables ORDER BY 1, 2"
        ).fetchall()
    if not rows:
        print(f"no tables in {sqlite_path}", file=sys.stderr)
        return 1

    for namespace in sorted({row[0] for row in rows}):
        status, reply = _request(f"{catalog_url}/v1/namespaces", {"namespace": namespace.split(".")})
        if status not in (200, 409):
            print(f"namespace {namespace}: HTTP {status} {reply}", file=sys.stderr)
            return len(rows)

    failed = 0
    for namespace, name, location in rows:
        path = urllib.parse.quote(namespace, safe="")
        status, reply = _request(
            f"{catalog_url}/v1/namespaces/{path}/register", {"name": name, "metadata-location": location}
        )
        if status == 200 or status == 409:
            status, reply = _request(f"{catalog_url}/v1/namespaces/{path}/tables/{name}")
        registered = reply.get("metadata-location") if status == 200 else None
        if registered == location:
            print(f"{namespace}.{name}: {location.rsplit('/', 1)[-1]}")
        else:
            failed += 1
            print(f"{namespace}.{name}: FAILED, HTTP {status}, catalog has {registered}: {reply.get('error')}")
    print(f"{len(rows) - failed} of {len(rows)} tables registered")
    return failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sqlite", help="the old catalog's iceberg_rest.db, with its -wal file beside it")
    parser.add_argument("catalog", help="the new catalog's URL, e.g. http://localhost:8181")
    args = parser.parse_args()
    return 1 if migrate(args.sqlite, args.catalog) else 0


if __name__ == "__main__":
    sys.exit(main())
