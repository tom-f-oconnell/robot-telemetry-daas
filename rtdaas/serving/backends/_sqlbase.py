"""Shared query logic for SQL backends (SQLite mirror and DuckDB/Iceberg).

The two backends differ only in table naming, timestamp handling, struct access and how a
query is executed, so the API-facing methods live here and subclasses override a handful of
small hooks.  All user input goes through ``?`` parameters — never into the SQL text.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from rtdaas import __version__
from rtdaas.config import DEFAULT_AVRO_DIR, Settings
from rtdaas.serving.backends.base import ARM_FIELDS, KINEMATIC_ARMS

log = logging.getLogger(__name__)

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"
CATALOG_YML = Path(__file__).resolve().parent.parent / "catalog.yml"

ARRAY_COLUMNS = {"joint_names", "position", "velocity", "effort", "active_arms"}

SESSION_LIST_SQL = """
SELECT session_id, robot_id,
       MIN(start_ts) AS start_ts, MAX(end_ts) AS end_ts, MAX(duration_s) AS duration_s,
       COUNT(*) AS n_arms, SUM(n_samples) AS n_samples, SUM(path_length_m) AS path_length_m,
       AVG(mean_speed_mps) AS mean_speed_mps, AVG(idle_fraction) AS idle_fraction,
       SUM(jaw_cycles) AS jaw_cycles, MAX(warn_events) AS warn_events, MAX(error_events) AS error_events,
       MAX(instrument_swaps) AS instrument_swaps, MAX(bag_file) AS bag_file,
       MAX(pipeline_version) AS pipeline_version, MAX(processed_at) AS processed_at
FROM {sessions}
{where}
GROUP BY session_id, robot_id
ORDER BY MIN(start_ts) DESC
LIMIT ? OFFSET ?
"""


def norm_ts(value: str | None, end: bool = False) -> str | None:
    """Normalise an ISO date/time string to the mirror's canonical UTC format.

    A bare date is the start of that day, or the end of it when ``end`` is true.
    Raises ``ValueError`` for unparsable input (the API maps it to a 400).
    """
    if value is None or value == "":
        return None
    t = pd.Timestamp(value)
    if pd.isna(t):
        raise ValueError(f"invalid timestamp {value!r}")
    if end and len(value.strip()) == 10:
        t = t + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    t = t.tz_convert("UTC") if t.tzinfo is not None else t.tz_localize("UTC")
    return t.strftime(TS_FORMAT)


def to_iso(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        t = pd.Timestamp(value)
        t = t.tz_convert("UTC") if t.tzinfo is not None else t.tz_localize("UTC")
        return t.strftime(TS_FORMAT)
    if isinstance(value, date):  # DATE columns (the marts' `day`) are not JSON-serialisable
        return value.isoformat()
    return value


def load_catalog_yml(path: Path = CATALOG_YML) -> list[dict]:
    try:
        doc = yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError:
        return []
    return list(doc.get("tables", []))


class SqlBackendBase:
    """Concrete API methods on top of a small set of engine hooks."""

    name = "sql"

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self._catalog_yml = load_catalog_yml()
        self._registry_cache: tuple[float, list[dict]] | None = None

    # ------------------------------------------------------------------ engine hooks
    def table(self, name: str) -> str:
        """Map a logical table name (``telemetry.sessions``) to the engine's identifier."""
        raise NotImplementedError

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Run a SELECT and return rows as dicts (engine-native values)."""
        raise NotImplementedError

    def execute(self, sql: str, params: tuple = ()) -> None:
        raise NotImplementedError

    def table_exists(self, name: str) -> bool:
        raise NotImplementedError

    def ts_param(self) -> str:
        """Placeholder expression for a timestamp parameter (``?`` or ``CAST(? AS TIMESTAMP)``)."""
        return "?"

    def arm_col(self, arm: str, field: str) -> str:
        """SQL expression selecting a flattened kinematic struct field, aliased ``arm_field``."""
        return f"{arm}_{field}"

    def post_row(self, row: dict) -> dict:
        """Normalise engine-native values to JSON (ISO timestamps, decoded arrays)."""
        out = {}
        for k, v in row.items():
            if k in ARRAY_COLUMNS and isinstance(v, str):
                try:
                    v = json.loads(v)
                except ValueError:
                    pass
            elif isinstance(v, float) and v != v:  # NaN -> null
                v = None
            out[k] = to_iso(v)
        return out

    def rows(self, sql: str, params: tuple = ()) -> list[dict]:
        return [self.post_row(r) for r in self.query(sql, params)]

    def one(self, sql: str, params: tuple = ()) -> dict | None:
        res = self.rows(sql, params)
        return res[0] if res else None

    # ------------------------------------------------------------------ catalog / contracts
    def catalog(self) -> list[dict]:
        yml = {e["name"]: e for e in self._catalog_yml}
        rows: list[dict] = []
        if self.table_exists("catalog"):
            for r in self.rows(f"SELECT * FROM {self.table('catalog')} ORDER BY name"):
                cols = r.pop("columns_json", None)
                try:
                    columns = json.loads(cols) if isinstance(cols, str) else (cols or [])
                except ValueError:
                    columns = []
                entry = dict(yml.get(r["name"], {}))
                entry.update({k: v for k, v in r.items() if v is not None or k not in entry})
                entry["columns"] = columns or entry.get("columns", [])
                entry["freshness_ok"] = bool(entry.get("freshness_ok", True))
                rows.append(entry)
        seen = {r["name"] for r in rows}
        for name, e in yml.items():
            if name not in seen:
                entry = dict(e)
                entry.setdefault("row_count", None)
                entry.setdefault("last_updated", None)
                entry.setdefault("freshness_ok", None)
                rows.append(entry)
        rows.sort(key=lambda e: e["name"])
        for e in rows:
            e.pop("partitioning", None)
        return rows

    def freshness(self) -> dict[str, dict]:
        """Per-table last-updated and SLA status, computed live from the tables."""
        now = datetime.now(UTC)
        out: dict[str, dict] = {}
        for e in self._catalog_yml:
            name, col, sla = e["name"], e.get("freshness_column"), e.get("sla_hours")
            info: dict[str, Any] = {
                "sla_hours": sla,
                "last_updated": None,
                "row_count": None,
                "fresh": None,
            }
            if self.table_exists(name):
                try:
                    r = self.one(f"SELECT COUNT(*) AS n FROM {self.table(name)}")
                    info["row_count"] = int(r["n"]) if r else 0
                    if col:
                        r = self.one(f"SELECT MAX({col}) AS m FROM {self.table(name)}")
                        info["last_updated"] = r["m"] if r else None
                except Exception as exc:  # pragma: no cover - engine specific
                    log.warning("freshness query failed for %s: %s", name, exc)
            if info["last_updated"] and sla:
                last = pd.Timestamp(info["last_updated"])  # DATE / naive values are UTC
                last = last.tz_localize(UTC) if last.tzinfo is None else last
                age_h = (now - last.to_pydatetime()).total_seconds() / 3600
                info["age_hours"] = round(age_h, 2)
                info["fresh"] = age_h <= float(sla)
            out[name] = info
        return out

    def contracts(self) -> list[dict]:
        """Committed Kafka envelope schemas, plus registry versions when the registry answers."""
        out: list[dict] = []
        kafka_dir = DEFAULT_AVRO_DIR / "kafka"
        for p in sorted(kafka_dir.glob("*.avsc")) if kafka_dir.exists() else []:
            try:
                schema = json.loads(p.read_text())
            except ValueError:
                continue
            topic = p.stem
            out.append(
                {
                    "subject": f"{topic}-value",
                    "topic": topic,
                    "source": "repo",
                    "version": None,
                    "schema_id": None,
                    "schema": schema,
                    "fields": _field_summary(schema),
                }
            )
        for entry in out:
            reg = self._registry_latest(entry["subject"])
            if reg:
                entry.update(
                    {"version": reg["version"], "schema_id": reg["id"], "source": "registry"}
                )
                entry["registry_schema_matches_repo"] = reg["schema"] == entry["schema"]
        return out

    def _registry_latest(self, subject: str) -> dict | None:
        url = self.settings.schema_registry_url
        if not url:
            return None
        now = time.monotonic()
        if self._registry_cache and now - self._registry_cache[0] < 300:
            cached = {e["subject"]: e for e in self._registry_cache[1]}
            return cached.get(subject)
        results: list[dict] = []
        try:
            import requests

            r = requests.get(f"{url.rstrip('/')}/subjects", timeout=1.0)
            r.raise_for_status()
            for s in r.json():
                lr = requests.get(f"{url.rstrip('/')}/subjects/{s}/versions/latest", timeout=1.0)
                if lr.ok:
                    body = lr.json()
                    schema = body.get("schema")
                    if isinstance(schema, str):
                        schema = json.loads(schema)
                    results.append(
                        {
                            "subject": s,
                            "version": body.get("version"),
                            "id": body.get("id"),
                            "schema": schema,
                        }
                    )
        except Exception as exc:  # registry optional: any failure -> cache empty for 5 min
            log.debug("schema registry unavailable (%s): %s", url, exc)
        self._registry_cache = (now, results)
        return {e["subject"]: e for e in results}.get(subject)

    # ------------------------------------------------------------------ telemetry
    def robots(self) -> list[dict]:
        if self.table_exists("telemetry.sessions"):
            return self.rows(
                f"""SELECT robot_id, MIN(start_ts) AS first_seen, MAX(end_ts) AS last_seen,
                           COUNT(DISTINCT session_id) AS n_sessions
                    FROM {self.table("telemetry.sessions")} GROUP BY robot_id ORDER BY robot_id"""
            )
        if self.table_exists("telemetry.tool_pose_raw"):
            return self.rows(
                f"""SELECT robot_id, MIN(ts) AS first_seen, MAX(ts) AS last_seen, 0 AS n_sessions
                    FROM {self.table("telemetry.tool_pose_raw")} GROUP BY robot_id ORDER BY robot_id"""
            )
        return []

    def sessions(self, robot_id=None, start=None, end=None, limit=200, offset=0) -> list[dict]:
        if not self.table_exists("telemetry.sessions"):
            return []
        clauses, params = [], []
        if robot_id:
            clauses.append("robot_id = ?")
            params.append(robot_id)
        if start:
            clauses.append(f"end_ts >= {self.ts_param()}")
            params.append(norm_ts(start))
        if end:
            clauses.append(f"start_ts <= {self.ts_param()}")
            params.append(norm_ts(end, end=True))
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = SESSION_LIST_SQL.format(sessions=self.table("telemetry.sessions"), where=where)
        rows = self.rows(sql, (*params, int(limit), int(offset)))
        for r in rows:
            r["arms"] = self._session_arms(r["session_id"])
        return rows

    def _session_arms(self, session_id: str) -> list[str]:
        return [
            r["arm"]
            for r in self.rows(
                f"SELECT arm FROM {self.table('telemetry.sessions')} WHERE session_id = ? ORDER BY arm",
                (session_id,),
            )
        ]

    def session(self, session_id: str) -> dict | None:
        if not self.table_exists("telemetry.sessions"):
            return None
        arms = self.rows(
            f"SELECT * FROM {self.table('telemetry.sessions')} WHERE session_id = ? ORDER BY arm",
            (session_id,),
        )
        if not arms:
            return None
        head = self.rows(
            SESSION_LIST_SQL.format(
                sessions=self.table("telemetry.sessions"), where="WHERE session_id = ?"
            ),
            (session_id, 1, 0),
        )[0]
        head["arms"] = [a["arm"] for a in arms]
        head["per_arm"] = arms
        head["events"] = self.events(
            robot_id=head["robot_id"], start=head["start_ts"], end=head["end_ts"], limit=500
        )
        return head

    def _session_span(self, session_id: str) -> tuple[str, str, str] | None:
        r = self.one(
            f"""SELECT robot_id, MIN(start_ts) AS s, MAX(end_ts) AS e FROM {self.table("telemetry.sessions")}
                WHERE session_id = ? GROUP BY robot_id""",
            (session_id,),
        )
        if not r or r["s"] is None:
            return None
        return r["robot_id"], r["s"], r["e"]

    def trajectory(self, session_id: str, arm: str, downsample: int = 1) -> list[dict]:
        span = self._session_span(session_id)
        if span is None or not self.table_exists("telemetry.tool_pose_raw"):
            return []
        robot_id, s, e = span
        n = max(1, int(downsample))
        tp = self.ts_param()
        sql = f"""
            SELECT ts, x, y, z, qx, qy, qz, qw, jaw_angle, speed FROM (
                SELECT ts, x, y, z, qx, qy, qz, qw, jaw_angle, speed,
                       ROW_NUMBER() OVER (ORDER BY ts) - 1 AS rn
                FROM {self.table("telemetry.tool_pose_raw")}
                WHERE robot_id = ? AND arm = ? AND ts >= {tp} AND ts <= {tp}
            ) t WHERE rn % ? = 0 ORDER BY ts
        """
        return self.rows(sql, (robot_id, arm, norm_ts(s), norm_ts(e), n))

    def session_kpis(self, session_id: str) -> list[dict]:
        span = self._session_span(session_id)
        if span is None or not self.table_exists("telemetry.kpi_1m"):
            return []
        robot_id, s, e = span
        s_floor = pd.Timestamp(s).floor("1min").strftime(TS_FORMAT)
        tp = self.ts_param()
        return self.rows(
            f"""SELECT * FROM {self.table("telemetry.kpi_1m")}
                WHERE robot_id = ? AND window_start >= {tp} AND window_start <= {tp}
                ORDER BY window_start, arm""",
            (robot_id, s_floor, norm_ts(e)),
        )

    def events(
        self, q=None, robot_id=None, severity_min=None, start=None, end=None, limit=200
    ) -> list[dict]:
        if not self.table_exists("telemetry.events_raw"):
            return []
        clauses, params = [], []
        if q:
            clauses.append("(code LIKE ? OR message LIKE ? OR arm LIKE ?)")
            like = f"%{q}%"
            params += [like, like, like]
        if robot_id:
            clauses.append("robot_id = ?")
            params.append(robot_id)
        if severity_min is not None:
            clauses.append("severity >= ?")
            params.append(int(severity_min))
        if start:
            clauses.append(f"ts >= {self.ts_param()}")
            params.append(norm_ts(start))
        if end:
            clauses.append(f"ts <= {self.ts_param()}")
            params.append(norm_ts(end, end=True))
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        return self.rows(
            f"SELECT * FROM {self.table('telemetry.events_raw')} {where} ORDER BY ts DESC LIMIT ?",
            (*params, int(limit)),
        )

    # ------------------------------------------------------------------ procedures
    def trials(self, task=None, skill_level=None, subject=None) -> list[dict]:
        if not self.table_exists("procedures.trials"):
            return []
        clauses, params = [], []
        for col, val in (("task", task), ("skill_level", skill_level), ("subject", subject)):
            if val:
                clauses.append(f"{col} = ?")
                params.append(val)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        return self.rows(
            f"SELECT * FROM {self.table('procedures.trials')} {where} ORDER BY task, trial_id",
            tuple(params),
        )

    def trial(self, trial_id: str) -> dict | None:
        if not self.table_exists("procedures.trials"):
            return None
        t = self.one(
            f"SELECT * FROM {self.table('procedures.trials')} WHERE trial_id = ?", (trial_id,)
        )
        if t is None:
            return None
        t["gestures"] = (
            self.rows(
                f"SELECT * FROM {self.table('procedures.gestures')} WHERE trial_id = ? ORDER BY start_frame",
                (trial_id,),
            )
            if self.table_exists("procedures.gestures")
            else []
        )
        summary = None
        if self.table_exists("procedures.frame_features"):
            summary = self.one(
                f"""SELECT COUNT(*) AS n_frames, AVG(motion_energy) AS motion_energy_mean,
                           AVG(sharpness) AS sharpness_mean, AVG(mean_intensity) AS mean_intensity,
                           MAX(t_s) AS duration_s
                    FROM {self.table("procedures.frame_features")} WHERE trial_id = ?""",
                (trial_id,),
            )
        t["frame_summary"] = summary or {}
        return t

    def trial_kinematics(self, trial_id: str, arms: list[str], downsample: int = 1) -> list[dict]:
        if not self.table_exists("procedures.kinematics"):
            return []
        arms = [a.lower() for a in arms] or ["psml", "psmr"]
        bad = [a for a in arms if a not in KINEMATIC_ARMS]
        if bad:
            raise ValueError(f"unknown arm(s) {bad}; expected one of {list(KINEMATIC_ARMS)}")
        n = max(1, int(downsample))
        cols = ", ".join(f"{self.arm_col(a, f)} AS {a}_{f}" for a in arms for f in ARM_FIELDS)
        return self.rows(
            f"""SELECT frame_idx, t_s, gesture, {cols}
                FROM {self.table("procedures.kinematics")}
                WHERE trial_id = ? AND frame_idx % ? = 0 ORDER BY frame_idx""",
            (trial_id, n),
        )

    def trial_frames(self, trial_id: str) -> list[dict]:
        if not self.table_exists("procedures.frame_features"):
            return []
        return self.rows(
            f"""SELECT frame_idx, t_s, mean_intensity, motion_energy, sharpness, gesture
                FROM {self.table("procedures.frame_features")} WHERE trial_id = ? ORDER BY frame_idx""",
            (trial_id,),
        )

    def gestures(self, task=None, gesture=None, skill_level=None, limit=500) -> list[dict]:
        if not self.table_exists("procedures.gestures"):
            return []
        clauses, params = [], []
        for col, val in (("task", task), ("gesture", gesture), ("skill_level", skill_level)):
            if val:
                clauses.append(f"{col} = ?")
                params.append(val)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        return self.rows(
            f"""SELECT * FROM {self.table("procedures.gestures")} {where}
                ORDER BY task, trial_id, start_frame LIMIT ?""",
            (*params, int(limit)),
        )

    # ------------------------------------------------------------------ quality / jobs
    def quality_runs(self, limit: int = 50) -> list[dict]:
        if not self.table_exists("dq.check_results"):
            return []
        return self.rows(
            f"""SELECT run_id, MAX(run_ts) AS run_ts, MIN(suite) AS suite,
                       COUNT(*) AS n_checks,
                       SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS n_pass,
                       SUM(CASE WHEN status = 'fail' AND severity = 'blocking' THEN 1 ELSE 0 END) AS n_blocking_fail,
                       SUM(CASE WHEN status = 'fail' AND severity <> 'blocking' THEN 1 ELSE 0 END) AS n_warn_fail,
                       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS n_error,
                       SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS n_skipped,
                       MAX(partition_value) AS partition_value
                FROM {self.table("dq.check_results")}
                GROUP BY run_id ORDER BY MAX(run_ts) DESC LIMIT ?""",
            (int(limit),),
        )

    def quality_results(self, run_id=None, table_name=None, status=None, limit=500) -> list[dict]:
        if not self.table_exists("dq.check_results"):
            return []
        clauses, params = [], []
        for col, val in (("run_id", run_id), ("table_name", table_name), ("status", status)):
            if val:
                clauses.append(f"{col} = ?")
                params.append(val)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        return self.rows(
            f"""SELECT * FROM {self.table("dq.check_results")} {where}
                ORDER BY run_ts DESC, table_name, check_name LIMIT ?""",
            (*params, int(limit)),
        )

    def record_job(self, kind: str, target: str, dag_run_id: str | None, status: str) -> dict:
        job = {
            "job_id": uuid.uuid4().hex[:12],
            "kind": kind,
            "target": target,
            "requested_at": datetime.now(UTC).strftime(TS_FORMAT),
            "status": status,
            "dag_run_id": dag_run_id,
        }
        self.execute(
            f"""INSERT INTO {self.table("jobs")} (job_id, kind, target, requested_at, status, dag_run_id)
                VALUES (?, ?, ?, ?, ?, ?)""",
            (job["job_id"], kind, target, job["requested_at"], status, dag_run_id),
        )
        return job

    def jobs(self, limit: int = 50) -> list[dict]:
        if not self.table_exists("jobs"):
            return []
        return self.rows(
            f"SELECT * FROM {self.table('jobs')} ORDER BY requested_at DESC LIMIT ?", (int(limit),)
        )

    # ------------------------------------------------------------------ misc
    def health(self) -> dict:
        fresh = self.freshness()
        return {
            "status": "ok",
            "backend": self.name,
            "version": __version__,
            "tables": {
                k: {
                    "rows": v.get("row_count"),
                    "last_updated": v.get("last_updated"),
                    "fresh": v.get("fresh"),
                }
                for k, v in fresh.items()
            },
        }


def _field_summary(schema: dict) -> list[dict]:
    """Flatten an Avro record schema into ``[{path, type}]`` for display and diffing."""
    out: list[dict] = []

    def typ(t) -> str:
        if isinstance(t, str):
            return t
        if isinstance(t, list):
            return "union<" + ",".join(typ(x) for x in t) + ">"
        if isinstance(t, dict):
            kind = t.get("type")
            if kind == "array":
                return f"array<{typ(t['items'])}>"
            if kind == "map":
                return f"map<{typ(t['values'])}>"
            if kind == "record":
                return t.get("name", "record")
            return str(kind)
        return str(t)

    def walk(rec: dict, prefix: str) -> None:
        for f in rec.get("fields", []):
            t = f.get("type")
            path = f"{prefix}{f['name']}"
            out.append(
                {
                    "path": path,
                    "type": typ(t),
                    "has_default": "default" in f,
                    "default": f.get("default"),
                }
            )
            if isinstance(t, dict) and t.get("type") == "record":
                walk(t, path + ".")

    walk(schema, "")
    return out
