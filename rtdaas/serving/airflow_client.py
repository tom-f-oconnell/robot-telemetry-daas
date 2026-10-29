"""Trigger Airflow DAG runs from the API (``POST /api/jobs/reprocess``).

Uses the stable REST API (``/api/v1/dags/{dag_id}/dagRuns``) with basic auth.  A connection
failure is reported as ``AirflowUnavailable`` so the API can record the request locally and
answer 202 instead of 500.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import requests

from rtdaas.config import Settings

log = logging.getLogger(__name__)

DAG_FOR_KIND = {
    "session": "telemetry_backfill",
    "date": "telemetry_backfill",
    "trial": "jigsaws_ingest",
}


class AirflowUnavailable(RuntimeError):
    pass


def build_conf(kind: str, target: str, robot_id: str | None = None) -> tuple[str, dict]:
    """Map a reprocess request to (dag_id, conf)."""
    if kind not in DAG_FOR_KIND:
        raise ValueError(f"unknown kind {kind!r}; expected one of {sorted(DAG_FOR_KIND)}")
    dag_id = DAG_FOR_KIND[kind]
    if kind == "date":
        return dag_id, {"start_date": target, "end_date": target, "robot_id": robot_id}
    if kind == "session":
        # session_id = <robot_id>-<yyyymmddTHHMMSS>  ->  the day it started
        robot, _, stamp = target.rpartition("-")
        try:
            day = datetime.strptime(stamp, "%Y%m%dT%H%M%S").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(
                f"session_id {target!r} does not end in a yyyymmddTHHMMSS stamp"
            ) from exc
        return dag_id, {"start_date": day, "end_date": day, "robot_id": robot or robot_id}
    # trial: Suturing_B001 -> task Suturing
    task = target.rsplit("_", 1)[0] if "_" in target else target
    return dag_id, {"task": task, "trial_id": target}


def trigger_dag(settings: Settings, dag_id: str, conf: dict, timeout: float = 5.0) -> str:
    """Create a DAG run and return its ``dag_run_id``."""
    run_id = f"api__{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    url = f"{settings.airflow_api_url.rstrip('/')}/dags/{dag_id}/dagRuns"
    try:
        r = requests.post(
            url,
            json={"dag_run_id": run_id, "conf": conf},
            auth=(settings.airflow_user, settings.airflow_password),
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise AirflowUnavailable(str(exc)) from exc
    if r.status_code >= 400:
        raise AirflowUnavailable(f"airflow returned {r.status_code}: {r.text[:200]}")
    return r.json().get("dag_run_id", run_id)
