"""DAG ``telemetry_backfill`` — manual: reprocess a date range (optionally one robot).

Trigger with config::

    {"start_date": "2026-10-01", "end_date": "2026-10-07", "robot_id": null}

    plan_dates -> batch_mcap[date] (mapped, max 4 parallel) -> quality_gate[date] (mapped)
        -> publish_marts (every date's mart partitions, then the catalog)
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from rtdaas.airflow.dags._common import (
        DEFAULT_ARGS,
        TAGS,
        day_range,
        notify_failure,
        spark_task,
        spark_task_mapped,
        suite_path,
    )
except ImportError:
    from _common import (
        DEFAULT_ARGS,
        TAGS,
        day_range,
        notify_failure,
        spark_task,
        spark_task_mapped,
        suite_path,
    )
with DAG(
    dag_id="telemetry_backfill",
    description="Reprocess bags for a date range: batch_mcap + quality gate per date, then publish them all.",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    params={
        "start_date": Param(
            "2026-10-01", type="string", format="date", description="first day (inclusive)"
        ),
        "end_date": Param(
            "2026-10-01", type="string", format="date", description="last day (inclusive)"
        ),
        "robot_id": Param(None, type=["null", "string"], description="restrict to one robot id"),
    },
    tags=TAGS + ["backfill"],
    doc_md=__doc__,
) as dag:

    @task
    def plan_dates(**context) -> list[str]:
        p = context["params"]
        dates = day_range(p["start_date"], p["end_date"])
        if len(dates) > 62:
            raise ValueError(f"refusing to backfill {len(dates)} days in one run; split the range")
        return dates

    @task
    def batch_args(dates: list[str], **context) -> list[list[str]]:
        robot = context["params"].get("robot_id")
        extra = ["--robot", robot] if robot else []
        return [["--date", d, *extra] for d in dates]

    @task
    def gate_args(dates: list[str]) -> list[list[str]]:
        return [["--suite", suite_path("telemetry.yml"), "--partition", d] for d in dates]

    @task
    def publish_dates(dates: list[str]) -> str:
        return ",".join(dates)

    dates = plan_dates()
    batch = spark_task_mapped(
        "batch_mcap", "batch_mcap.py", batch_args(dates), executors=4, max_active_tis_per_dag=4
    )
    gate = spark_task_mapped(
        "quality_gate", "quality_gate.py", gate_args(dates), executors=1, retries=0
    )

    publish = spark_task(
        "publish_marts",
        "publish_marts.py",
        ["--date", "{{ ti.xcom_pull(task_ids='publish_dates') }}"],
        executors=1,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    batch >> gate >> publish_dates(dates) >> publish
