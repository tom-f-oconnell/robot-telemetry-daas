"""DAG ``dq_daily`` — 06:00 UTC: every quality suite against yesterday's partitions + lake maintenance.

    quality_gate[suite] (mapped over telemetry / procedures / streaming) -> dq_summary
    iceberg_maintenance (rewrite_data_files + expire_snapshots on the raw tables) runs in parallel.

Each suite is its own task instance so one failing suite does not hide the others; the
summary task fails (and notifies) if any suite reported blocking failures.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

# the copy next to this file first: git-sync updates it together with the DAGs
try:
    from _common import (
        DEFAULT_ARGS,
        TAGS,
        notify_failure,
        spark_task,
        spark_task_mapped,
        suite_path,
    )
except ImportError:  # DAG files outside their folder (the installed package)
    from rtdaas.airflow.dags._common import (
        DEFAULT_ARGS,
        TAGS,
        notify_failure,
        spark_task,
        spark_task_mapped,
        suite_path,
    )
SUITES = ("telemetry.yml", "procedures.yml", "streaming.yml")


with DAG(
    dag_id="dq_daily",
    description="Daily data-quality run over all suites plus Iceberg compaction/snapshot expiry.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    tags=TAGS + ["dq"],
    doc_md=__doc__,
) as dag:

    @task
    def suite_args(**context) -> list[list[str]]:
        # data_interval_end is the run's logical day; the partitions checked are the previous day.
        day = (context["data_interval_end"] - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        return [["--suite", suite_path(s), "--partition", day] for s in SUITES]

    gates = spark_task_mapped(
        "quality_gate", "quality_gate.py", suite_args(), executors=1, retries=0
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def dq_summary(**context) -> str:
        ti = context["ti"]
        failed = [
            t.map_index
            for t in ti.get_dagrun().get_task_instances(state=["failed", "upstream_failed"])
            if t.task_id == "quality_gate"
        ]
        if failed:
            raise RuntimeError(
                f"blocking quality failures in suites: {[SUITES[i] for i in failed if i >= 0]}"
            )
        return "all suites passed"

    maintenance = spark_task(
        "iceberg_maintenance", "maintenance.py", ["--older-than-days", "7"], executors=2
    )

    gates >> dq_summary()
