"""DAG ``telemetry_bag_ingest`` — daily: bags for the day -> raw tables -> sessions/KPIs -> gate -> publish.

wait_for_bags (S3KeySensor) -> spark_batch_mcap -> quality_gate -> publish_marts -> index_es_sessions
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# the copy next to this file first: git-sync updates it together with the DAGs
try:
    from _common import (
        DEFAULT_ARGS,
        S3_CONN_ID,
        TAGS,
        es_doc_count,
        notify_failure,
        rtdaas_env,
        spark_task,
        suite_path,
    )
except ImportError:  # DAG files outside their folder (the installed package)
    from rtdaas.airflow.dags._common import (
        DEFAULT_ARGS,
        S3_CONN_ID,
        TAGS,
        es_doc_count,
        notify_failure,
        rtdaas_env,
        spark_task,
        suite_path,
    )
log = logging.getLogger(__name__)

RAW_BUCKET = rtdaas_env().get("S3_BUCKET_RAW", "rtdaas-raw")
# {{ ds }} is YYYY-MM-DD; the landing layout is bags/<robot>/<yyyy>/<mm>/<dd>/<session>.mcap
BAG_KEY_PATTERN = "bags/*/{{ ds[:4] }}/{{ ds[5:7] }}/{{ ds[8:10] }}/*.mcap"


def _verify_sessions_indexed(ds: str, **_) -> int:
    day_start, day_end = f"{ds}T00:00:00Z", f"{ds}T23:59:59Z"
    n = es_doc_count("rtdaas-sessions", {"range": {"start_ts": {"gte": day_start, "lte": day_end}}})
    log.info("rtdaas-sessions documents for %s: %d", ds, n)
    if n == 0:
        log.warning("no sessions indexed for %s (no bags, or ES indexing skipped)", ds)
    return n


with DAG(
    dag_id="telemetry_bag_ingest",
    description="Daily MCAP bag ingestion: raw tables, sessions, exact KPIs, quality gate, publish.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    tags=TAGS,
    doc_md=__doc__,
) as dag:
    wait_for_bags = S3KeySensor(
        task_id="wait_for_bags",
        bucket_name=RAW_BUCKET,
        bucket_key=BAG_KEY_PATTERN,
        wildcard_match=True,
        aws_conn_id=S3_CONN_ID,
        mode="reschedule",
        poke_interval=10 * 60,
        timeout=6 * 60 * 60,
        soft_fail=False,
    )

    spark_batch_mcap = spark_task(
        "spark_batch_mcap", "batch_mcap.py", ["--date", "{{ ds }}"], executors=2
    )

    quality_gate = spark_task(
        "quality_gate",
        "quality_gate.py",
        ["--suite", suite_path("telemetry.yml"), "--partition", "{{ ds }}"],
        executors=1,
        retries=0,
    )

    publish_marts = spark_task(
        "publish_marts", "publish_marts.py", ["--date", "{{ ds }}"], executors=1
    )

    index_es_sessions = PythonOperator(
        task_id="index_es_sessions",
        python_callable=_verify_sessions_indexed,
        execution_timeout=dt.timedelta(minutes=5),
    )

    wait_for_bags >> spark_batch_mcap >> quality_gate >> publish_marts >> index_es_sessions
