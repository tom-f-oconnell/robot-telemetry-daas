"""DAG ``jigsaws_ingest`` — manual / on dataset drop: JIGSAWS -> procedures.* -> gate -> publish -> ES check.

    discover -> spark_batch_jigsaws -> quality_gate -> publish_snowflake -> index_es_gestures

Trigger with config ``{"task": "Suturing"}`` to restrict to one task (default: all).
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

try:
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
except ImportError:
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
log = logging.getLogger(__name__)

RAW_BUCKET = rtdaas_env().get("S3_BUCKET_RAW", "rtdaas-raw")
JIGSAWS_PREFIX = "jigsaws/"


def _discover(**context) -> int:
    """Count kinematics files under the dataset prefix; fail fast when there is nothing to ingest."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    task_filter = (context.get("params") or {}).get("task")
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    keys = hook.list_keys(bucket_name=RAW_BUCKET, prefix=JIGSAWS_PREFIX) or []
    kin = [k for k in keys if "/kinematics/AllGestures/" in k and k.endswith(".txt")]
    if task_filter:
        kin = [k for k in kin if f"/{task_filter}/" in f"/{k}"]
    log.info(
        "found %d kinematics files under s3://%s/%s (task=%s)",
        len(kin),
        RAW_BUCKET,
        JIGSAWS_PREFIX,
        task_filter,
    )
    if not kin:
        raise RuntimeError(
            "no JIGSAWS kinematics files found; upload the dataset (or run rtdaas-jigsaws synth)"
        )
    return len(kin)


def _verify_gestures_indexed(**_) -> int:
    n = es_doc_count("rtdaas-gestures")
    log.info("rtdaas-gestures documents: %d", n)
    if n == 0:
        raise RuntimeError("no gesture documents indexed")
    return n


with DAG(
    dag_id="jigsaws_ingest",
    description="Ingest the JIGSAWS kinematics + video dataset into procedures.* and the gesture index.",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    params={
        "task": Param(
            None, type=["null", "string"], enum=[None, "Suturing", "Knot_Tying", "Needle_Passing"]
        )
    },
    tags=TAGS + ["jigsaws"],
    doc_md=__doc__,
) as dag:
    discover = PythonOperator(task_id="discover", python_callable=_discover)

    spark_batch_jigsaws = spark_task(
        "spark_batch_jigsaws",
        "batch_jigsaws.py",
        [
            "--root",
            f"s3a://{RAW_BUCKET}/jigsaws",
            "{% if params.task %}--task{% endif %}",
            "{% if params.task %}{{ params.task }}{% endif %}",
        ],
        executors=4,
        execution_timeout=dt.timedelta(hours=6),
    )

    quality_gate = spark_task(
        "quality_gate",
        "quality_gate.py",
        ["--suite", suite_path("procedures.yml")],
        executors=1,
        retries=0,
    )

    publish_snowflake = spark_task(
        "publish_snowflake",
        "publish_snowflake.py",
        [
            "--date",
            "{{ ds }}",
            "--marts",
            "PROCEDURES.GESTURE_SKILL_SUMMARY",
            "--target",
            "{{ var.value.get('rtdaas_publish_target', 'duckdb') }}",
        ],
        executors=1,
    )

    index_es_gestures = PythonOperator(
        task_id="index_es_gestures", python_callable=_verify_gestures_indexed
    )

    discover >> spark_batch_jigsaws >> quality_gate >> publish_snowflake >> index_es_gestures
