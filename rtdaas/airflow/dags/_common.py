"""Shared helpers for the rtdaas DAGs: default args, Spark task factory, notifications.

Spark tasks run in one of two modes, selected by the Airflow Variable
``rtdaas_spark_mode`` (or env ``RTDAAS_SPARK_MODE``):

* ``submit`` — ``SparkSubmitOperator`` against ``spark_default`` (docker compose: the
  standalone master at ``spark://spark-master:7077``). The job file is the path inside the
  Airflow image (``/opt/rtdaas/rtdaas/spark/<job>.py``).
* ``k8s`` — ``SparkKubernetesOperator`` submitting a ``SparkApplication`` CR built by
  :func:`spark_app_template` to the spark-operator in the ``rtdaas`` namespace, using the
  ``rtdaas-spark`` image and the same job file baked into it.

Runtime configuration for the jobs (spec §9 variables) comes from the Variable
``rtdaas_env`` (JSON object) merged over the scheduler's own ``RTDAAS_``/platform env.
"""

from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import os
from typing import Any

from airflow.models import Variable

log = logging.getLogger(__name__)

RTDAAS_ROOT = os.environ.get("RTDAAS_ROOT", "/opt/rtdaas")
SPARK_JOBS_DIR = f"{RTDAAS_ROOT}/rtdaas/spark"
SUITES_DIR = f"{RTDAAS_ROOT}/quality/checks"
SPARK_IMAGE = os.environ.get("RTDAAS_SPARK_IMAGE", "ghcr.io/tom-f-oconnell/rtdaas-spark:latest")
SPARK_NAMESPACE = os.environ.get("RTDAAS_SPARK_NAMESPACE", "rtdaas")
SPARK_SERVICE_ACCOUNT = os.environ.get("RTDAAS_SPARK_SERVICE_ACCOUNT", "rtdaas-spark")
S3_CONN_ID = "rtdaas_s3"
SPARK_CONN_ID = "spark_default"
K8S_CONN_ID = "kubernetes_default"

# Env keys forwarded from the scheduler process to spark-submit / the SparkApplication.
_FORWARDED_ENV = (
    "KAFKA_BOOTSTRAP",
    "SCHEMA_REGISTRY_URL",
    "ES_URL",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_BUCKET_RAW",
    "S3_BUCKET_LAKE",
    "ICEBERG_REST_URI",
    "ICEBERG_WAREHOUSE",
    "SERVING_DUCKDB_PATH",
    "PIPELINE_VERSION",
    "LOG_LEVEL",
    "RTDAAS_SPARK_PACKAGES",
)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

TAGS = ["rtdaas"]


def spark_mode() -> str:
    mode = os.environ.get("RTDAAS_SPARK_MODE") or Variable.get(
        "rtdaas_spark_mode", default_var="submit"
    )
    return mode if mode in ("submit", "k8s") else "submit"


def rtdaas_env() -> dict[str, str]:
    """Job environment: forwarded scheduler env, overridden by the ``rtdaas_env`` Variable (JSON)."""
    env = {k: os.environ[k] for k in _FORWARDED_ENV if k in os.environ}
    try:
        override = Variable.get("rtdaas_env", default_var="{}", deserialize_json=True)
    except Exception:  # malformed variable must not break DAG parsing
        override = {}
    if isinstance(override, str):
        try:
            override = json.loads(override)
        except json.JSONDecodeError:
            override = {}
    env.update({str(k): str(v) for k, v in (override or {}).items()})
    return env


def job_path(job_file: str) -> str:
    return f"{SPARK_JOBS_DIR}/{job_file}"


def suite_path(name: str) -> str:
    return f"{SUITES_DIR}/{name}"


def spark_app_template(
    name: str,
    app_file: str,
    args: list[str],
    image: str | None = None,
    env: dict[str, str] | None = None,
    executors: int = 2,
) -> dict:
    """A ``sparkoperator.k8s.io/v1beta2`` SparkApplication manifest (dict) for one job run."""
    env_list = [{"name": k, "value": v} for k, v in sorted((env or rtdaas_env()).items())]
    env_from = [
        {"configMapRef": {"name": "rtdaas-config"}},
        {"configMapRef": {"name": "rtdaas-env-overlay", "optional": True}},
        {"secretRef": {"name": "rtdaas-secrets"}},
    ]
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name[:63].rstrip("-"),
            "namespace": SPARK_NAMESPACE,
            "labels": {"app.kubernetes.io/part-of": "rtdaas"},
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": image or SPARK_IMAGE,
            "imagePullPolicy": "Always",  # SPARK_IMAGE is a mutable tag
            "mainApplicationFile": f"local://{job_path(app_file)}",
            "arguments": list(args),
            "sparkVersion": "3.5.3",
            "restartPolicy": {"type": "Never"},
            "sparkConf": {"spark.kubernetes.file.upload.path": "s3a://rtdaas-lake/spark-upload"},
            # Sized to fit beside streaming on the 4-VM cluster (Spark adds 40 % memory overhead).
            "driver": {
                "cores": 1,
                "coreRequest": "250m",
                "memory": "1g",
                "serviceAccount": SPARK_SERVICE_ACCOUNT,
                "env": env_list,
                "envFrom": env_from,
                "labels": {"app.kubernetes.io/part-of": "rtdaas"},
            },
            "executor": {
                "cores": 1,
                "coreRequest": "500m",
                "instances": executors,
                "memory": "2g",
                "env": env_list,
                "envFrom": env_from,
                "labels": {"app.kubernetes.io/part-of": "rtdaas"},
            },
        },
    }


def _k8s_name(task_id: str) -> str:
    return "rtdaas-" + task_id.replace("_", "-") + "-{{ ts_nodash | lower }}"


@functools.cache
def _spark_k8s_operator():
    from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
        SparkKubernetesOperator,
    )

    class RtdaasSparkKubernetesOperator(SparkKubernetesOperator):
        # Otherwise an argument ending in .yml (--suite <path>) is loaded as a Jinja template file:
        # TemplateNotFound. The specs are dicts (template_spec), never files.
        template_ext = ()

    return RtdaasSparkKubernetesOperator


def spark_task(task_id: str, app_file: str, args: list[str], executors: int = 2, **kwargs):
    """Operator running ``rtdaas/spark/<app_file>`` with ``args`` in the configured mode."""
    if spark_mode() == "k8s":
        return _spark_k8s_operator()(
            task_id=task_id,
            namespace=SPARK_NAMESPACE,
            kubernetes_conn_id=K8S_CONN_ID,
            template_spec=spark_app_template(
                _k8s_name(task_id), app_file, args, executors=executors
            ),
            get_logs=True,
            delete_on_termination=True,
            **kwargs,
        )

    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=job_path(app_file),
        application_args=list(args),
        env_vars=rtdaas_env(),
        name=f"rtdaas-{task_id}",
        verbose=False,
        **kwargs,
    )


def spark_task_mapped(task_id: str, app_file: str, arg_lists, executors: int = 2, **kwargs):
    """Dynamically mapped variant: one task instance per element of ``arg_lists`` (XCom)."""
    if spark_mode() == "k8s":
        from airflow.decorators import task

        @task(task_id=f"{task_id}_specs")
        def specs(lists: list[list[str]]) -> list[dict]:
            return [
                spark_app_template(
                    f"rtdaas-{task_id}-{i}".replace("_", "-"), app_file, a, executors=executors
                )
                for i, a in enumerate(lists)
            ]

        return (
            _spark_k8s_operator()
            .partial(
                task_id=task_id,
                namespace=SPARK_NAMESPACE,
                kubernetes_conn_id=K8S_CONN_ID,
                get_logs=True,
                delete_on_termination=True,
                **kwargs,
            )
            .expand(template_spec=specs(arg_lists))
        )

    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    return SparkSubmitOperator.partial(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=job_path(app_file),
        env_vars=rtdaas_env(),
        name=f"rtdaas-{task_id}",
        verbose=False,
        **kwargs,
    ).expand(application_args=arg_lists)


def notify_failure(context: dict) -> None:
    """``on_failure_callback``: post to a Slack incoming webhook if configured, else log."""
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "?"
    msg = (
        f":rotating_light: rtdaas DAG `{dag_id}` task `{getattr(ti, 'task_id', '?')}` failed "
        f"(run {context.get('run_id')}). Log: {getattr(ti, 'log_url', 'n/a')}"
    )
    webhook = os.environ.get("RTDAAS_SLACK_WEBHOOK") or Variable.get(
        "rtdaas_slack_webhook", default_var=""
    )
    if webhook:
        try:
            import requests

            requests.post(webhook, json={"text": msg}, timeout=10)
            return
        except Exception as exc:  # never raise from a callback
            log.warning("slack notification failed: %s", exc)
    log.error(msg)


def es_doc_count(index: str, query: dict | None = None) -> int:
    """Document count in an Elasticsearch index (used by post-publish verification tasks)."""
    import requests

    es_url = rtdaas_env().get("ES_URL", "http://elasticsearch:9200").rstrip("/")
    r = requests.post(
        f"{es_url}/{index}/_count", json={"query": query or {"match_all": {}}}, timeout=30
    )
    r.raise_for_status()
    return int(r.json().get("count", 0))


def day_range(start: str, end: str) -> list[str]:
    """Inclusive list of ISO dates from ``start`` to ``end``."""
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    if e < s:
        s, e = e, s
    return [(s + dt.timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]
