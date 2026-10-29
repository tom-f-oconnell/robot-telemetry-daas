"""DAG ``contracts_sync`` — register the committed Avro contracts with the schema registry.

    check_committed_contracts -> set_compatibility_and_register -> verify_registry

Run on deploy (triggered by CD, or manually) and never on a schedule: the contracts that matter
are the ones in the deployed image under ``$RTDAAS_ROOT/contracts/avro``. The check step is a
sanity gate (the real compatibility review happens in CI on the pull request, see
``.github/workflows/contracts.yml``); the register step is idempotent — re-registering an
identical schema returns the existing id.
"""

from __future__ import annotations

import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

try:
    from rtdaas.airflow.dags._common import (
        DEFAULT_ARGS,
        RTDAAS_ROOT,
        TAGS,
        notify_failure,
        rtdaas_env,
    )
except ImportError:  # plain dags folder (no rtdaas package on the path)
    from _common import DEFAULT_ARGS, RTDAAS_ROOT, TAGS, notify_failure, rtdaas_env
log = logging.getLogger(__name__)

AVRO_DIR = f"{RTDAAS_ROOT}/contracts/avro"
ENV = rtdaas_env()
REGISTRY_URL = ENV.get("SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7")
TOPICS = (
    "rtdaas.telemetry.joint_states",
    "rtdaas.telemetry.tool_pose",
    "rtdaas.telemetry.events",
    "rtdaas.telemetry.heartbeat",
)


with DAG(
    dag_id="contracts_sync",
    description="Register committed Avro contracts with the schema registry (run on deploy).",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure, "retries": 0},
    tags=TAGS + ["contracts"],
    doc_md=__doc__,
) as dag:
    check = BashOperator(
        task_id="check_committed_contracts",
        bash_command=(
            f"python -m rtdaas.contracts.cli check --base {AVRO_DIR} --head {AVRO_DIR} --mode BACKWARD"
        ),
        env=ENV,
        append_env=True,
    )

    register = BashOperator(
        task_id="set_compatibility_and_register",
        bash_command=f"python -m rtdaas.contracts.cli register --registry {REGISTRY_URL} --avro {AVRO_DIR}",
        env=ENV,
        append_env=True,
    )

    @task
    def verify_registry() -> dict:
        """Every topic subject must resolve to a schema with the expected envelope fields."""
        from rtdaas.contracts.registry import RegistryClient, subject_for_topic

        client = RegistryClient(REGISTRY_URL)
        versions: dict[str, int] = {}
        for topic in TOPICS:
            subject = subject_for_topic(topic)
            schema_id, version, schema = client.latest(subject)
            fields = {f["name"] for f in schema.get("fields", [])}
            if not {"robot_id", "ros_topic", "recv_time_ns", "msg"} <= fields:
                raise RuntimeError(f"{subject} v{version} (id {schema_id}) is missing envelope fields: {fields}")
            versions[subject] = version
        log.info("registry versions: %s", versions)
        return versions

    check >> register >> verify_registry()
