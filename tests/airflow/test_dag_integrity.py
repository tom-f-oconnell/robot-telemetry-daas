"""DAG integrity (run in CI's `airflow` job with apache-airflow installed)."""

from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
pytestmark = pytest.mark.airflow

DAG_DIR = Path(__file__).resolve().parents[2] / "rtdaas" / "airflow" / "dags"
EXPECTED = {
    "telemetry_bag_ingest": {
        "wait_for_bags",
        "spark_batch_mcap",
        "quality_gate",
        "publish_marts",
        "index_es_sessions",
    },
    "telemetry_backfill": {"publish_marts"},
    "jigsaws_ingest": {"spark_batch_jigsaws", "quality_gate", "publish_marts"},
    "dq_daily": {"quality_gate", "dq_summary", "iceberg_maintenance"},
    "contracts_sync": {
        "check_committed_contracts",
        "set_compatibility_and_register",
        "verify_registry",
    },
}


@pytest.fixture(scope="module")
def dagbag(monkeypatch_module=None):
    import os

    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == set(EXPECTED)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_dag_shape(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert "rtdaas" in dag.tags
    assert EXPECTED[dag_id] <= set(dag.task_ids), (dag_id, dag.task_ids)
    assert dag.default_args.get("retries") is not None
    assert dag.max_active_runs == 1
    assert not dag.catchup


def test_bag_ingest_sensor_pattern(dagbag):
    sensor = dagbag.get_dag("telemetry_bag_ingest").get_task("wait_for_bags")
    assert sensor.bucket_key == "bags/*/{{ ds[:4] }}/{{ ds[5:7] }}/{{ ds[8:10] }}/*.mcap"
    assert sensor.mode == "reschedule"
