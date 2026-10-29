"""Airflow DAGs for rtdaas (spec §4.9). The DAG files live in :mod:`rtdaas.airflow.dags`.

In the Airflow image the same directory is copied to ``/opt/airflow/dags``; the DAG files
import their shared helpers from ``_common`` either as ``rtdaas.airflow.dags._common`` (package
installed) or as a sibling module (plain dags folder).
"""
