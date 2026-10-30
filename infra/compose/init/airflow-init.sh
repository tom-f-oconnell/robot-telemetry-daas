#!/usr/bin/env bash
# One-shot Airflow bootstrap for the compose stack:
#   - migrate the metadata DB
#   - create the admin user (AIRFLOW_USER / AIRFLOW_PASSWORD, default airflow/airflow)
#   - set the Variables / Connections the rtdaas DAGs expect
# Variables and connections set through AIRFLOW_VAR_* / AIRFLOW_CONN_* env vars in
# docker-compose.yml are also honoured by Airflow; the explicit `airflow variables set`
# below persists them in the DB so they show up in the UI and survive env changes.
set -euo pipefail

echo "Running airflow db migrate ..."
airflow db migrate

echo "Creating admin user ..."
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-airflow}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:-airflow}" \
  --firstname Admin --lastname User --role Admin \
  --email admin@example.com >/dev/null 2>&1 || echo "admin user already exists"

echo "Setting rtdaas variables ..."
airflow variables set rtdaas_spark_mode submit
airflow variables set rtdaas_spark_master spark://spark-master:7077
airflow variables set rtdaas_home /opt/rtdaas
airflow variables set rtdaas_quality_checks_dir /opt/rtdaas/quality/checks
airflow variables set rtdaas_publish_target duckdb

echo "Setting rtdaas connections ..."
airflow connections delete rtdaas_s3 >/dev/null 2>&1 || true
airflow connections add rtdaas_s3 \
  --conn-type aws \
  --conn-login "${S3_ACCESS_KEY:-minioadmin}" \
  --conn-password "${S3_SECRET_KEY:-minioadmin}" \
  --conn-extra "{\"endpoint_url\": \"${S3_ENDPOINT:-http://minio:9000}\", \"region_name\": \"us-east-1\"}"

# spark_default is what the DAGs' SparkSubmitOperator uses (rtdaas/airflow/dags/_common.py)
airflow connections delete spark_default >/dev/null 2>&1 || true
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client"}'

# Snowflake connection is optional; only created when credentials are supplied.
if [[ -n "${SNOWFLAKE_ACCOUNT:-}" && -n "${SNOWFLAKE_USER:-}" ]]; then
  airflow connections delete rtdaas_snowflake >/dev/null 2>&1 || true
  airflow connections add rtdaas_snowflake \
    --conn-type snowflake \
    --conn-login "${SNOWFLAKE_USER}" \
    --conn-password "${SNOWFLAKE_PASSWORD:-}" \
    --conn-extra "{\"account\": \"${SNOWFLAKE_ACCOUNT}\", \"warehouse\": \"${SNOWFLAKE_WAREHOUSE:-}\", \"database\": \"${SNOWFLAKE_DATABASE:-RTDAAS}\", \"role\": \"${SNOWFLAKE_ROLE:-}\"}"
fi

# Runs as root (compose `user: "0:0"`): hand the logs volume to the uid the other Airflow
# containers run as, last so it also covers anything created above.
if [[ $(id -u) == 0 ]]; then
  echo "Handing /opt/airflow/logs to ${RTDAAS_UID:-50000}:0 ..."
  chown -R "${RTDAAS_UID:-50000}:0" /opt/airflow/logs
fi

echo "airflow-init done."
