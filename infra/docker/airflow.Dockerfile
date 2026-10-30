# Airflow image with the rtdaas package importable by DAGs and spark-submit available.
# Build context: repository root.
#   docker build -f infra/docker/airflow.Dockerfile -t ghcr.io/<owner>/rtdaas-airflow:dev .
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root
# JRE for the Spark provider's spark-submit (submit mode), procps for Airflow's `ps` usage.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# A spark-submit client matching the cluster version, used only in `submit` mode (compose).
ARG SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark
RUN set -eux; \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz; \
    mkdir -p "${SPARK_HOME}"; \
    tar -xzf /tmp/spark.tgz -C "${SPARK_HOME}" --strip-components=1; \
    rm /tmp/spark.tgz
# Jobs run their driver here (client mode), so it needs the same connector jars as the cluster.
COPY infra/docker/spark-jars.sh /tmp/spark-jars.sh
RUN SPARK_VERSION=${SPARK_VERSION} bash /tmp/spark-jars.sh "${SPARK_HOME}/jars" \
 && rm /tmp/spark-jars.sh \
 && chown -R airflow:root "${SPARK_HOME}"
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    JAVA_HOME=/usr/lib/jvm/default-java \
    RTDAAS_SPARK_PACKAGES=0

USER airflow
# Providers pinned with the official constraints file for this Airflow/Python combination.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-amazon" \
      "apache-airflow-providers-apache-spark" \
      "apache-airflow-providers-cncf-kubernetes" \
      "apache-airflow-providers-elasticsearch" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# rtdaas core (no extras) so DAG files can import rtdaas.config / rtdaas.quality etc.
COPY --chown=airflow:root pyproject.toml README.md /opt/rtdaas/
COPY --chown=airflow:root rtdaas /opt/rtdaas/rtdaas
COPY --chown=airflow:root contracts /opt/rtdaas/contracts
COPY --chown=airflow:root quality /opt/rtdaas/quality
RUN pip install --no-cache-dir --no-deps /opt/rtdaas \
 && pip install --no-cache-dir "numpy>=1.26" "pandas>=2.1" "pydantic>=2.5" "PyYAML>=6.0" "requests>=2.31" \
      "duckdb>=1.1" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY --chown=airflow:root rtdaas/airflow/dags /opt/airflow/dags
ENV RTDAAS_HOME=/opt/rtdaas
