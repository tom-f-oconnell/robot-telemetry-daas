# Spark image for rtdaas batch + streaming jobs.
# Build context: repository root.
#   docker build -f infra/docker/spark.Dockerfile -t ghcr.io/<owner>/rtdaas-spark:dev .
#
# Jars for Iceberg / Kafka / Avro / S3A are baked into $SPARK_HOME/jars so that
# jobs never need network access at submit time (spark.jars.packages is NOT used).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.6.1
ARG SPARK_VERSION=3.5.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=2.20.160
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# libgl1 / libglib2.0-0 are required by opencv-python-headless at import time on some
# distros; curl for jar downloads; procps for Spark's scripts.
# The base image's Python is jammy's 3.10, but rtdaas needs >= 3.11, and PySpark needs the
# executors' minor version to match the driver's (Airflow submits in client mode from
# python:3.11), so add 3.11 from deadsnakes and run everything from a venv on it.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 curl ca-certificates procps \
      gnupg software-properties-common \
 && add-apt-repository -y ppa:deadsnakes/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends python3.11 python3.11-venv \
 && python3.11 -m venv /opt/venv \
 && rm -rf /var/lib/apt/lists/*
ENV PATH=/opt/venv/bin:$PATH

# Connector jars (exact Maven Central URLs); shared with airflow.Dockerfile.
COPY infra/docker/spark-jars.sh /tmp/spark-jars.sh
RUN ICEBERG_VERSION=${ICEBERG_VERSION} SPARK_VERSION=${SPARK_VERSION} \
    HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION} AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION} \
    MAVEN=${MAVEN} bash /tmp/spark-jars.sh "${SPARK_HOME}/jars" \
 && chown 185:0 "${SPARK_HOME}"/jars/*.jar && rm /tmp/spark-jars.sh

# Install the rtdaas package (pure-Python core + spark extra).
WORKDIR /opt/rtdaas
COPY pyproject.toml README.md ./
COPY rtdaas ./rtdaas
COPY contracts ./contracts
COPY quality ./quality
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir "/opt/rtdaas[spark]" \
 && python -c "import rtdaas.spark.common" \
 && chown -R 185:0 /opt/rtdaas

ENV RTDAAS_HOME=/opt/rtdaas \
    RTDAAS_SPARK_PACKAGES=0 \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONUNBUFFERED=1

USER 185
WORKDIR /opt/spark/work-dir
