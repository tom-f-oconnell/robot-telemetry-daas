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
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

# Connector jars (exact Maven Central URLs).
RUN set -eux; cd "${SPARK_HOME}/jars"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/software/amazon/awssdk/url-connection-client/${AWS_SDK_BUNDLE_VERSION}/url-connection-client-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"; \
    chown 185:0 ./*.jar

# Install the rtdaas package (pure-Python core + spark extra).
WORKDIR /opt/rtdaas
COPY pyproject.toml README.md ./
COPY rtdaas ./rtdaas
COPY contracts ./contracts
COPY quality ./quality
RUN pip install --no-cache-dir "/opt/rtdaas[spark]" \
 && chown -R 185:0 /opt/rtdaas

ENV RTDAAS_HOME=/opt/rtdaas \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1

USER 185
WORKDIR /opt/spark/work-dir
