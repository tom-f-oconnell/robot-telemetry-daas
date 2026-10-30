#!/usr/bin/env bash
# spark-jars.sh <jars dir> — download the connector jars (Iceberg / Kafka / Avro / S3A) that rtdaas
# jobs need into a Spark distribution's jars/ directory.
#
# Used by both spark.Dockerfile (executors, and the driver for jobs submitted from that image) and
# airflow.Dockerfile (the client-mode driver for jobs Airflow submits). Baking them in means jobs
# never resolve spark.jars.packages at submit time; keep the versions in step with
# rtdaas/spark/common.py:SPARK_PACKAGES.
set -euxo pipefail

ICEBERG_VERSION="${ICEBERG_VERSION:-1.6.1}"
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-2.20.160}"
MAVEN="${MAVEN:-https://repo1.maven.org/maven2}"

cd "${1:?usage: spark-jars.sh <jars dir>}"
curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
curl -fsSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"
curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"
curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
curl -fsSLO "${MAVEN}/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar"
curl -fsSLO "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
curl -fsSLO "${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar"
curl -fsSLO "${MAVEN}/software/amazon/awssdk/url-connection-client/${AWS_SDK_BUNDLE_VERSION}/url-connection-client-${AWS_SDK_BUNDLE_VERSION}.jar"
curl -fsSLO "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
