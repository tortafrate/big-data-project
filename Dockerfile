FROM apache/airflow:3.1.5

USER root

ARG SPARK_VERSION=3.5.1
ARG SPARK_HADOOP_PROFILE=hadoop3

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
         curl \
         ca-certificates \
         tar \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN set -eux; \
  SPARK_TGZ="spark-${SPARK_VERSION}-bin-${SPARK_HADOOP_PROFILE}.tgz"; \
  SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"; \
  mkdir -p /opt; \
  curl -fL "${SPARK_URL}" -o "/tmp/${SPARK_TGZ}"; \
  tar -xzf "/tmp/${SPARK_TGZ}" -C /opt; \
  rm -f "/tmp/${SPARK_TGZ}"; \
  ln -sfn "/opt/spark-${SPARK_VERSION}-bin-${SPARK_HADOOP_PROFILE}" "${SPARK_HOME}"; \
  chown -R airflow:0 "/opt/spark-${SPARK_VERSION}-bin-${SPARK_HADOOP_PROFILE}" "${SPARK_HOME}"

RUN set -eux; \
  curl -fL --retry 5 --retry-delay 2 \
    -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"; \
  curl -fL --retry 5 --retry-delay 2 \
    -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar" \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"; \
  chown airflow:0 "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
