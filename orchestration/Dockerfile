FROM apache/airflow:2.9.1

ARG AIRFLOW_VERSION=2.9.1

USER root

# install OpenJDK-17
RUN apt-get update && \
apt-get install -y openjdk-17-jdk && \
apt-get install -y ant && \
apt-get clean

# set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir \
  "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt" \
  -r /requirements.txt