FROM apache/airflow:2.9.1

USER root

#install OpenJDK-17
RUN apt-get update && \
apt-get install -y openjdk-17-jdk && \
apt-get install -y ant && \
apt-get clean;

# set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt