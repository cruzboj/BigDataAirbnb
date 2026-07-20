from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="kafka_subscriber",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def subscriber():
    spark_packages = ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "org.apache.spark:spark-avro_2.12:3.5.0"
    ])
    
    SparkSubmitOperator(
        task_id="submit_spark_reviews_consumer",
        application="/opt/airflow/processing/tasks/spark_reviews_consumer.py",
        conn_id="my_spark_conn",
        packages=spark_packages,
        py_files="/opt/airflow/processing/session.py,/opt/airflow/processing/schema.py",
        deploy_mode="client",
        env_vars={"PYTHONPATH": "/opt/airflow:/opt/airflow/processing"},
        conf={
            "spark.executorEnv.PYTHONPATH": "/home/iceberg:/home/iceberg/processing"
        },
        verbose=True,
    )

subscriber()