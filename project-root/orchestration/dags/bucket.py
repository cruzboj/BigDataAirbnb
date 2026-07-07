from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    dag_id="create_bucket_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def create_bucket_dag():

    create_bucket = SparkSubmitOperator(
        task_id='create_bucket',
        application="/opt/airflow/processing/tasks/create_bucket.py",
        conn_id="my_spark_conn",
        packages="org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        env_vars={"PYTHONPATH": "/opt/airflow"},
        verbose=True
    )

create_bucket_dag()