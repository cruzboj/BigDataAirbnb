from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    dag_id="clean_data_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def clean_data_dag():
    
    clean_data = SparkSubmitOperator(
        task_id='clean_data',
        application="/opt/airflow/processing/tasks/bronze_to_silver.py",
        conn_id="my_spark_conn",
        packages="org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        env_vars={"PYTHONPATH": "/opt/airflow"},
        verbose=True
    )

clean_data_dag()