from datetime import datetime, timedelta

from airflow.decorators import dag  # type: ignore[attr-defined]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config.const import SPARK_MAVEN_REPOSITORIES, SPARK_S3_PACKAGES


@dag(
    dag_id="clean_data_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def clean_data_dag():
    SparkSubmitOperator(
        task_id="clean_data",
        application="/opt/airflow/processing/tasks/bronze_to_silver.py",
        conn_id="my_spark_conn",
        packages=SPARK_S3_PACKAGES,
        repositories=SPARK_MAVEN_REPOSITORIES,
        deploy_mode="client",
        env_vars={"PYTHONPATH": "/opt/airflow"},
        conf={"spark.executorEnv.PYTHONPATH": "/home/iceberg"},
        verbose=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )


clean_data_dag()
