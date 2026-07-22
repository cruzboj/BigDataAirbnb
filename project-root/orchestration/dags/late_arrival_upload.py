from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config.const import SPARK_MAVEN_REPOSITORIES, SPARK_S3_PACKAGES


@dag(
    dag_id="late_arrival_upload",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
)
def upload_raw():

    SparkSubmitOperator(
        task_id="upload_adsprovider_late_arrival",
        application="/opt/airflow/processing/tasks/upload_late_arrival.py",
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


upload_raw()
