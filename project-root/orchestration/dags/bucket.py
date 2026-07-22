from datetime import datetime, timedelta

from airflow.decorators import dag  # type: ignore[attr-defined]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config.const import SPARK_CORES_MAX, SPARK_MAVEN_REPOSITORIES, SPARK_S3_PACKAGES


@dag(
    dag_id="create_bucket_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def create_bucket_dag():
    SparkSubmitOperator(
        task_id="create_bucket",
        application="/opt/airflow/processing/tasks/create_bucket.py",
        conn_id="my_spark_conn",
        packages=SPARK_S3_PACKAGES,
        repositories=SPARK_MAVEN_REPOSITORIES,
        deploy_mode="client",
        env_vars={"PYTHONPATH": "/opt/airflow"},
        conf={
            "spark.executorEnv.PYTHONPATH": "/home/iceberg",
            "spark.cores.max": SPARK_CORES_MAX,
        },
        verbose=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )


create_bucket_dag()
