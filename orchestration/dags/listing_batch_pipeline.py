from datetime import datetime, timedelta

from airflow.decorators import dag  # type: ignore[attr-defined]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config.const import SPARK_CORES_MAX, SPARK_MAVEN_REPOSITORIES, SPARK_S3_PACKAGES


@dag(
    dag_id="listing_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def listing_batch_pipeline() -> None:
    upload_listings_to_bronze = SparkSubmitOperator(
        task_id="upload_listings_to_bronze",
        application="/opt/airflow/processing/tasks/upload_listings_to_bronze.py",
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

    clean_listings_to_silver = SparkSubmitOperator(
        task_id="clean_listings_to_silver",
        application="/opt/airflow/processing/tasks/clean_listings_to_silver.py",
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

    upload_listings_to_bronze >> clean_listings_to_silver


listing_batch_pipeline()
