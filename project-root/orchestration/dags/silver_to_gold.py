from datetime import datetime, timedelta

from airflow.decorators import dag  # type: ignore[attr-defined]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from config.const import SPARK_CORES_MAX, SPARK_MAVEN_REPOSITORIES, SPARK_S3_PACKAGES


@dag(
    dag_id="silver_to_gold_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
)
def silver_to_gold_dag():
    SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/processing/tasks/silver_to_gold.py",
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


silver_to_gold_dag()
