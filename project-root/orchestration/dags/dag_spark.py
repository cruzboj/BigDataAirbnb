from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    schedule=None,
    catchup=False,
)
def spark_dag():

    write_data = SparkSubmitOperator(
        task_id='write_data_to_minio',
        application="/opt/airflow/processing/minio_connector.py",
        conn_id="my_spark_conn",
        packages="org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        env_vars={"PYTHONPATH": "/opt/airflow"},
        verbose=True
    )

spark_dag()