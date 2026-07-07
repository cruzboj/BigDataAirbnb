from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="master_controller_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)

def spark_dag():

    trigger_bucket = TriggerDagRunOperator(
        task_id="trigger_bucket",
        trigger_dag_id="create_bucket_dag",
        wait_for_completion=True 
    )

    trigger_upload = TriggerDagRunOperator(
        task_id="trigger_upload",
        trigger_dag_id="upload_raw_dag",
        wait_for_completion=True
    )

    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_clean",
        trigger_dag_id="clean_data_dag",
        wait_for_completion=True
    )

    trigger_bucket >> trigger_upload >> trigger_clean

spark_dag()