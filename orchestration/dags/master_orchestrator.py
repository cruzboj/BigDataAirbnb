# pyright: reportMissingImports=false
from datetime import datetime

from airflow.decorators import dag  # type: ignore[attr-defined]
from airflow.operators.trigger_dagrun import (  # type: ignore[import-not-found]
    TriggerDagRunOperator,
)


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
        wait_for_completion=True,
    )

    trigger_listing_batch = TriggerDagRunOperator(
        task_id="trigger_listing_batch",
        trigger_dag_id="listing_batch_pipeline",
        wait_for_completion=True,
    )

    trigger_silver_to_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="silver_to_gold_dag",
        wait_for_completion=True,
    )

    trigger_late_upload = TriggerDagRunOperator(
        task_id="trigger_late_upload",
        trigger_dag_id="ads_late_arrival_pipeline",
        wait_for_completion=True,
    )

    trigger_bucket >> trigger_listing_batch >> trigger_silver_to_gold
    trigger_bucket >> trigger_late_upload


spark_dag()
