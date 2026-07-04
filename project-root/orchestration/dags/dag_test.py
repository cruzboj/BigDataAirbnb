from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'airbnb_medallion_pipeline',
    default_args=default_args,
    description='End-to-end Pipeline for Airbnb Data (Bronze -> Silver)',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1), # תאריך התחלה
    catchup=False,
    tags=['airbnb', 'iceberg', 'spark'],
) as dag:

    # הגדרת המשימה להרצת הסקריפט שכתבנו בתיקיית ה-processing
    bronze_to_silver_task = SparkSubmitOperator(
        task_id='process_bronze_to_silver',
        application='/opt/airflow/processing/bronze_to_silver.py', # הנתיב בתוך הקונטיינר
        conn_id='spark_default',
        name='airbnb_bronze_to_silver_job',
        # אנחנו שולחים את המשימה לקונטיינר ה-Spark שלנו לביצוע
        conf={'spark.master': 'spark://spark-iceberg:7077'}, 
        verbose=True
    )

    # כאן יבוא בהמשך העיבוד מ-Silver ל-Gold
    # bronze_to_silver_task >> silver_to_gold_task
    
    bronze_to_silver_task