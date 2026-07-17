from processing.const import (
    bronze_bucket,
    gold_bucket,
    silver_bucket,
)
from processing.data_loader import DataLoader
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler
from processing.schema import SILVER_SCHEMAS 
from pyspark.sql.functions import col

def process_silver_tables(bronze_df, silver_schemas, storage_handler, silver_bucket):
    """
    """

    column_mapping = {
        "listing_id": "id",
        "host_profile_id": "host_id",
        "host_profile_url": "host_url",
        "price_per_night": "price"
    }

    natural_keys = {
        "HostDetails": ["host_id"],
        "HostMetrics": ["id"],
        "RentDetails": ["id"],
        "RentMetrics": ["id"],
        "Location": ["id"],
        "Review": ["id"],
        "Metadata": ["id"]
    }

    for table_name, schema in silver_schemas.items():
        print(f"Loading data using schema {table_name}")
        
        cast_exprs = []
        for field in schema:
            target_col_name = field.name
            source_col_name = column_mapping.get(target_col_name, target_col_name)
            expr = col(source_col_name).cast(field.dataType).alias(target_col_name)
            cast_exprs.append(expr)
        
        silver_df = bronze_df.select(*cast_exprs)
        
        if table_name in natural_keys:
            key_columns = natural_keys[table_name]
            silver_df = silver_df.dropDuplicates(key_columns)
        
        output_path = f"{table_name}.parquet"
        storage_handler.bucket_upload(silver_bucket, output_path, silver_df)
        print(f"Successfully uploaded {table_name} to s3a://{silver_bucket}/{output_path}")

with SparkSessionManager("airbnb") as spark:
    storage = S3StorageHandler(spark)

    df = storage.read_files(f"s3a://{bronze_bucket}/listing_*.parquet")

    process_silver_tables(df, SILVER_SCHEMAS, storage, silver_bucket)

