import os
from processing.const import (
    bronze_bucket,
)
from processing.data_loader import DataLoader
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler
from processing.schema import bronze_listing_schema

with SparkSessionManager("airbnb") as spark:
    data_loader = DataLoader("airbnb", spark=spark)
    batches = data_loader.load_batch(bronze_listing_schema)
    
    storage = S3StorageHandler(spark)
    for batch in batches:
        filename = batch.select("filename").first()["filename"]
        filename = os.path.basename(filename)
        filename = filename.split('.')[0]
        storage.bucket_upload(bronze_bucket, f"listing_{filename}.parquet", batch)


    df = storage.read_files(f"s3a://{bronze_bucket}/listing_*.parquet")
    df.show(1)

