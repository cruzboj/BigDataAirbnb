from processing.const import (
    bronze_bucket,
    gold_bucket,
    silver_bucket,
)
from processing.data_loader import DataLoader
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

with SparkSessionManager("airbnb") as spark:
    data_loader = DataLoader("airbnb", spark=spark)
    batches = data_loader.load_batch()

    storage = S3StorageHandler(spark)
    storage.create_buckets([bronze_bucket, silver_bucket, gold_bucket])
    for i, batch in enumerate(batches):
        storage.bucket_upload(bronze_bucket, f"test_file-{i}.parquet", batch)

    df = storage.read_file(f"s3a://{bronze_bucket}/test_file-0.parquet")
    df.show(truncate=False)

if __name__ == "__main__":
    initialize_buckets()