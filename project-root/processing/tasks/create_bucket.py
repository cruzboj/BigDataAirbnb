from processing.const import (
    bronze_bucket,
    gold_bucket,
    silver_bucket,
)

from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

with SparkSessionManager("airbnb") as spark:
    storage = S3StorageHandler(spark)
    storage.create_buckets([bronze_bucket, silver_bucket, gold_bucket])

