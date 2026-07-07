# from processing.const import (
#     bronze_bucket,
#     gold_bucket,
#     silver_bucket,
# )

# from processing.session import SparkSessionManager
# from processing.storage.s3 import S3StorageHandler

# with SparkSessionManager() as spark_session:
#     storage = S3StorageHandler(spark_session)
#     df = storage.read_file(f"s3a://{bronze_bucket}/airbnb_bronze.parquet")

print("hello from bronze_to_silver.py")