from typing import List

from minio import Minio
from minio.error import S3Error
from pyspark.sql import DataFrame, SparkSession

from processing.const import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY
from processing.storage.util import normalize_minio_endpoint


class S3StorageHandler:
    def __init__(
        self,
        spark_session: SparkSession,
    ):
        endpoint, secure = normalize_minio_endpoint(MINIO_ENDPOINT)
        self.client = Minio(endpoint, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=secure)
        self.spark = spark_session

    def create_buckets(self, buckets: List[str]):
        try:
            for bucket in buckets:
                if not self.client.bucket_exists(bucket):
                    print(f"🪣 Bucket '{bucket}' does not exist. Creating it...")
                    self.client.make_bucket(bucket)
                    print(f"✅ Bucket '{bucket}' created successfully!")
                else:
                    print(f"🪣 Bucket '{bucket}' already exists, skipping creation.")
        except S3Error as e:
            print(f"❌ Error interacting with MinIO: {e}")
            raise

    def bucket_upload(self, bucket: str, filename: str, dataframe: DataFrame):
        dataframe.write.mode("overwrite").parquet(f"s3a://{bucket}/{filename}")

    def read_file(self, file_path: str) -> DataFrame:
        return self.spark.read.parquet(file_path)
