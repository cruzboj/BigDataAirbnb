from typing import List
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error
from pyspark.sql import DataFrame, SparkSession

from processing.const import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def _normalize_minio_endpoint(endpoint: str) -> tuple[str, bool]:
    raw_endpoint = endpoint.strip()
    if not raw_endpoint:
        raise ValueError("MINIO_ENDPOINT cannot be empty")

    parsed = urlparse(raw_endpoint if "://" in raw_endpoint else f"//{raw_endpoint}")
    secure = parsed.scheme.lower() == "https" if parsed.scheme else False
    normalized_endpoint = parsed.netloc

    if not normalized_endpoint:
        raise ValueError(f"Invalid MINIO_ENDPOINT: '{endpoint}'")

    return normalized_endpoint.rstrip("/"), secure


class S3StorageHandler:
    def __init__(
        self,
        spark_session: SparkSession,
    ):
        endpoint, secure = _normalize_minio_endpoint(MINIO_ENDPOINT)
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
