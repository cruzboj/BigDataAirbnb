import logging

from minio import Minio
from minio.error import S3Error
from pyspark.sql import DataFrame, SparkSession

from processing.const import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY
from processing.storage.util import normalize_minio_endpoint

logger = logging.getLogger("airbnb.processing.storage.s3")


class S3StorageHandler:
    def __init__(self, spark_session: SparkSession):
        endpoint, secure = normalize_minio_endpoint(MINIO_ENDPOINT)
        self.client = Minio(endpoint, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=secure)
        self.spark = spark_session

    def create_buckets(self, buckets: list[str]) -> None:
        try:
            for bucket in buckets:
                if not self.client.bucket_exists(bucket):
                    logger.info("Bucket '%s' does not exist. Creating it...", bucket)
                    self.client.make_bucket(bucket)
                    logger.info("Bucket '%s' created successfully.", bucket)
                else:
                    logger.info(
                        "Bucket '%s' already exists; skipping creation.", bucket
                    )
        except S3Error:
            logger.exception("Error interacting with MinIO")
            raise

    def bucket_upload(
        self,
        bucket: str,
        filename: str,
        dataframe: DataFrame,
        mode: str = "overwrite",
    ) -> None:
        dataframe.write.mode(mode).parquet(f"s3a://{bucket}/{filename}")

    def read_files(self, file_path: str) -> DataFrame:
        return self.spark.read.parquet(file_path)
