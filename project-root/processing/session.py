from types import TracebackType

from pyspark.sql import SparkSession

from processing.const import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    SPARK_CORES_MAX,
    SPARK_MAVEN_REPOSITORIES,
    SPARK_S3_PACKAGES,
)


class SparkSessionManager:
    """Context manager for creating and stopping a configured SparkSession."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark: SparkSession | None = None

    def __enter__(self) -> SparkSession:
        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars.packages", SPARK_S3_PACKAGES)
            .config("spark.jars.repositories", SPARK_MAVEN_REPOSITORIES)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.cores.max", SPARK_CORES_MAX)
            .getOrCreate()
        )
        return self.spark

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
