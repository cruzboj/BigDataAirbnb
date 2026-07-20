from types import TracebackType

from pyspark.sql import SparkSession

from processing.const import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

# Keep Hadoop AWS libs aligned with the Hadoop version bundled with Spark 3.5.x.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_BUNDLE_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.12.367"


class SparkSessionManager:
    """Context manager for creating and stopping a configured SparkSession."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark: SparkSession | None = None

    def __enter__(self) -> SparkSession:
        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .config(
                "spark.jars.packages",
                f"{HADOOP_AWS_PACKAGE},{AWS_SDK_BUNDLE_PACKAGE}",
            )
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
