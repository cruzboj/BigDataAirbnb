from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    regexp_extract,
)

from config.const import LISTINGS_PATH
from processing.const import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY
from processing.schema import bronze_listing_schema, format_schema, parse_array_columns


class DataLoader:
    def __init__(self, app_name):
        self.app_name = app_name

    def create_spark_session(self):
        """
        Creates a SparkSession with the necessary S3A dependencies for MinIO.
        """

        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
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

    def load_batch(self):
        schema = format_schema(bronze_listing_schema)

        data = []
        for path in LISTINGS_PATH:
            df = (
                self.spark.read.schema(schema)
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
                .csv(path)
                .withColumn(
                    "city",
                    regexp_extract(input_file_name(), r"listings_([a-zA-Z]+)", 1),
                )
            )

            df = parse_array_columns(df, bronze_listing_schema)

            df = df.withColumn("filename", input_file_name())
            df = df.withColumn("ingestion_ts", current_timestamp())
            data.append(df)

        return data
