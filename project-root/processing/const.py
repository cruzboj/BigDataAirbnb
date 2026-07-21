"""Processing-layer constants and environment configuration."""

import os

BRONZE_BUCKET = "bronze-bucket"
SILVER_BUCKET = "silver-bucket"
GOLD_BUCKET = "gold-bucket"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# Spark packages required for s3a:// support (MinIO/S3 via Hadoop AWS)
SPARK_S3_PACKAGES = os.getenv(
    "SPARK_S3_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
)
SPARK_MAVEN_REPOSITORIES = os.getenv(
    "SPARK_MAVEN_REPOSITORIES", "https://repo.maven.apache.org/maven2"
)

# Backward-compatible aliases
bronze_bucket = BRONZE_BUCKET
silver_bucket = SILVER_BUCKET
gold_bucket = GOLD_BUCKET
