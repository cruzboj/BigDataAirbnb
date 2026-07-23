"""Application configuration constants."""

import os
from pathlib import Path

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8082")
TOPIC_NAME = "airbnb_reviews"

DATA_DIR = Path("./data/raw")
REVIEWS_PATH = str(DATA_DIR / "reviews_copenhagen_denmark.csv")
LISTINGS_PATH = [
    str(DATA_DIR / "listings_copenhagen_denmark.csv.gz"),
    str(DATA_DIR / "listings_chicago_usa.csv.gz"),
]
LATE_ARRIVAL_PATH = "/opt/airflow/data/raw/adsProviders.csv"

# Shared Spark submit dependencies for Airflow DAGs
SPARK_S3_PACKAGES = os.getenv(
    "SPARK_S3_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
)
SPARK_MAVEN_REPOSITORIES = os.getenv(
    "SPARK_MAVEN_REPOSITORIES", "https://repo.maven.apache.org/maven2"
)
SPARK_CORES_MAX = os.getenv("SPARK_CORES_MAX", "4")

# Bronze (streamed Kafka reviews) input path and silver output key for cleaning task
STREAMING_DATA_PATH = os.getenv(
    "STREAMING_DATA_PATH", "s3a://bronze-bucket/reviews/review_*.parquet"
)
STREAMING_DATA_OUTPUT = os.getenv("STREAMING_DATA_OUTPUT", "Review.parquet")
