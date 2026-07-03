import os

bronze_bucket = "bronze-bucket"
silver_bucket = "silver-bucket"
gold_bucket = "gold-bucket"


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_PATH = os.getenv("MINIO_PATH", "s3a://airbnb-data/")
