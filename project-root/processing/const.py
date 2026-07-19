"""Processing-layer constants and environment configuration."""

import os

BRONZE_BUCKET = "bronze-bucket"
SILVER_BUCKET = "silver-bucket"
GOLD_BUCKET = "gold-bucket"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# Backward-compatible aliases
bronze_bucket = BRONZE_BUCKET
silver_bucket = SILVER_BUCKET
gold_bucket = GOLD_BUCKET
