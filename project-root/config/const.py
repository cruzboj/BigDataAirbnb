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
