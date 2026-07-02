import os

# Kafka configuration
#TODO: change the localhost into a container location , "kafka:29092" "http://schema-registry:8082"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8082")
REVIEWS_PATH = "./data/raw/reviews.csv"
TOPIC_NAME = "airbnb_reviews"

# Spark configuration
LISTINGS_PATH = [
    "./data/raw/listings_denmark.csv.gz",
    "./data/raw/listings_chicago.csv.gz",
]

MINIO_PATH = os.getenv("MINIO_PATH", "s3a://airbnb-data/")