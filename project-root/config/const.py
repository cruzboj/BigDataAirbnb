import os

# TODO: change the localhost into a container location , "kafka:29092" "http://schema-registry:8082"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8082")
REVIEWS_PATH = "./data/raw/reviews.csv"
TOPIC_NAME = "airbnb_reviews"

LISTINGS_PATH = [
    "./data/raw/listings_copenhagen_denmark.csv.gz",
    "./data/raw/listings_chicago_usa.csv.gz",
]
