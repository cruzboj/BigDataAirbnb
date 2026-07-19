import logging

from config.const import LISTINGS_PATH, REVIEWS_PATH
from processing.const import BRONZE_BUCKET
from processing.data_loader import DataLoader
from processing.schema import bronze_listing_schema, bronze_reviews_schema
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler
from processing.util import extract_filename

logger = logging.getLogger("airbnb.processing.tasks.upload_bronze")


def main() -> None:
    with SparkSessionManager("airbnb") as spark:
        data_loader = DataLoader("airbnb", spark=spark)

        listing_batches = data_loader.load_batch(bronze_listing_schema, LISTINGS_PATH)
        review_batches = data_loader.load_batch(bronze_reviews_schema, REVIEWS_PATH)

        storage = S3StorageHandler(spark)

        for batch in listing_batches:
            filename = extract_filename(batch)
            storage.bucket_upload(BRONZE_BUCKET, f"listing_{filename}.parquet", batch)

        for batch in review_batches:
            filename = extract_filename(batch)
            storage.bucket_upload(BRONZE_BUCKET, f"review_{filename}.parquet", batch)

        listing_df = storage.read_files(f"s3a://{BRONZE_BUCKET}/listing_*.parquet")
        listing_df.show(1)

        review_df = storage.read_files(f"s3a://{BRONZE_BUCKET}/review_*.parquet")
        review_df.show(1)

        logger.info("Bronze upload task completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
