import logging

from processing.const import BRONZE_BUCKET, GOLD_BUCKET, SILVER_BUCKET
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.create_bucket")


def main() -> None:
    with SparkSessionManager("airbnb") as spark:
        storage = S3StorageHandler(spark)
        storage.create_buckets([BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET])
        logger.info("Bucket creation task completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
