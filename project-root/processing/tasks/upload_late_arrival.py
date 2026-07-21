import logging
from pathlib import Path

from pyspark.sql.functions import col, current_timestamp, expr

from config.const import LATE_ARRIVAL_PATH
from processing.const import BRONZE_BUCKET
from processing.data_loader import DataLoader
from processing.schema import bronze_ads_schema
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.upload_late_arrival")


def _has_late_files(path: str) -> bool:
    base_path = Path(path)

    if not base_path.exists():
        logger.warning("Late-arrival path does not exist: %s", path)
        return False

    if base_path.is_file():
        return True

    return any(p.is_file() for p in base_path.rglob("*"))


def main() -> None:
    with SparkSessionManager("airbnb") as spark:
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Starting late-arrival batch upload task...")

        if not _has_late_files(LATE_ARRIVAL_PATH):
            logger.info(
                "No late-arrival files found in %s. Nothing to upload.",
                LATE_ARRIVAL_PATH,
            )
            return

        data_loader = DataLoader("airbnb", spark=spark)
        late_batches = data_loader.load_batch(bronze_ads_schema, LATE_ARRIVAL_PATH)

        if not late_batches:
            logger.info("Late-arrival loader returned no batches. Nothing to upload.")
            return

        filtered_df = late_batches[0].filter(
            col("last_updated").isNotNull()
            & (col("last_updated") >= current_timestamp() - expr("INTERVAL 48 HOURS"))
            & (col("last_updated") <= current_timestamp() + expr("INTERVAL 48 HOURS"))
        )

        storage = S3StorageHandler(spark)
        storage.bucket_upload(BRONZE_BUCKET, "adsprovider.parquet", filtered_df)

        logger.info("Late-arrival batch upload task completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
