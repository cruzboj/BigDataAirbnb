import logging

from config.const import LATE_ARRIVAL_PATH
from processing.const import BRONZE_BUCKET
from processing.schema import bronze_ads_schema
from processing.session import SparkSessionManager
from processing.data_loader import DataLoader
from processing.storage.s3 import S3StorageHandler
from processing.util import extract_filename

logger = logging.getLogger("airbnb.processing.tasks.upload_bronze")

def main() -> None:
    with SparkSessionManager("airbnb") as spark:
        logger.info("Starting Late Arrival streaming task...")
        data_loader = DataLoader("airbnb", spark=spark)
        ads_late_arrival_df = data_loader.load_delayed(bronze_ads_schema,LATE_ARRIVAL_PATH)
        

        storage = S3StorageHandler(spark)
        query = (
            ads_late_arrival_df.writeStream
            .foreachBatch(
                lambda batch_df, batch_id: storage.bucket_upload(
                    BRONZE_BUCKET,
                    "adsprovider.parquet",
                    batch_df,
                )
            )
            .option(
                "checkpointLocation",
                f"s3a://{BRONZE_BUCKET}/checkpoints/adsprovider",
            )
            .start()
        )

        query.awaitTermination()
        logger.info("Late Arrival upload task completed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()