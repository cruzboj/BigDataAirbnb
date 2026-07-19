import logging
from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from processing.cleanup.listing import ListingProcessor
from processing.const import BRONZE_BUCKET, SILVER_BUCKET
from processing.schema import SILVER_SCHEMAS
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.bronze_to_silver")


def process_silver_tables(
    bronze_df: DataFrame,
    silver_schemas: Mapping[str, StructType],
    storage_handler: S3StorageHandler,
    silver_bucket: str,
) -> None:
    """Cast cleaned bronze data into each silver schema and upload parquet files."""
    column_mapping = {
        "listing_id": "id",
        "host_profile_id": "host_id",
        "host_profile_url": "host_url",
        "price_per_night": "price",
    }

    natural_keys = {
        "HostDetails": ["host_id"],
        "HostMetrics": ["id"],
        "RentDetails": ["id"],
        "RentMetrics": ["id"],
        "Location": ["id"],
        "Review": ["id"],
        "Metadata": ["id"],
    }

    available_columns = set(bronze_df.columns)
    succeeded: list[str] = []
    failed: list[str] = []

    for table_name, schema in silver_schemas.items():
        logger.info("Loading data using schema %s", table_name)

        try:
            source_columns: list[str] = []
            cast_exprs = []
            for field in schema:
                target_col_name = field.name
                source_col_name = column_mapping.get(target_col_name, target_col_name)
                source_columns.append(source_col_name)
                cast_exprs.append(
                    col(source_col_name).cast(field.dataType).alias(target_col_name)
                )

            missing_columns = sorted(
                {
                    source_col
                    for source_col in source_columns
                    if source_col not in available_columns
                }
            )
            if missing_columns:
                raise ValueError(
                    f"Missing source columns: {', '.join(missing_columns)}"
                )

            silver_df = bronze_df.select(*cast_exprs)

            if table_name in natural_keys:
                silver_df = silver_df.dropDuplicates(natural_keys[table_name])

            output_path = f"{table_name}.parquet"
            storage_handler.bucket_upload(silver_bucket, output_path, silver_df)
            logger.info(
                "Successfully uploaded %s to s3a://%s/%s",
                table_name,
                silver_bucket,
                output_path,
            )
            succeeded.append(table_name)

        except Exception as exc:  # noqa: BLE001 - task should continue per schema
            logger.warning(
                "[SCHEMA_SKIP] Failed to apply schema '%s'. Skipping upload. Error: %s",
                table_name,
                exc,
            )
            failed.append(table_name)

    logger.info("=== Silver schema processing summary ===")
    logger.info("Succeeded schemas (%d): %s", len(succeeded), succeeded)
    logger.info("Failed schemas (%d): %s", len(failed), failed)


def main() -> None:
    with SparkSessionManager("airbnb") as spark:
        storage = S3StorageHandler(spark)

        bronze_df = storage.read_files(f"s3a://{BRONZE_BUCKET}/listing_*.parquet")
        listing_processor = ListingProcessor(bronze_df)
        cleaned_df = listing_processor.process()
        listing_processor.validate_cleaned_data()

        process_silver_tables(cleaned_df, SILVER_SCHEMAS, storage, SILVER_BUCKET)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
