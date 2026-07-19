import logging
from collections.abc import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lead, lit
from pyspark.sql.types import StructType

from processing.cleanup.listing import ListingProcessor
from processing.const import BRONZE_BUCKET, SILVER_BUCKET
from processing.schema import SILVER_SCHEMAS, silver_review_schema
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler

logger = logging.getLogger("airbnb.processing.tasks.bronze_to_silver")


def process_silver_tables(
    bronze_df: DataFrame,
    silver_schemas: Mapping[str, StructType],
    storage_handler: S3StorageHandler,
    silver_bucket: str,
    column_mapping: Mapping[str, str] | None = None,
    natural_keys_override: Mapping[str, list[str]] | None = None,
) -> None:
    """Cast cleaned bronze data into each silver schema and upload parquet files."""
    merged_column_mapping = {
        "listing_id": "id",
        "host_profile_id": "host_id",
        "host_profile_url": "host_url",
        "price_per_night": "price",
    }
    if column_mapping:
        merged_column_mapping.update(column_mapping)

    natural_keys = {
        "HostDetails": ["host_id"],
        "HostMetrics": ["id"],
        "RentDetails": ["id"],
        "RentMetrics": ["id"],
        "Location": ["id"],
        "Review": ["id"],
        "Metadata": ["id"],
    }
    if natural_keys_override:
        natural_keys.update(natural_keys_override)

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
                source_col_name = merged_column_mapping.get(
                    target_col_name, target_col_name
                )
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


def _ensure_schema_source_columns(
    df: DataFrame,
    schema: StructType,
    column_mapping: Mapping[str, str],
) -> DataFrame:
    """Ensure all source columns required by schema + mapping exist on DataFrame."""
    result_df = df
    for field in schema:
        source_col_name = column_mapping.get(field.name, field.name)
        if source_col_name not in result_df.columns:
            result_df = result_df.withColumn(
                source_col_name,
                lit(None).cast(field.dataType),
            )
    return result_df


def process_review_table(
    listing_df: DataFrame,
    review_df: DataFrame,
    storage_handler: S3StorageHandler,
    silver_bucket: str,
) -> None:
    """Build and upload Review silver table from review bronze data using SCD Type 2."""
    try:
        logger.info("Preparing Review silver table from review bronze data")
        logger.info("Review bronze columns: %s", review_df.columns)

        review_input_count = review_df.count()
        logger.info("Review bronze row count: %d", review_input_count)

        review_scd_window = Window.partitionBy("id").orderBy(col("date").asc())

        review_scd_df = (
            review_df.withColumn("start_date", col("date"))
            .withColumn("end_date", lead("date").over(review_scd_window))
            .withColumn("is_current", col("end_date").isNull())
        )
        logger.info(
            "Applied SCD Type 2 columns for Review (start_date, end_date, is_current)"
        )

        listing_review_fields = [
            "review_scores_rating",
            "review_scores_accuracy",
            "review_scores_cleanliness",
            "number_of_reviews_ltm",
        ]
        listing_lookup_df = listing_df.select(
            col("id").alias("listing_id"),
            *[col(field_name) for field_name in listing_review_fields],
        ).dropDuplicates(["listing_id"])
        logger.info(
            "Joining review bronze with listing fields needed by gold model: %s",
            listing_review_fields,
        )
        review_scd_df = review_scd_df.join(
            listing_lookup_df, on="listing_id", how="left"
        )

        review_column_mapping = {
            "id": "listing_id",
            "comment": "comments",
            "likes_count": "likes_votes",
        }
        logger.info("Review column mapping: %s", review_column_mapping)

        review_source_df = _ensure_schema_source_columns(
            review_scd_df,
            silver_review_schema,
            review_column_mapping,
        )
        logger.info("Ensured Review source columns required by schema are present")

        preview_cast_exprs = []
        for field in silver_review_schema:
            source_col_name = review_column_mapping.get(field.name, field.name)
            preview_cast_exprs.append(
                col(source_col_name).cast(field.dataType).alias(field.name)
            )
        review_preview_df = review_source_df.select(*preview_cast_exprs)
        logger.info("Preview of Review silver dataframe (5 rows):")
        review_preview_df.show(5, truncate=False)

        process_silver_tables(
            review_source_df,
            {"Review": silver_review_schema},
            storage_handler,
            silver_bucket,
            column_mapping=review_column_mapping,
            natural_keys_override={
                "Review": ["id", "reviewer_id", "start_date", "comment"]
            },
        )
        logger.info(
            "Finished Review silver table processing for bucket %s", silver_bucket
        )
    except Exception as exc:  # noqa: BLE001 - task should continue for other silver outputs
        logger.warning(
            "[SCHEMA_SKIP] Failed to prepare Review source dataframe. Skipping upload. Error: %s",
            exc,
        )


def main() -> None:
    with SparkSessionManager("airbnb") as spark:
        storage = S3StorageHandler(spark)

        listing_bronze_df = storage.read_files(
            f"s3a://{BRONZE_BUCKET}/listing_*.parquet"
        )
        review_bronze_df = storage.read_files(f"s3a://{BRONZE_BUCKET}/review_*.parquet")

        listing_processor = ListingProcessor(listing_bronze_df)
        cleaned_listing_df = listing_processor.process()
        listing_processor.validate_cleaned_data()

        non_review_schemas = {
            name: schema for name, schema in SILVER_SCHEMAS.items() if name != "Review"
        }
        process_silver_tables(
            cleaned_listing_df,
            non_review_schemas,
            storage,
            SILVER_BUCKET,
        )

        process_review_table(
            cleaned_listing_df,
            review_bronze_df,
            storage,
            SILVER_BUCKET,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
