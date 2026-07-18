from pyspark.sql.functions import col

from processing.cleanup.listing import ListingProcessor
from processing.const import bronze_bucket, silver_bucket
from processing.schema import SILVER_SCHEMAS
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler


def process_silver_tables(bronze_df, silver_schemas, storage_handler, silver_bucket):
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
    succeeded = []
    failed = []

    for table_name, schema in silver_schemas.items():
        print(f"Loading data using schema {table_name}")

        try:
            source_columns = []
            cast_exprs = []
            for field in schema:
                target_col_name = field.name
                source_col_name = column_mapping.get(target_col_name, target_col_name)
                source_columns.append(source_col_name)
                expr = col(source_col_name).cast(field.dataType).alias(target_col_name)
                cast_exprs.append(expr)

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
                key_columns = natural_keys[table_name]
                silver_df = silver_df.dropDuplicates(key_columns)

            output_path = f"{table_name}.parquet"
            storage_handler.bucket_upload(silver_bucket, output_path, silver_df)
            print(
                f"Successfully uploaded {table_name} to s3a://{silver_bucket}/{output_path}"
            )
            succeeded.append(table_name)

        except Exception as exc:
            print(
                f"[SCHEMA_SKIP] Failed to apply schema '{table_name}'. "
                f"Skipping upload for this table. Error: {exc}"
            )
            failed.append(table_name)

    print("\n=== Silver schema processing summary ===")
    print(f"Succeeded schemas ({len(succeeded)}): {succeeded}")
    print(f"Failed schemas ({len(failed)}): {failed}")


with SparkSessionManager("airbnb") as spark:
    storage = S3StorageHandler(spark)

    bronze_df = storage.read_files(f"s3a://{bronze_bucket}/listing_*.parquet")
    listing_processor = ListingProcessor(bronze_df)
    cleaned_df = listing_processor.process()
    listing_processor.validate_cleaned_data()

    process_silver_tables(cleaned_df, SILVER_SCHEMAS, storage, silver_bucket)
