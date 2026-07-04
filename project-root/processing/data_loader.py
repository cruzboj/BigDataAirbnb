from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    regexp_extract,
)

from config.const import LISTINGS_PATH
from processing.schema import bronze_listing_schema, format_schema, parse_array_columns


class DataLoader:
    def __init__(self, app_name: str, spark: SparkSession):
        self.app_name = app_name
        self.spark = spark

    def load_batch(self):
        schema = format_schema(bronze_listing_schema)

        data = []
        for path in LISTINGS_PATH:
            df = (
                self.spark.read.schema(schema)
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
                .csv(path)
                .withColumn(
                    "city",
                    regexp_extract(input_file_name(), r"listings_([a-zA-Z]+)", 1),
                )
            )

            df = parse_array_columns(df, bronze_listing_schema)

            df = df.withColumn("filename", input_file_name())
            df = df.withColumn("ingestion_ts", current_timestamp())
            data.append(df)

        return data
