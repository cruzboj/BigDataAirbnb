from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    element_at,
    input_file_name,
    lit,
    regexp_extract,
    regexp_replace,
    size,
    split,
    when,
)
from pyspark.sql.types import StructType

from processing.schema import format_schema, parse_array_columns


class DataLoader:
    def __init__(self, app_name: str, spark: SparkSession):
        self.app_name = app_name
        self.spark = spark

    def load_batch(
        self, target_schema: StructType, file_paths: str | Iterable[str] | None = None
    ) -> list[DataFrame]:
        schema = format_schema(target_schema)

        if file_paths is None:
            raise ValueError("file_paths is required")

        paths = [file_paths] if isinstance(file_paths, str) else list(file_paths)
        if not paths:
            raise ValueError("file_paths cannot be empty")

        dataframes: list[DataFrame] = []
        for path in paths:
            df = (
                self.spark.read.schema(schema)
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
                .csv(path)
            )

            filename = input_file_name()
            basename = regexp_extract(filename, r"([^/]+)$", 1)
            stem = regexp_replace(basename, r"(\.[^./]+)+$", "")
            parts = split(stem, "_")

            city = when(size(parts) >= 2, element_at(parts, -2)).otherwise(lit(None))
            country = when(size(parts) >= 2, element_at(parts, -1)).otherwise(lit(None))

            enriched_df = (
                df.withColumn("filename", filename)
                .withColumn("city", city)
                .withColumn("country", country)
            )
            enriched_df = parse_array_columns(enriched_df, target_schema)
            enriched_df = enriched_df.withColumn("ingestion_ts", current_timestamp())

            dataframes.append(enriched_df)

        return dataframes

    def load_delayed(
        self, target_schema: StructType, path: str | None = None
    ) -> DataFrame:
        schema = format_schema(target_schema)

        if not path:
            raise ValueError("path cannot be empty")

        df = (
            self.spark.readStream.schema(schema)
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .csv(path)
        )

        dataframe = df.withColumn("ingestion_ts", current_timestamp())
        dataframe = dataframe.withWatermark("last_updated", "48 hours")

        return dataframe
