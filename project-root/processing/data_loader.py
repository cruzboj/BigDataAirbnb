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

    def load_batch(self,target_schema):
        schema = format_schema(target_schema)

        data = []

        for path in LISTINGS_PATH:
            df = (
                self.spark.read.schema(schema)
                .option("header", "true")
                .option("multiLine", "true")
                .option("quote", '"')
                .option("escape", '"')
                .csv(path)

            )

            if target_schema == bronze_listing_schema:
                filename = input_file_name()
                city = regexp_extract(filename, r"listings_([a-zA-Z]+)_", 1)
                country = regexp_extract(filename, r"listings_[a-zA-Z]+_([a-zA-Z]+)", 1)
                df = df.withColumn("filename", filename)
                df = df.withColumn("city", city)
                df = df.withColumn("country", country)


            df = parse_array_columns(df, target_schema)

            df = df.withColumn("ingestion_ts", current_timestamp())
            

            data.append(df)

        return data
