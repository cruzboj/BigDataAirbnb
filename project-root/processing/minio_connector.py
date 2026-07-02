from processing.data_loader import DataLoader

"""
    TODO: make 3 buckets (brozne,silver,gold)
"""

bronze_bucket = "bronze-bucket"
silver_bucket = "silver-bucket"
gold_bucket = "gold-bucket"


data_loader = DataLoader("airbnb")
spark = data_loader.create_spark_session()

# sample_rows = [
#     (1, "Copenhagen", 120.5),
#     (2, "Chicago", 95.0),
#     (3, "Aarhus", 80.25),
# ]

# sample_df = spark.createDataFrame(sample_rows, ["id", "city", "price"])
# output_path = MINIO_PATH + f"{bronze_bucket}/sample.parquet"

# sample_df.write.mode("overwrite").parquet(output_path)
# print(f"Wrote sample parquet to: {output_path}")

# check_df = spark.read.parquet(output_path)
# check_df.show(truncate=False)

data_loader.load_batch()
# TODO: initialize buckets - bronze, silver, gold from the minio SDK
# TODO: take loaded batch data and insert it into minio s3 storage


spark.stop()
