from processing.const import (
    bronze_bucket,
    gold_bucket,
    silver_bucket,
)
from processing.data_loader import DataLoader
from processing.storage.s3 import S3StorageHandler

data_loader = DataLoader("airbnb")
spark = data_loader.create_spark_session()

# sample_rows = [
#     (1, "Copenhagen", 120.5),
#     (2, "Chicago", 95.0),
#     (3, "Aarhus", 80.25),
# ]

# sample_df = spark.createDataFrame(sample_rows, ["id", "city", "price"])
# output_path = f"s3a://{bronze_bucket}/sample.parquet"

# sample_df.write.mode("overwrite").parquet(output_path)
# print(f"Wrote sample parquet to: {output_path}")

# check_df = spark.read.parquet(output_path)
# check_df.show(truncate=False)

batches = data_loader.load_batch()
storage = S3StorageHandler(spark)
storage.create_buckets([bronze_bucket, silver_bucket, gold_bucket])
for i, batch in enumerate(batches):
    storage.bucket_upload(bronze_bucket, f"test_file-{i}.parquet", batch)

df = storage.read_file(f"s3a://{bronze_bucket}/test_file-0.parquet")
df.show(truncate=False)

spark.stop()
