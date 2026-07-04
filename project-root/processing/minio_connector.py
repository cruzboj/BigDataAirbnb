from processing.data_loader import DataLoader
from minio import Minio
from minio.error import S3Error

"""
    TODO: make 3 buckets (brozne,silver,gold)
"""



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

df = data_loader.load_batch()
# TODO: initialize buckets - bronze, silver, gold from the minio SDK
# TODO: take loaded batch data and insert it into minio s3 storage

def initialize_buckets():

    bronze_bucket = "bronze-bucket"
    silver_bucket = "silver-bucket"
    gold_bucket = "gold-bucket"

    buckets = [bronze_bucket, silver_bucket, gold_bucket]

    client = Minio(
        "localhost:9000", 
        access_key="admin",
        secret_key="password",
        secure=False
    )

    for bucket_name in buckets:
        try:
            found = client.bucket_exists(bucket_name)
            if not found:
                client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created successfully.")
            else:
                print(f"Bucket '{bucket_name}' already exists.")
        except S3Error as err:
            print(f"Error occurred with bucket '{bucket_name}': {err}")


spark.stop()

if __name__ == "__main__":
    initialize_buckets()