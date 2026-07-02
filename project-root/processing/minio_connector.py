from data_loader import DataLoader
from config.const import MINIO_PATH

"""
    TODO: make 3 buckets (brozne,silver,gold)
"""

bronze_bucket = "bronze-bucket"
silver_bucket = "silver-bucket"
gold_bucket = "gold-bucket"


data_loader = DataLoader("airbnb")
spark = data_loader.create_spark_session()
df = data_loader.create_dataframe()

df.write.mode("overwrite").parquet(MINIO_PATH + f"{bronze_bucket}/sample.parquet")