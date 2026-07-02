from data_loader import DataLoader
from config.const import PATH_MINIO

"""
    TODO: make 3 buckets (brozne,silver,gold)
"""
df = DataLoader("airbnb").create_dataframe("batch")

df.write.mode("overwrite").parquet(PATH_MINIO)