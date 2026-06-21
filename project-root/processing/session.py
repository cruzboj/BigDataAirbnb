from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """
        Create and return a SparkSession configured for the Airbnb project (Local Execution).
    """
    spark = SparkSession.builder \
        .appName("Airbnb") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "rest") \
        .config("spark.sql.catalog.spark_catalog.uri", "http://rest:8181") \
        .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.spark_catalog.s3.path-style-access", "true") \
        .config("spark.sql.catalog.spark_catalog.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.spark_catalog.s3.secret-access-key", "password") \
        .getOrCreate()
    
    return spark