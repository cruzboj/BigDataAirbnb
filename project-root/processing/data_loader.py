#catalyst_optimizer.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType , DoubleType , IntegerType
from pyspark.sql.functions import col,to_timestamp,expr

from dataframe import create_dataframe_batch , create_dataframe_streaming ,create_dataframe_late_arrivals
"""
    #TODO :
        1.unresolved logical plan (parsed: make sure it wont spit garbage)
            1a.make 3 sets of readinng :
                - batch data (nightly batch)
                - steaming (real time)
                - late arrivals (up to 4 8h delay)
        2.type check
        3.optimized join plan
        4.physical plan run the code and see if it works
    
    use .explain() to see the plan and check if it is optimized or not
"""
class DataLoader:
    def __init__(self,app_name):
        self.app_name = app_name

    def create_spark_session(self):
        """
            Creates a SparkSession with the necessary S3A dependencies for MinIO.
        """
        self.spark = SparkSession.builder.appName(self.app_name)\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
        return self.spark
        
    def create_dataframe(self):
        """
            
        """
        
        #define_data.py
        df_batch = create_dataframe_batch(self.spark)
        # df_stream = create_dataframe_streaming(self.spark)
        # df_late_arrivals = create_dataframe_late_arrivals(self.spark)
        return df_batch

def main():
    c_o = DataLoader("Catalyst_Optimizer_Test")
    df = c_o.create_dataframe("batch")
    print("Loading datasets...")
    print("--- Batch DataFrame Top 10 Rows ---")
    df.filter(col("city") == "denmark").select("id", "name", "price", "city", "ingestion_ts").show(5)
    df.filter(col("city") == "chicago").select("id", "name", "price" , "city", "ingestion_ts").show(5)
    
if __name__ == '__main__':
    main()