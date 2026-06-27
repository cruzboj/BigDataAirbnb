#catalyst_optimizer.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType , DoubleType , IntegerType
from pyspark.sql.functions import col,to_timestamp,expr

from define_dataframe import create_dataframe_batch , create_dataframe_streaming ,create_dataframe_late_arrivals
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

def create_spark_session(app_name):
    """
    """
    if app_name is None:
        raise ValueError("App name cannot be None")
        
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    #define_data.py
    df_batch = create_dataframe_batch(spark)
    df_stream = create_dataframe_streaming(spark)
    df_late_arrivals = create_dataframe_late_arrivals(spark)

    return df_batch


def main():
    df = create_spark_session("Catalyst_Optimizer_Test")

    print("Loading datasets...")
    print("--- Batch DataFrame Top 10 Rows ---")
    df.filter(col("city") == "denmark").select("id", "name", "price", "city", "ingestion_ts").show(5)
    df.filter(col("city") == "chicago").select("id", "name", "price" , "city", "ingestion_ts").show(5)
if __name__ == '__main__':
    main()