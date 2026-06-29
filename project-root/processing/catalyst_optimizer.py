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
class catalystOptimizer:
    def __init__(self,app_name):
        self.app_name = app_name

    def create_spark_session(self,set_type):
        """
            
        """
        if set_type is None:
            raise ValueError("App name cannot be None")
            
        spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        
        #define_data.py
        if set_type == "batch":
            df_batch = create_dataframe_batch(spark)
            return df_batch
        elif set_type == "streaming":
            df_stream = create_dataframe_streaming(spark)
            return df_stream
        
        df_late_arrivals = create_dataframe_late_arrivals(spark)


def main():
    c_o = catalystOptimizer("Catalyst_Optimizer_Test")
    df = c_o.create_spark_session("batch")
    print("Loading datasets...")
    print("--- Batch DataFrame Top 10 Rows ---")
    df.filter(col("city") == "denmark").select("id", "name", "price", "city", "ingestion_ts").show(5)
    df.filter(col("city") == "chicago").select("id", "name", "price" , "city", "ingestion_ts").show(5)
if __name__ == '__main__':
    main()