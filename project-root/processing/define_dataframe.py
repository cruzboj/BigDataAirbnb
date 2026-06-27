#define_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import csv
import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from config.const import CSV_FILE_PATH , LISTINGS_PATH
from schema import bronze_listing_schema

#TODO: LOOP all listings files

def create_dataframe_batch(spark):
    """
        read csv file format convert into Dataframe
        enriched Dataframe with city categorie 
    """
    if not isinstance(spark, SparkSession):
        raise TypeError(f"Expected a SparkSession object, but got {type(spark).__name__}")

    dataframes = []

    for path in LISTINGS_PATH:
        temp_df = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .csv(path)

        temp_df = temp_df.withColumn("filename", input_file_name())
        temp_df = temp_df.withColumn("city", regexp_extract(col("filename"), "listings_([a-zA-Z]+)", 1))
        temp_df = temp_df.withColumn("ingestion_ts", current_timestamp())
        dataframes.append(temp_df)

        from functools import reduce
        from pyspark.sql import DataFrame
        
        df = reduce(DataFrame.unionByName, dataframes)
    
    return df

def create_dataframe_streaming(spark):
    return

def create_dataframe_late_arrivals(spark):
    return