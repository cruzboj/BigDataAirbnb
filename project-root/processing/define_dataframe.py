#define_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import csv
import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from config.const import LISTINGS_PATH , KAFKA_BROKER , TOPIC_NAME
from schema import bronze_listing_schema



def create_dataframe_batch(spark):
    """
        read csv file format convert into Dataframe
        enriched Dataframe with city categorie 
    """
    if not isinstance(spark, SparkSession):
        raise TypeError(f"Expected a SparkSession object, but got {type(spark).__name__}")

    dataframes = []

    """
        #TODO: 
            2.type check
            3.optimized join plan
            4.physical plan run the code and see if it works
    """

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
    """
        listen to kafka consumer "topic" and add then as they get streamed
    """
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    
    df_parsed = df_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    return

def create_dataframe_late_arrivals(spark):
    return