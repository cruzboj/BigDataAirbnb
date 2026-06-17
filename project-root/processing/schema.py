from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("AirBnB").getOrCreate()

my_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("signup_date", TimestampType(), True)
])

df = spark.read.schema(my_schema).json("path/to/data.json")

df.show()