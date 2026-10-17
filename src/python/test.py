from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

# Create a SparkSession
spark = SparkSession.builder \
   .appName("PySpark Example") \
   .master("local[1]") \
   .getOrCreate()
from pyspark.sql.types import *

olympic_schema = StructType([
    StructField("Athlete", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("Sport", StringType(), True),
    StructField("Gold", IntegerType(), True),
    StructField("Silver", IntegerType(), True),
    StructField("Bronze", IntegerType(), True),
    StructField("Total", IntegerType(), True)
])
df=spark.read.csv("D:\\sparkData\\covid_data.csv")

olympics_df = spark.read \
    .option("header", "false") \
    .option("sep", "\t") \
    .schema(olympic_schema) \
    .csv("D:\\sparkData\\olympix_data.csv")
# Show the DataFrame

