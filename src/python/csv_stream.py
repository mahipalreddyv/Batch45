from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("PySpark Example") .master("local[1]") .getOrCreate()
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

olympics_df = (spark.readStream.option("header", "false") \
    .option("sep", "\t") \
    .schema(olympic_schema).format("csv").load("D:\\batch_45\\source"))

query=(olympics_df.writeStream.format("csv")
       .option("header", "true")
       .option("path","D:\\batch_45\\target")
      .option("checkpointLocation","D:\\batch_45\\checkpoint")
       .outputMode("append")
       .start())


query.awaitTermination()