from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import countDistinct, when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("PySpark Example") .master("local[1]") .getOrCreate()

zipcodedf=spark.readStream.option("header",True).format("csv").load("D:\\sparkData\\zipcode.csv")

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

olympics_read_df=spark.read.schema(olympic_schema).option("sep",'\t').csv("D:\\sparkData\\olympix_data.csv")
olympics_read_df.show(10,False)

olympics_df=olympics_read_df.drop_duplicates().dropna()
#athlet count
olympics_df.select(countDistinct("athlete").alias("athlete_count"))
#country wise medal analysis
country_medals=olympics_df.groupBy("year").agg(
    f.sum("Gold").alias("Total_gold"),
    f.sum("Silver").alias("Total_silver"),
    f.sum("Bronze").alias("Total_bronze"),
    f.sum("Total").alias("Total_Medals"))

country_medals.orderBy("Total_Medals",ascending=False).select("year","Total_Medals")

#olympics_df.write.partitionBy("Country").save("D:\\Batch_45\\olympix_results")


result=olympics_df.withColumn(
    "Gold_percentage",when(col("Total") > 0,col("Gold")/col("Total")*100).otherwise(0)
)

result.select("athlete","Gold","Total","Gold_percentage")


age_df=olympics_df.withColumn(
    "age_group", when(col("Age") < 20 ,"under 20")
                .when(col("age") < 30 ,"20-29")
                .when(col("age") < 40 ,"30-39")
                .otherwise("40+"))

age_df.select("Athlete","Age","age_group").withColumnRenamed("age_group","age_group1").show(10,False)

athlete_medals=olympics_df.groupBy("athlete").agg(
    f.sum("total").alias("Total_medals")
)

windowspec=Window.partitionBy("sport").orderBy(col("Total_medals").desc())

rank_atheles=(athlete_medals.join(olympics_df
                                 .select("Athlete","sport")
                                 .distinct(),"athlete")
              .withColumn("rank", f.dense_rank().over(windowspec)))

rank_atheles.filter(col("rank") <=3).show()


