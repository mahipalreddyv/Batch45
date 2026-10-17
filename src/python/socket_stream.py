from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split
from pyspark.sql.functions import window


spark = SparkSession.builder.appName("PySpark Example") .master("local[1]") .getOrCreate()
df=spark.readStream.format("socket").option("host","localhost").option("port","9999").load()

word=df.select(explode(split(df.value," ")).alias("word"))



query=word.writeStream.format("csv") \
.option("path","D:\\batch_45\\target2") \
.option("checkpointLocation","checkpoint_2") \
.outputMode("append") \
.start()


query.awaitTermination()