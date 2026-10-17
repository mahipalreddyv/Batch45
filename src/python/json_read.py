from pyspark.sql import SparkSession
import sys
spark = (SparkSession.builder.appName("PySpark Example")
         .master("local[1]") .getOrCreate())


schema_path=sys.argv[1]
source_path=sys.argv[2]
target_path=sys.argv[3]

readjson_schema=spark.read.json(schema_path).schema

read_DF1=spark.readStream.schema(readjson_schema).format("json").load(source_path)

expr="""events.client as clientName,
events.beaconType as beaconType,
events.beaconVersion as beaconVersion,
events.data.displayAd.indexExchangeHB as indexExchangeHB,
events.data.displayAd.amazonA9HB as amazonA9HB,
events.data.displayAd.instanceID as instanceID,
events.data.displayAd.instanceID as inView,
events.data.milestones.amazonA9Requested as amazonA9Requested,
events.data.milestones.amazonA9Received as amazonA9Received,
events.data.milestones.adRequested as adRequested
 """.split(",")

exprDF=read_DF1.selectExpr(expr).where("clientName !='NULL'")

exprDF.show()

exprDF.writeStream.format("console").outputMode("append").start().awaitTermination()
