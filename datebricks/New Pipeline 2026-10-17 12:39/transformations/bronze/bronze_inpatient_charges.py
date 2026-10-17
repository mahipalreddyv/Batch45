from pyspark import pipelines as dp


@dp.table(
    name="bronze_inpatient_charges",
    comment="Raw ingestion of inpatient charges CSV data from the batch45 volume",
)
def bronze_inpatient_charges():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("pathGlobFilter", "inpatientCharges.csv")
        .load("/Volumes/batch45_catalog/default/test/")
    )
