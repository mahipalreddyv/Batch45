from pyspark import pipelines as dp
from pyspark.sql.functions import avg, count, sum as spark_sum, round as spark_round


@dp.materialized_view(
    name="gold_charges_by_state",
    comment="Aggregated inpatient charges summary by provider state",
)
def gold_charges_by_state():
    return (
        spark.read.table("silver_inpatient_charges")
        .groupBy("provider_state")
        .agg(
            count("*").alias("total_records"),
            spark_sum("total_discharges").alias("total_discharges"),
            spark_round(avg("average_covered_charges"), 2).alias("avg_covered_charges"),
            spark_round(avg("average_total_payments"), 2).alias("avg_total_payments"),
            spark_round(avg("average_medicare_payments"), 2).alias("avg_medicare_payments"),
        )
        .orderBy("provider_state")
    )
