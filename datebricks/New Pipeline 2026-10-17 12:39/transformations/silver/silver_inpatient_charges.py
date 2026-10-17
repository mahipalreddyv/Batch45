from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim


@dp.table(
    name="silver_inpatient_charges",
    comment="Cleaned inpatient charges with snake_case column names and trimmed strings",
)
def silver_inpatient_charges():
    return (
        spark.readStream.table("bronze_inpatient_charges")
        .select(
            trim(col("DRGDefinition")).alias("drg_definition"),
            col("ProviderId").alias("provider_id"),
            trim(col("ProviderName")).alias("provider_name"),
            trim(col("ProviderStreetAddress")).alias("provider_street_address"),
            trim(col("ProviderCity")).alias("provider_city"),
            trim(col("ProviderState")).alias("provider_state"),
            col("ProviderZipCode").alias("provider_zip_code"),
            trim(col("HospitalReferralRegionDescription")).alias(
                "hospital_referral_region_description"
            ),
            col("TotalDischarges").alias("total_discharges"),
            col("AverageCoveredCharges").alias("average_covered_charges"),
            col("AverageTotalPayments").alias("average_total_payments"),
            col("AverageMedicarePayments").alias("average_medicare_payments"),
        )
    )
