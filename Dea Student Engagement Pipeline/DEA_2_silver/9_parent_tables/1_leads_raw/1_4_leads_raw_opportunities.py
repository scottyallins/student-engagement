# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_5_leads_raw_opportunities
df_parent = spark.table("crm_ingestion.silver.leads_raw")

df_opps = (
    df_parent
        .withColumn("opportunity", explode_outer("opportunities"))
        .select(
            col("bronze_insert_date"),
            col("id").alias("lead_id"),

            # Core fields
            col("opportunity.id").alias("opportunity_id"),
            col("opportunity.contact_id"),
            col("opportunity.contact_name"),
            col("opportunity.created_by"),
            col("opportunity.created_by_name"),
            col("opportunity.date_created"),
            col("opportunity.date_updated"),
            col("opportunity.date_lost"),
            col("opportunity.date_won"),
            col("opportunity.expected_value"),
            col("opportunity.annualized_value"),
            col("opportunity.annualized_expected_value"),
            col("opportunity.confidence"),
            col("opportunity.lead_id").alias("opportunity_lead_id"),
            col("opportunity.lead_name"),
            col("opportunity.note"),
            col("opportunity.note_html"),
            col("opportunity.organization_id"),
            col("opportunity.pipeline_id"),
            col("opportunity.pipeline_name"),
            col("opportunity.status_display_name"),
            col("opportunity.status_id"),
            col("opportunity.status_label"),
            col("opportunity.status_type"),
            col("opportunity.updated_by"),
            col("opportunity.updated_by_name"),
            col("opportunity.user_id"),
            col("opportunity.user_name"),
            col("opportunity.value"),
            col("opportunity.value_currency"),
            col("opportunity.value_formatted"),
            col("opportunity.value_period"),

            # Custom fields — MUST use backticks
            col("opportunity.`custom.cf_DPNuCUGxrFXoDtl2d8te9KKkJgapCb2JtLhR1WaRrzg`").alias("custom_cf_DPNu"),
            col("opportunity.`custom.cf_HXUbFt9DpwvUwJUuRf0wmgP5rgaf76BoU7ElHgmLwBL`").alias("custom_cf_HXUb"),
            col("opportunity.`custom.cf_Z3ONKlVKtvaoA9X93OiwyZSoWKCjp5bhUnAkpGrE2h0`").alias("custom_cf_Z3ON"),
            col("opportunity.`custom.cf_nBQgTEp26tGIn2MvQGro5Mc5DebNhwYVo1IF2hN9ert`").alias("custom_cf_nBQg"),

            # Custom array
            col("opportunity.`custom.cf_cGI9OyyDGwmoRPiVEK20ExKAB6WhcNCoYDV1itlVMXF`").alias("custom_cf_array"),

            # Arrays
            col("opportunity.attachments"),
            col("opportunity.integration_links")
        )
)

table_name = "crm_ingestion.silver.leads_raw_opportunities"

df_opps.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crm_ingestion.silver.leads_raw_opportunities")

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
