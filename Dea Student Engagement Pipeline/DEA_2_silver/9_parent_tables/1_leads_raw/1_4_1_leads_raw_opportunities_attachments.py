# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_5_1_leads_raw_opportunities_attachments
from pyspark.sql.functions import col, explode_outer

df_opps = spark.table("crm_ingestion.silver.leads_raw_opportunities")

df_opps_attachments = (
    df_opps
        .withColumn("attachment", explode_outer("attachments"))
        .select(
            col("bronze_insert_date"),
            col("lead_id"),
            col("opportunity_id"),
            col("attachment")
        )
)

table_name = "crm_ingestion.silver.leads_raw_opportunities_attachments"

df_opps_attachments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
