# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_10_2_integrations_integration_data
# ============================================================================
# SILVER NESTED CHILD TABLE — lead_activites_raw_integrations_integration_data
# ============================================================================
# Purpose: Extracts integration_data STRUCT fields from integrations
# Lineage: integrations → integration_data (struct fields, NOT participants yet)
# ============================================================================

from pyspark.sql.functions import col

parent_table = "crm_ingestion.silver.lead_activites_raw_integrations"
child_table  = "crm_ingestion.silver.lead_activites_raw_integrations_integration_data"

print("\n" + "="*80)
print("🔥 BUILDING NESTED CHILD TABLE — lead_activites_raw_integrations_integration_data")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("integration_data").isNotNull())
        .select(
            col("activity_id"),
            col("lead_id"),
            col("integration_id"),
            col("integration_data.duration"),
            col("integration_data.end_time"),
            col("integration_data.processing_status"),
            col("integration_data.start_time"),
            col("integration_data.zoom_account_id"),
            col("integration_data.zoom_uuid"),
            col("integration_data.participants"),  # Keep array for next extraction
            col("bronze_insert_date")
        )
)

df_child.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(child_table)

print(f"\n✅ Table created: {child_table}")
print(f"🔢 Row count: {df_child.count():,}")

print("\n📘 SCHEMA:")
df_child.printSchema()

print("\n📊 SAMPLE ROWS (5):")
display(spark.table(child_table).limit(5))
