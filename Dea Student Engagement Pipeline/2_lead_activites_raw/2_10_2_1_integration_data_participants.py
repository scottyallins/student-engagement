# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_10_2_1_integration_data_participants
# ============================================================================
# SILVER NESTED GRANDCHILD TABLE — lead_activites_raw_integrations_integration_data_participants
# ============================================================================
# Purpose: Explodes participants array from integration_data
# Lineage: integrations → integration_data → participants → participant
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw_integrations_integration_data"
child_table  = "crm_ingestion.silver.lead_activites_raw_integrations_integration_data_participants"

print("\n" + "="*80)
print("🔥 BUILDING NESTED GRANDCHILD TABLE — lead_activites_raw_integrations_integration_data_participants")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("participants").isNotNull())
        .withColumn("participant", explode_outer(col("participants")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("integration_id"),
            col("participant.name").alias("participant_name"),
            col("participant.zoom_id").alias("participant_zoom_id"),
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
