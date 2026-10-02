# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_10_integrations
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_integrations
# ============================================================================
# Purpose: Explodes integrations array (Zoom, calendar integrations)
# NOW INCLUDES: artifacts array and integration_data.participants array!
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_integrations"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_integrations")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("integrations").isNotNull())
        .withColumn("integrations", explode_outer(col("integrations")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("integrations.id").alias("integration_id"),
            col("integrations.created_at"),
            col("integrations.event_occurrence_id"),
            col("integrations.integration_name"),
            col("integrations.integration_object_id"),
            col("integrations.organization_id"),
            col("integrations.artifacts"),              # NEW!
            col("integrations.integration_data"),       # Contains participants array!
            col("bronze_insert_date")
        )
)

df_child.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(child_table)


print(f"✅ Created: {child_table}")
print(f"🔢 Rows: {spark.table(child_table).count():,}")
spark.table(child_table).show(50, truncate=False)
