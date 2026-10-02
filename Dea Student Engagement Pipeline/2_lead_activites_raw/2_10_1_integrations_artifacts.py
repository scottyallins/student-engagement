# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_10_1_integrations_artifacts
# ============================================================================
# SILVER NESTED CHILD TABLE — lead_activites_raw_integrations_artifacts
# ============================================================================
# Purpose: Explodes artifacts array within integrations
# Lineage: integrations → integration → artifacts → artifact
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw_integrations"
child_table  = "crm_ingestion.silver.lead_activites_raw_integrations_artifacts"

print("\n" + "="*80)
print("🔥 BUILDING NESTED CHILD TABLE — lead_activites_raw_integrations_artifacts")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("artifacts").isNotNull())
        .withColumn("artifacts", explode_outer(col("artifacts")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("artifacts.id").alias("integration_id"),
            col("artifacts.created_at").alias("artifact_created_at"),
            col("artifacts.id").alias("artifact_id"),
            col("artifacts.organization_id").alias("artifact_organization_id"),
            col("artifacts.updated_at").alias("artifact_updated_at"),
            col("artifacts.artifact_data"),
            col("artifacts.artifact_type"),
            col("artifacts.event_integration_id"),
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
