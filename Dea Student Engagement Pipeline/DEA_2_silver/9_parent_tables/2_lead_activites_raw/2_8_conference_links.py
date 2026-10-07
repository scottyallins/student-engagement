# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_8_conference_links
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_conference_links
# ============================================================================
# Purpose: Explodes conference_links array (Zoom, Teams, Meet links)
# Structure: Array of structs with type, url
# Lineage: conference_links → conference_link → conference_link.type, conference_link.url
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_conference_links"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_conference_links")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("conference_links").isNotNull())
        .withColumn("conference_link", explode_outer(col("conference_links")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("user_id"),
            col("conference_link.type"),
            col("conference_link.url"),
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
