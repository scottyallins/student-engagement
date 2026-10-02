# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_6_cc
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_cc
# ============================================================================
# Purpose: Explodes cc array (carbon copy recipients)
# Structure: Scalar array<string> of email addresses
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_cc"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_cc")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("cc").isNotNull())
        .withColumn("cc_address", explode_outer(col("cc")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("cc_address").alias("email"),
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
