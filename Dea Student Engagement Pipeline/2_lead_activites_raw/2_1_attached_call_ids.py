# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_1_attached_call_ids
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_attached_call_ids
# ============================================================================
# Purpose: Many-to-many relationship table linking meetings to related call IDs
# This table is NEEDED when:
#   - You need to find all calls associated with a meeting
#   - You're analyzing meeting preparation (calls before meeting)
#   - You're tracking follow-up calls after meetings
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_attached_call_ids"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_attached_call_ids")
print("="*80)

df_parent = spark.table(parent_table)

# Explode the attached_call_ids array into individual rows
df_child = (
    df_parent
        .filter(col("attached_call_ids").isNotNull())  # Only rows with data
        .withColumn("call_id", explode_outer(col("attached_call_ids")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("call_id"),
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
