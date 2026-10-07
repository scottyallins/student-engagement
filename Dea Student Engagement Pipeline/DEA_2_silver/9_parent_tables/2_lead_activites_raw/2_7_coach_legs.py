# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_7_coach_legs
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_coach_legs
# ============================================================================
# Purpose: Explodes coach_legs array (coaching session tracking)
# Structure: Array of structs with date_connected, date_created, date_done,
#            participation_history (nested array), status, user_id
# Lineage: coach_legs → coach_leg → coach_leg.field_name
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_coach_legs"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_coach_legs")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("coach_legs").isNotNull())
        .withColumn("coach_legs", explode_outer(col("coach_legs")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("coach_legs.user_id"),
            col("coach_legs.date_connected"),
            col("coach_legs.date_created"),
            col("coach_legs.date_done"),
            col("coach_legs.participation_history"),
            col("coach_legs.status"),
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
