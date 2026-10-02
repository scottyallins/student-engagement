# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_7_1_coach_legs_participation_history
# ============================================================================
# SILVER NESTED CHILD TABLE — lead_activites_raw_participation_history
# ============================================================================
# Purpose: Explodes nested participation_history within coach_legs
# Lineage: coach_legs → coach_leg → participation_history → participation_event
# Structure: Each event has action, timestamp
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw_coach_legs"
child_table  = "crm_ingestion.silver.lead_activites_raw_coach_legs_participation_history"

print("\n" + "="*80)
print("🔥 BUILDING NESTED CHILD TABLE — lead_activites_raw_coach_legs_participation_history")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("participation_history").isNotNull())
        .withColumn("participation_event", explode_outer(col("participation_history")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("user_id"),
            col("participation_event.action"),
            col("participation_event.timestamp"),
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
