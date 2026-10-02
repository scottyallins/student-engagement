# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_3_attendees
# ================================================================================
# 🔥 CHILD TABLE — lead_activites_raw_attendees
# Clear lineage: attendees → attendee → attendee.field_name
# ================================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_attendees"

print(f"\n{'='*80}")
print(f"🔥 BUILDING CHILD TABLE — {child_table.split('.')[-1]}")
print("="*80)

df_parent = spark.table(parent_table)

# Clear lineage: attendees → attendee → attendee.field_name
df_child = (
    df_parent
        .filter(col("attendees").isNotNull())
        .withColumn("attendees", explode_outer(col("attendees")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("attendees.contact_id"),
            col("attendees.email",
            col("attendees.is_organizer"),
            col("attendees.name"),
            col("attendees.status"),
            col("attendees.user_id"),
            col("bronze_insert_date")
        )
)

df_child.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(child_table)

print(f"\n✅ Table created: {child_table}")
print(f"🔢 Row count: {spark.table(child_table).count():,}")

print("\n📘 SCHEMA:")
spark.table(child_table).printSchema()

print("\n📊 SAMPLE ROWS (5):")
display(spark.table(child_table).limit(5))
