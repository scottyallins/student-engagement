# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_11_mentions
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_mentions
# ============================================================================
from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_mentions"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_mentions")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("mentions").isNotNull())
        .withColumn("mention", explode_outer(col("mentions")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("mention"),
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
df_child.printSchema()
display(spark.table(child_table).limit(5))
