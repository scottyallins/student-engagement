# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_14_opens
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_opens
# ============================================================================
from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_opens"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_opens")
print("="*80)

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("opens").isNotNull())
        .withColumn("open", explode_outer(col("opens")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("open"),
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
