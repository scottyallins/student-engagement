# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_9_4_envelope_reply_to
from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw_envelope"
child_table  = "crm_ingestion.silver.lead_activites_raw_envelope_reply_to"

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("reply_to").isNotNull())
        .withColumn("reply_to_item", explode_outer(col("reply_to")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("reply_to_item.name").alias("user_id"),
            col("reply_to_item.email").alias("email"),
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
spark.table(child_table).show(200, truncate=False)
