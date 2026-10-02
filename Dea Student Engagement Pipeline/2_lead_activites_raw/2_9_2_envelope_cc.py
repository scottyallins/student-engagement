# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_9_2_envelope_cc
from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw_envelope"
child_table  = "crm_ingestion.silver.lead_activites_raw_envelope_cc"

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("cc").isNotNull())
        .withColumn("cc", explode_outer(col("cc")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("cc.name").alias("user_id"),
            col("cc.email").alias("email"),
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
spark.table(child_table).show(4000, truncate=False)
