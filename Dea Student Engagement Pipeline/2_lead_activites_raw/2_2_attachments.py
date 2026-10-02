# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_2_attachments
# ============================================================================
# SILVER CHILD TABLE — lead_activites_raw_attachments
# ============================================================================
# Purpose: Explodes attachments array (files, images, PDFs attached to activities)
# Coverage: ~91% of activities have attachments
# ============================================================================

from pyspark.sql.functions import col, explode_outer

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_attachments"

print("\n" + "="*80)
print("🔥 BUILDING CHILD TABLE — lead_activites_raw_attachments")
print("="*80)

df_parent = spark.table(parent_table)

# Explode the attachments array into individual rows
# Use singular parent name for clear lineage: attachments → attachment
df_child = (
    df_parent
        .filter(col("attachments").isNotNull())
        .withColumn("attachment", explode_outer(col("attachments")))
        .select(
            col("activity_id"),
            col("lead_id"),
            col("attachment.content_id"),
            col("attachment.content_type"),
            col("attachment.filename"),
            col("attachment.inline_only"),
            col("attachment.media_id"),
            col("attachment.size"),
            col("attachment.thumbnail_url"),
            col("attachment.url"),
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
