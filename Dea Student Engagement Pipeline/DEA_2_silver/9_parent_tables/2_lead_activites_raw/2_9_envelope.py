# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2_9_envelope
# ⭐ 2_9_envelope — Silver Table Build Cell
# This extracts the top‑level envelope struct, not the nested arrays.
# Nested arrays become 2_9_1 through 2_9_6.

# Your envelope struct:

# Code
# envelope: struct
#     cc: array<struct>
#     to: array<struct>
#     bcc: array<struct>
#     date: string
#     from: array<struct>
#     sender: array<struct>
#     subject: string
#     reply_to: array<struct>
#     message_id: string
#     in_reply_to: string
#     is_autoreply: boolean
# All nested arrays are handled in later subtables.
from pyspark.sql.functions import col

parent_table = "crm_ingestion.silver.lead_activites_raw"
child_table  = "crm_ingestion.silver.lead_activites_raw_envelope"

df_parent = spark.table(parent_table)

df_child = (
    df_parent
        .filter(col("envelope").isNotNull())
        .select(
            col("activity_id"),
            col("lead_id"),
            col("envelope.date").alias("envelope_date"),
            col("envelope.subject").alias("envelope_subject"),
            col("envelope.message_id").alias("envelope_message_id"),
            col("envelope.in_reply_to").alias("envelope_in_reply_to"),
            col("envelope.is_autoreply").alias("envelope_is_autoreply"),
            # Keep arrays for child extractions
            col("envelope.bcc"),
            col("envelope.cc"),
            col("envelope.from"),
            col("envelope.reply_to"),
            col("envelope.sender"),
            col("envelope.to"),
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
spark.table(child_table).show(20, truncate=False)
