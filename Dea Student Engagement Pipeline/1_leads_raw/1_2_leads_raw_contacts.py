# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_2_leads_raw_contacts
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_parent = spark.table("crm_ingestion.silver.leads_raw")

df_contacts = (
    df_parent
        .withColumn("contact", explode_outer("contacts"))
        .select(
            col("bronze_insert_date"),
            col("id").alias("lead_id"),# Parent lead ID
            col("contact.id").alias("contact_id"),  # Contact ID
            col("contact.created_by"), # Contact struct fields
            col("contact.date_created"),
            col("contact.date_updated"),
            col("contact.display_name"),
            col("contact.timezone"),
            col("contact.timezone_source"),
            col("contact.title"),
            col("contact.updated_by"),
            col("contact.organization_id"),
            col("contact.name"),
            col("contact.lead_id").alias("contact_lead_id"),  # Nested FK
            col("contact.emails"),  # Arrays
            col("contact.phones"),  # Arrays
            col("contact.integration_links"),  # Arrays
            col("contact.urls"), # Arrays

        )
)

# ⭐ Sanitize column names
def sanitize_column_name(col_name):
    return (col_name.replace(" ", "_")
                    .replace(".", "_")
                    .replace(",", "")
                    .replace(";", "")
                    .replace("{", "")
                    .replace("}", "")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("\n", "")
                    .replace("\t", "")
                    .replace("=", "_")
                    .replace("-", "_"))

for old_col in df_contacts.columns:
    new_col = sanitize_column_name(old_col)
    if old_col != new_col:
        df_contacts = df_contacts.withColumnRenamed(old_col, new_col)


# ⭐ Write Table 1_2 — leads_raw_contacts
table_name = "crm_ingestion.silver.leads_raw_contacts"

path = "dbfs:/Volumes/crm_ingestion/silver/crm_storage/leads_raw_contacts"

df_contacts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crm_ingestion.silver.leads_raw_contacts")

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
