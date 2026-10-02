# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_2_3_leads_raw_contacts_integration_links
df_contacts_links = (
    df_contacts
        .withColumn("link", explode_outer("integration_links"))
        .select(
            col("bronze_insert_date"),
            col("lead_id"),
            col("contact_id"),
            col("link.name").alias("link_name"),
            col("link.url").alias("link_url")
        )
)
table_name = "crm_ingestion.silver.leads_raw_contacts_integration_links"

df_contacts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crm_ingestion.silver.leads_raw_contacts_integration_links")

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
