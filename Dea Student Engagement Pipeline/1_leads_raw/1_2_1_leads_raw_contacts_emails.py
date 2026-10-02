# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_2_1_leads_raw_contacts_emails
df_contacts = spark.table("crm_ingestion.silver.leads_raw_contacts")

df_contact_emails = (
    df_contacts
        .withColumn("email", explode_outer("emails"))
        .select(
            col("bronze_insert_date"),
            col("lead_id"),
            col("contact_id"),
            col("email.email").alias("email"),
            col("email.is_unsubscribed").alias("is_unsubscribed"),
            col("email.type").alias("email_type")
        )
)

table_name = "crm_ingestion.silver.leads_raw_contacts_emails"

df_contact_emails.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable("leads_raw_contacts_emails")

print(f"\n✅ Table created: {"leads_raw_contacts_emails"}")
print(f"🔢 Row count: {spark.table("leads_raw_contacts_emails").count():,}")

print("\n📘 SCHEMA:")
spark.table("leads_raw_contacts_emails").printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table("leads_raw_contacts_emails").limit(3))
