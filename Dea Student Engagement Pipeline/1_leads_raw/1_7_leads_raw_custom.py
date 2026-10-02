# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_7_leads_raw_custom
df_parent = spark.table("crm_ingestion.silver.leads_raw")

df_custom = (
    df_parent
        .select(
            col("bronze_insert_date"),
            col("id").alias("lead_id"),

            col("custom.Avatar").alias("Avatar"),
            col("custom.BASE_ENGAGEMENT_SCORE"),
            col("custom.CSM"),
            col("custom.Channel_ID"),
            col("custom.`Contract Start Date`").alias("Contract_Start_Date"),
            col("custom.DAYS_SINCE_LAST_EMAIL"),
            col("custom.DAYS_SINCE_LAST_LOGIN"),
            col("custom.DAYS_SINCE_LAST_MEETING"),
            col("custom.DAYS_SINCE_LAST_MESSAGE_FROM_CLIENT"),
            col("custom.DAYS_SINCE_LAST_MESSAGE_FROM_TEAM_MEMBER"),
            col("custom.ENGAGEMENT_PATTERN"),
            col("custom.FINAL_SCORE"),
            col("custom.Funnel"),
            col("custom.`Funnel ID`").alias("Funnel_ID"),
            col("custom.HEALTH_BAND"),
            col("custom.`Lead Owner`").alias("Lead_Owner"),
            col("custom.MEETING_ENGAGED_FLAG"),
            col("custom.PLATFORM_ENGAGED_FLAG"),
            col("custom.`Page ID`").alias("Page_ID"),
            col("custom.`Reactivation Owner`").alias("Reactivation_Owner"),
            col("custom.SENTIMENTS_LAST_30_DAYS"),
            col("custom.SLACK_ENGAGED_FLAG"),
            col("custom.`SMS Blast Sept`").alias("SMS_Blast_Sept"),
            col("custom.TOTAL_MISSED_CALLS"),
            col("custom.Tier"),
            col("custom.`Time Zone`").alias("Time_Zone"),
            col("custom.`Type Of Follow Up`").alias("Type_Of_Follow_Up"),
            col("custom.auto_checkin")
        )
)

table_name = "crm_ingestion.silver.leads_raw_custom"

df_contacts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crm_ingestion.silver.leads_raw_custom")

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
