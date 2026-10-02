# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_6_leads_raw_tasks
df_parent = spark.table("crm_ingestion.silver.leads_raw")

df_tasks = (
    df_parent
        .withColumn("task", explode_outer("tasks"))
        .select(
            col("bronze_insert_date"),
            col("id").alias("lead_id"),

            col("task.id").alias("task_id"),
            col("task._type"),
            col("task.agent_config_id"),
            col("task.assigned_to"),
            col("task.assigned_to_name"),
            col("task.contact_id"),
            col("task.contact_name"),
            col("task.created_by"),
            col("task.created_by_name"),
            col("task.date"),
            col("task.date_created"),
            col("task.date_updated"),
            col("task.deduplication_key"),
            col("task.due_date"),
            col("task.is_complete"),
            col("task.is_dateless"),
            col("task.is_primary_lead_notification"),
            col("task.lead_id").alias("task_lead_id"),
            col("task.lead_name"),
            col("task.object_id"),
            col("task.object_type"),
            col("task.organization_id"),
            col("task.priority"),
            col("task.resolution"),
            col("task.sequence_id"),
            col("task.sequence_subscription_id"),
            col("task.text"),
            col("task.updated_by"),
            col("task.updated_by_name"),
            col("task.view")
        )
)

table_name = "crm_ingestion.silver.leads_raw_tasks"

df_contacts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crm_ingestion.silver.leads_raw_tasks")

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
