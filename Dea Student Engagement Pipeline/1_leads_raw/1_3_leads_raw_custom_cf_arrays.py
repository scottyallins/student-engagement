# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_3_leads_raw_custom_cf_arrays
from pyspark.sql.types import *

df_custom_cf_arrays = spark.createDataFrame(
    [],
    schema="""
        bronze_insert_date TIMESTAMP,
        lead_id STRING,
        cf_key STRING,
        value STRING
    """
)

table_name = "crm_ingestion.silver.leads_raw_custom_cf_arrays"

df_custom_cf_arrays.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
