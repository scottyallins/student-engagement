# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_7_2_leads_raw_custom_HADES_TYPE
# ============================================================================
# CHILD TABLE — 1_7_2_leads_raw_custom_HADES_TYPE
# Extracts custom["HADES TYPE"] array from leads_raw_custom
# Lineage: leads_raw → custom → HADES TYPE
# ============================================================================

from pyspark.sql.functions import col, explode_outer

print("\n" + "="*80)
print(f"BUILDING CHILD TABLE — 1_7_2_leads_raw_custom_HADES_TYPE")
print("="*80)

df_parent = spark.table("crm_ingestion.silver.leads_raw_custom")

# Check if the column exists (auto-created by process_table as leads_raw_custom_HADES_TYPE)
table_name = f"crm_ingestion.silver.leads_raw_custom_hades_type"

try:
    cnt = spark.table(table_name).count()
    print(f"✅ Table exists: {table_name} ({cnt:,} rows)")
    spark.table(table_name).printSchema()
except Exception as e:
    print(f"⚠️ Table not found: {table_name}")
    print(f"   This table is auto-created by process_table() in the parent notebook")

