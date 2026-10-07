# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 3. PIPELINE - close_crm_users_raw
# MAGIC
# MAGIC Bronze → Silver pipeline for `close_crm_users_raw`. **2 tables**: parent + organizations.
# MAGIC
# MAGIC **PK**: `id` | **Source**: `crm_ingestion.bronze.close_crm_users_raw`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('close_crm_users_raw')