# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 8. PIPELINE - mdl_users_raw
# MAGIC
# MAGIC Bronze → Silver pipeline for `mdl_users_raw`. Clean JSON, no nesting.
# MAGIC
# MAGIC **Source**: `crm_ingestion.bronze.mdl_users_raw`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('mdl_users_raw')