# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 4. PIPELINE - custom_activites_raw
# MAGIC
# MAGIC Bronze → Silver pipeline for `custom_activites_raw`. **4 tables**: parent + editable_with_roles + fields (+ fields_editable_with_roles grandchild).
# MAGIC
# MAGIC **PK**: `id` | **Source**: `crm_ingestion.bronze.custom_activites_raw`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('custom_activites_raw')