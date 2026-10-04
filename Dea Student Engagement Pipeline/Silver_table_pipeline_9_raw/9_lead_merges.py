# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 9. PIPELINE - lead_merges
# MAGIC
# MAGIC Bronze → Silver pipeline for `lead_merges`. Clean JSON, no nesting.
# MAGIC
# MAGIC **Source**: `crm_ingestion.bronze.lead_merges`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('lead_merges')