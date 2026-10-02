# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 6. PIPELINE - calendly_scheduled_events
# MAGIC
# MAGIC Bronze → Silver pipeline for `calendly_scheduled_events`. Simple flat structure — no nested arrays.
# MAGIC
# MAGIC **PK**: `event_uri` | **Source**: `crm_ingestion.bronze.calendly_scheduled_events`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('calendly_scheduled_events')