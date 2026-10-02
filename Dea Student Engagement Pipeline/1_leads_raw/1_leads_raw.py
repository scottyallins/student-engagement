# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 1. PIPELINE - leads_raw
# MAGIC
# MAGIC Bronze → Silver pipeline for `leads_raw`. **18 tables**: parent + addresses, contacts (emails, phones, urls, integration_links), custom_cf_arrays, integration_links, opportunities (attachments, integration_links), tasks, custom (Add-ons, HADES_TYPE, Lead_Source, Objections_Faced, Reactivation_Campaign).
# MAGIC
# MAGIC **PK**: `id` | **Source**: `crm_ingestion.bronze.leads_raw`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('leads_raw')