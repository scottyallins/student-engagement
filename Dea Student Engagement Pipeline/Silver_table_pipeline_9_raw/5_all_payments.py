# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 5. PIPELINE - all_payments
# MAGIC
# MAGIC Bronze → Silver pipeline for `all_payments`. Simple flat structure — 5 fields: PAYMENT_DATE, CUSTOMER_EMAIL, PAYMENT_STATUS, AMOUNT_RECEIVED, PAYMENT_GATEWAY.
# MAGIC
# MAGIC **PK**: `customer_email + payment_date` | **Source**: `crm_ingestion.bronze.all_payments`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('all_payments')