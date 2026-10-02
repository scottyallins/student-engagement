# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 7. PIPELINE - student_sentiment
# MAGIC
# MAGIC Bronze → Silver pipeline for `student_sentiment`. Clean JSON, no nesting.
# MAGIC
# MAGIC **Source**: `crm_ingestion.bronze.student_sentiment`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('student_sentiment')