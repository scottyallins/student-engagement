# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
jdbc_url = ...
connection_properties = ...

# COMMAND ----------

# DBTITLE 1,CONFIGURATION
# ============================================================================
# BRONZE LAYER - CONNECTION CONFIGURATION
# ============================================================================
# PostgreSQL source database connection
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Connection details
jdbc_host = "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com"
jdbc_port = "5432"
jdbc_database = "dea_analytics_dev"
jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_database}"

connection_properties = {
    "user": "student_user",
    "password": "DataEngineer12345",
    "driver": "org.postgresql.Driver"
}

# Unity Catalog configuration
CATALOG = "crm_ingestion"
BRONZE_SCHEMA = "bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

print("✅ Connection Configuration Loaded")
print(f"   Source: {jdbc_host}/{jdbc_database}")
print(f"   Target: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

from pyspark.sql import DataFrame

bronze_tables = [
    "all_payments",
    "calendly_scheduled_events",
    "close_crm_users_raw",
    "custom_activites_raw",
    "lead_activites_raw",
    "lead_merges",
    "leads_raw",
    "mdl_users_raw",
    "student_sentiment"
]

def load_postgres_table(source_table: str, target_table: str):
    query = f""" (
        SELECT *
        FROM {source_table}
        WHERE DATE(INSERT_DATE) = CURRENT_DATE
        ) t
    """
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", "student_user")
        .option("password", "DataEngineer12345")
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    (
        df.write
        .format("delta")
        .mode("append") 
        .saveAsTable(target_table)
    )
    print(f"Loaded {source_table} to {target_table}")

for t in bronze_tables:
    load_postgres_table(
        source_table=f"raw.{t}",
        target_table=f"crm_ingestion.bronze.{t}"
    )