# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2b_lead_activites_raw_custom_cf
# ==============================================================================
# EXTRACT CUSTOM.CF_* FIELDS (CDC-BASED SILENT OPERATOR)
# ==============================================================================
# Extracts custom.cf_* fields into normalized table
# Operates silently - only processes NEW/UPDATED records via CDC watermark
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "crm_ingestion"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.lead_activites_raw"
custom_cf_table = f"{CATALOG}.{SILVER_SCHEMA}.lead_activites_raw_custom_cf"

# Get CDC watermark (only process new/updated records)
def get_last_watermark(target_table):
    try:
        result = spark.sql(f"""
            SELECT MAX(insert_date) as max_insert_date
            FROM {target_table}
        """).collect()[0]
        return result['max_insert_date']
    except Exception:
        return None

last_watermark = get_last_watermark(custom_cf_table)

# Filter bronze table for new/updated records only
if last_watermark:
    df_bronze = spark.table(bronze_table).filter(F.col("insert_date") > last_watermark)
else:
    df_bronze = spark.table(bronze_table)

# Parse activities as map and extract custom.cf_* fields
df_activities_map = df_bronze.select(
    F.col("insert_date"),
    F.explode(
        F.from_json(
            F.col("raw_data"),
            StructType([
                StructField("data", ArrayType(
                    MapType(StringType(), StringType())
                ))
            ])
        ).data
    ).alias("activity_map")
)

# Extract activity_id and explode fields
df_exploded = df_activities_map.select(
    F.col("insert_date"),
    F.col("activity_map")["id"].alias("activity_id"),
    F.explode(F.col("activity_map")).alias("field_name", "field_value")
)

# Filter for custom.cf_* fields only
df_custom_cf = df_exploded.filter(
    F.col("field_name").startswith("custom.cf_")
).select(
    F.col("activity_id"),
    F.col("field_name").alias("custom_field_name"),
    F.col("field_value").alias("custom_field_value"),
    F.col("insert_date")
)

# Write to table (append mode for CDC)
if df_custom_cf.count() > 0:
    df_custom_cf.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(custom_cf_table)
