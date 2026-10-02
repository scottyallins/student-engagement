# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_7_1_leads_raw_custom_arrays
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1️⃣ Load Bronze
df_raw = spark.table("crm_ingestion.bronze.leads_raw")

# 2️⃣ Parse raw_data JSON into a struct
df_parsed = df_raw.withColumn(
    "raw_json",
    from_json(col("raw_data"), MapType(StringType(), StringType()))
)

# 3️⃣ Parse the custom object (which is itself JSON)
df_parsed = df_parsed.withColumn(
    "custom",
    from_json(col("raw_json")["custom"], MapType(StringType(), ArrayType(StringType())))
)

# 4️⃣ Extract lead_id from raw_json
df_parsed = df_parsed.withColumn(
    "lead_id",
    col("raw_json")["id"]
)

# 5️⃣ The 5 custom array keys
custom_array_keys = [
    "Add-ons",
    "HADES TYPE",
    "Lead Source",
    "Objections Faced?",
    "Reactivation Campaign"
]

df_list = []

# 6️⃣ Extract each array
for key in custom_array_keys:
    df_exploded = (
        df_parsed
            .withColumn("value", explode_outer(col("custom")[key]))
            .select(
                col("insert_date").alias("bronze_insert_date"),
                col("lead_id"),
                lit(key).alias("custom_key"),
                col("value")
            )
    )
    df_list.append(df_exploded)

# 7️⃣ Union all arrays
df_custom_arrays = df_list[0]
for df in df_list[1:]:
    df_custom_arrays = df_custom_arrays.unionByName(df)

# 8️⃣ Write Silver table
table_name = "crm_ingestion.silver.leads_raw_custom_arrays"

df_custom_arrays.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

# 9️⃣ Verification prints
print(f"\n✅ Table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

print("\n📘 SCHEMA:")
spark.table(table_name).printSchema()

print("\n📊 SAMPLE ROWS (3):")
display(spark.table(table_name).limit(3))
df = spark.table("crm_ingestion.silver.leads_raw_custom_arrays")

print("\n📘 DISTINCT CUSTOM KEYS:")
for k in df.select("custom_key").distinct().orderBy("custom_key").collect():
    print("-", k["custom_key"])
