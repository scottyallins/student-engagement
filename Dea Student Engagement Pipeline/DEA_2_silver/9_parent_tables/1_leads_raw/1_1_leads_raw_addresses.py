# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,1_1_leads_raw_addresses
# ===========================================================
# CHILD TABLE 1 — leads_raw_addresses (ENGINE VERSION)
# ===========================================================

df_parent = spark.table("crm_ingestion.silver.leads_raw")

# Confirm array exists
array_cols, struct_cols = get_array_and_struct_columns(df_parent)

if "addresses" not in array_cols:
    raise Exception("❌ 'addresses' array not found in parent table.")

# Explode addresses
df_exploded = df_parent.select(
    col("id").alias("lead_id"),
    col("bronze_insert_date"),
    explode_outer(col("addresses")).alias("address")
)


# Flatten struct fields inside address
elem_type = df_exploded.schema["address"].dataType

df_addresses = df_exploded.select(
    "lead_id",
    "bronze_insert_date",
    *[col(f"address.`{f.name}`").alias(f.name) for f in elem_type.fields]
)

# Sanitize column names
def sanitize(col_name):
    return (col_name.replace(" ", "_")
                   .replace(".", "_")
                   .replace(",", "")
                   .replace(";", "")
                   .replace("{", "")
                   .replace("}", "")
                   .replace("(", "")
                   .replace(")", "")
                   .replace("\n", "")
                   .replace("\t", "")
                   .replace("=", "_")
                   .replace("-", "_")
                   .replace("?", "")
                   .replace("&", "_"))

for old in df_addresses.columns:
    new = sanitize(old)
    if old != new:
        df_addresses = df_addresses.withColumnRenamed(old, new)

# Write child table
table_name = "crm_ingestion.silver.leads_raw_addresses"

df_addresses.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✅ Child table created: {table_name}")
print(f"🔢 Row count: {spark.table(table_name).count():,}")

spark.table(table_name).printSchema()
display(spark.table(table_name).limit(10))
