# Databricks notebook source
# DBTITLE 1,DROP ALL TABLES — Start Fresh
# MAGIC %skip
# MAGIC # ============================================================================
# MAGIC # DROP ALL TABLES — Start Fresh
# MAGIC # Drops every table in crm_ingestion.bronze and crm_ingestion.silver
# MAGIC # Run this cell ONCE before re-ingesting from scratch
# MAGIC # ============================================================================
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC schemas_to_clear = [
# MAGIC     "crm_ingestion.bronze",
# MAGIC     "crm_ingestion.silver",
# MAGIC     "crm_ingestion.gold",
# MAGIC ]
# MAGIC
# MAGIC for schema_fqn in schemas_to_clear:
# MAGIC     try:
# MAGIC         tables = spark.sql(f"SHOW TABLES IN {schema_fqn}").collect()
# MAGIC     except Exception as e:
# MAGIC         print(f"SKIP {schema_fqn} (does not exist): {str(e)[:80]}")
# MAGIC         continue
# MAGIC
# MAGIC     if not tables:
# MAGIC         print(f"{schema_fqn} — no tables to drop")
# MAGIC         continue
# MAGIC
# MAGIC     for row in tables:
# MAGIC         tbl = row["tableName"]
# MAGIC         fqn = f"{schema_fqn}.{tbl}"
# MAGIC         try:
# MAGIC             spark.sql(f"DROP TABLE IF EXISTS {fqn}")
# MAGIC             print(f"  DROPPED {fqn}")
# MAGIC         except Exception as e:
# MAGIC             print(f"  FAIL {fqn}: {str(e)[:80]}")
# MAGIC
# MAGIC print("\n✅ All tables dropped. Ready for fresh ingestion.")

# COMMAND ----------

# DBTITLE 1,CONFIGURATION
# ============================================================================
# BRONZE LAYER - CONNECTION CONFIGURATION
# ============================================================================
# PostgreSQL source database connection
# ============================================================================

import json
import re
import time
import uuid
import pandas as pd
import numpy as np 
from datetime import datetime
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

print("✅ Connection Configuration Loaded")
print(f"   Source: {jdbc_host}/{jdbc_database}")
print(f"   Target: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,CDC (Silver layer)
# ============================================================================
# CDC belongs in SILVER, not Bronze.
# ============================================================================
# Bronze always does a FULL LOAD (overwrite) from Postgres every run.
# No watermarks, no MERGE, no CDC logic here.
#
# Silver's job:
#   1. First silver run  → Process ALL bronze data (initial load)
#   2. Subsequent runs   → Read watermark → query bronze for changed rows → MERGE
#   3. Batch loading may be needed in Silver's initial load (after array explosion)
#   4. CDC updates are small (only changed rows) → no batch needed
#
# This cell is intentionally empty. CDC functions will be defined in the
# Silver notebook when it is built.
# ============================================================================

print("✅ CDC lives in Silver — Bronze does full load only")

# COMMAND ----------

# DBTITLE 1,LOAD size and Batch
# ============================================================================
# LOAD TABLE — Size-based full load from Postgres to Bronze Delta
# ============================================================================
# Bronze always does a FULL LOAD (overwrite). No CDC, no MERGE.
# CDC and MERGE belong in the Silver layer.
#
# Strategy:
#   1. Estimate source size: COUNT + 100-row sample for avg bytes/row
#   2. < 256MB  → SINGLE_READ (spark.read.jdbc, no batching)
#   3. >= 256MB → PARTITIONED (Spark native JDBC partitioning on insert_date)
#   4. Always overwrite (full load every run)
# ============================================================================

import time
from builtins import round, max, min  # override pyspark functions (imported by cell 2 star import)

TARGET_BATCH_MB = 256

def estimate_source_size(jdbc_url, props, table_name):
    """Quick row count + avg row size from a 100-row sample."""
    cnt_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT COUNT(*) AS cnt FROM {table_name}) _c",
        properties=props
    )
    row_count = cnt_df.collect()[0]["cnt"]

    if row_count == 0:
        return {"rows": 0, "avg_bytes": 0, "total_mb": 0.0}

    try:
        sample_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"(SELECT raw_data::text FROM {table_name} LIMIT 100) _s",
            properties=props
        )
        avg_bytes = sample_df.selectExpr("AVG(LENGTH(raw_data)) as avg_len").collect()[0]["avg_len"] or 500
    except Exception:
        avg_bytes = 500

    total_mb = (avg_bytes * row_count) / (1024 * 1024)
    return {"rows": row_count, "avg_bytes": int(avg_bytes), "total_mb": round(total_mb, 1)}


def load_table(table_name, target_table):
    """Full load from Postgres to Bronze Delta. Always overwrite."""
    start = time.time()
    print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

    # Step 1: Estimate source size
    size = estimate_source_size(jdbc_url, connection_properties, table_name)
    if size["rows"] == 0:
        print("⏭️ SKIP: No data")
        return 0
    print(f"  source: {size['rows']:,} rows | ~{size['avg_bytes']:,} bytes/row | ~{size['total_mb']:.1f}MB total")

    # Step 2: Pick strategy
    if size["total_mb"] < TARGET_BATCH_MB:
        print(f"  strategy: SINGLE_READ")
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    else:
        num_partitions = min(16, max(2, int(size["total_mb"] / TARGET_BATCH_MB) + 1))
        bounds_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"(SELECT MIN(insert_date) as lo, MAX(insert_date) as hi FROM {table_name}) _b",
            properties=connection_properties
        )
        bounds = bounds_df.collect()[0]
        lo, hi = bounds["lo"], bounds["hi"]
        if lo and hi and lo != hi:
            print(f"  strategy: PARTITIONED ({num_partitions} partitions on insert_date)")
            df = spark.read.jdbc(
                url=jdbc_url, table=table_name, properties=connection_properties,
                column="insert_date", lowerBound=lo.isoformat(), upperBound=hi.isoformat(),
                numPartitions=num_partitions
            )
        else:
            print(f"  strategy: SINGLE_READ (partition fallback)")
            df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    # Step 3: Write (always full load — overwrite)
    row_count = df.count()
    elapsed = time.time() - start

    if row_count == 0:
        print("⏭️ SKIP: No data after read")
        return 0

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    print(f"✅ LOADED: {row_count:,} rows in {elapsed:.1f}s")
    return row_count

print(f"✅ load_table() loaded | Target batch: {TARGET_BATCH_MB}MB")
print(f"   Usage: load_table('raw.table', 'catalog.schema.table')")

# COMMAND ----------

# DBTITLE 1,1_all_payments
load_table("raw.all_payments", "crm_ingestion.bronze.all_payments")

# COMMAND ----------

# DBTITLE 1,2_calendly_scheduled_events
load_table("raw.calendly_scheduled_events", "crm_ingestion.bronze.calendly_scheduled_events")

# COMMAND ----------

# DBTITLE 1,3_close_crm_users_raw
load_table("raw.close_crm_users_raw", "crm_ingestion.bronze.close_crm_users_raw")

# COMMAND ----------

# DBTITLE 1,4_custom_activites_raw
load_table("raw.custom_activites_raw", "crm_ingestion.bronze.custom_activites_raw")

# COMMAND ----------

# DBTITLE 1,5_lead_activites_raw
load_table("raw.lead_activites_raw", "crm_ingestion.bronze.lead_activites_raw")

# COMMAND ----------

# DBTITLE 1,6_lead_merges
load_table("raw.lead_merges", "crm_ingestion.bronze.lead_merges")

# COMMAND ----------

# DBTITLE 1,7_leads_raw
load_table("raw.leads_raw", "crm_ingestion.bronze.leads_raw")

# COMMAND ----------

# DBTITLE 1,8_mdl_users_raw
load_table("raw.mdl_users_raw", "crm_ingestion.bronze.mdl_users_raw")

# COMMAND ----------

# DBTITLE 1,9_student_sentiment
load_table("raw.student_sentiment", "crm_ingestion.bronze.student_sentiment")

# COMMAND ----------

# DBTITLE 1,1 all_payments


# -----------------------------------------------------------
# Ingestion Start
# -----------------------------------------------------------
start_time = time.time()
table_name      = "raw.all_payments"
target_table    = "crm_ingestion.bronze.all_payments"

print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

last_watermark = get_last_watermark(target_table)



    # -----------------------------------------------------------
    # ⭐ BUILD CDC QUERY - Only pull changed records
    # -----------------------------------------------------------
    # 🧠 WHY "OR" BETWEEN TWO CONDITIONS (not "AND")?
    # We want records that meet EITHER condition:
    #   1. insert_date > max_insert  → New records added to Postgres
    #   2. date_updated > max_update → Existing records modified
    #
    # 🔥 WHY WRAP IN SUBQUERY "(...) as cdc"?
    # Spark's JDBC reader requires table parameter to be a valid
    # SQL statement usable in FROM clause. Wrapping makes it valid.
    #
    # ⭐ DYNAMIC PATTERN = This query adapts to ANY table schema!
    # No hardcoded column names (except watermark fields).
    # If Postgres adds 50 new columns, this query still works.
    query = (
        f"(SELECT * FROM {table_name} "
        f"WHERE insert_date > '{max_insert}'::timestamp "
        f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
    )

    # 🔥 Execute JDBC query against Postgres
    # Only pulls changed records (efficient!)
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    row_count = df.count()  # How many changed records?

    if row_count > 0:  # ⭐ Changes detected = proceed with MERGE
        
        # 🧠 Create temp view for MERGE statement
        # Temp view = in-memory reference for SQL operations
        df.createOrReplaceTempView("temp_all_payments")

        # -----------------------------------------------------------
        # 🔥 DELTA MERGE - Idempotent Upsert (Industry Best Practice)
        # -----------------------------------------------------------
        # 🧠 AHA! — WHY MERGE (not INSERT)?
        # Running cell twice = same records pulled again
        # INSERT would create duplicates
        # MERGE = UPDATE if exists, INSERT if new (no duplicates!)
        #
        # ⭐ MATCH CONDITION:
        # GET_JSON_OBJECT extracts 'id' from raw JSON string
        # Compares source id to target id
        #
        # 🔥 WHEN MATCHED = Record already exists → UPDATE
        # WHEN NOT MATCHED = New record → INSERT
        #
        # This is ACID-compliant (Atomic, Consistent, Isolated, Durable)
        # Either entire MERGE succeeds or entire MERGE fails (no partial writes)
        spark.sql(f"""
            MERGE INTO {target_table} t
            USING temp_all_payments s
            ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
            WHEN MATCHED THEN UPDATE SET
                t.raw_data    = s.raw_data,
                t.insert_date = s.insert_date
            WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
            VALUES (s.raw_data, s.insert_date)
        """)

        # ✅ Success! Report metrics to user
        print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

    else:
        # 🧠 No changes since last run = nothing to do
        # This is NORMAL and EXPECTED (not an error)
        # Prevents unnecessary MERGE operations
        print("⏭️ SKIP: No changes")

# -----------------------------------------------------------
# ⭐🔥🧠 FULL LOAD - Initial Table Creation
# -----------------------------------------------------------
# 🧠 AHA! — This path runs ONLY on first execution!
# Once table exists, all subsequent runs use CDC MODE above.
# This is the "bootstrap" that creates the Bronze table.
#
# 🔥 WHY SEPARATE FULL LOAD PATH (not always MERGE)?
# Can't MERGE into a table that doesn't exist!
# MERGE requires target table to already be there.
# FULL LOAD = CREATE table, CDC MODE = UPDATE table.
# -----------------------------------------------------------
else:
    print("🆕 FULL LOAD")  # First run indicator
    
    # ⭐ Read ENTIRE table from Postgres
    # No WHERE clause = every single row
    # This happens once, then never again (CDC takes over)
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    # -----------------------------------------------------------
    # 🔥 WRITE TO DELTA LAKE - Create Bronze table
    # -----------------------------------------------------------
    # 🧠 AHA! — Why these specific options?
    #
    # format("delta") = Use Delta Lake (not Parquet)
    #   → Enables ACID transactions, time travel, MERGE
    #   → Industry best practice for data lakes
    #
    # mode("overwrite") = Replace table if it exists
    #   → Safe on first run (table doesn't exist yet)
    #   → If re-running FULL LOAD, replaces entire table
    #
    # option("mergeSchema", "true") = DYNAMIC SCHEMA EVOLUTION
    #   → 🔥 THIS IS THE BULLETPROOF MAGIC!
    #   → If Postgres adds new columns, Delta automatically adapts
    #   → Existing columns preserved, new columns added
    #   → Pipeline NEVER breaks due to schema changes
    #
    # saveAsTable() = Create Unity Catalog managed table
    #   → Registers in metastore (queryable via SQL)
    #   → Governed by Unity Catalog permissions
    #   → Supports time travel, optimization, vacuum
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    # ✅ Success! Report metrics to user
    print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")


# COMMAND ----------

# DBTITLE 1,2_calendly_scheduled_events
# # ============================================================================
# # ⭐🔥🧠 calendly_scheduled_events - Meeting Bookings
# # ============================================================================
# # 🧠 PATTERN: Follows MASTER CDC PATTERN from all_payments cell (Cell 5)
# #
# # ⭐ WHAT MAKES THIS TABLE UNIQUE:
# # Tracks scheduled meetings/calls from Calendly integration.
# # Key for understanding lead engagement and sales pipeline velocity.
# #
# # 🔥 WHY IT'S NEEDED:
# # "Booked meeting" is often the first high-intent signal in B2B sales.
# # Time from lead creation to first meeting = critical conversion metric.
# # No shows vs completed meetings = lead quality indicator.
# # ============================================================================

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name      = "raw.calendly_scheduled_events"
# target_table    = "crm_ingestion.bronze.calendly_scheduled_events"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     query = (
#         f"(SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
#     )

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
#     row_count = df.count()

#     if row_count > 0:
#         df.createOrReplaceTempView("temp_calendly_scheduled_events")

#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_calendly_scheduled_events s
#             ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)

#         print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

#     else:
#         print("⏭️ SKIP: No changes")

# # -----------------------------------------------------------
# # FULL LOAD
# # -----------------------------------------------------------
# else:
#     print("🆕 FULL LOAD")
#     df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(target_table)

#     print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")


# COMMAND ----------

# DBTITLE 1,3_close_crm_users_raw
# # ============================================================================
# # ⭐🔥🧠 close_crm_users_raw - CRM User Data (BATCHED INGESTION)
# # ============================================================================
# # 🧠 PATTERN: Follows MASTER CDC PATTERN BUT with BATCHED READS!
# # 🔥 This cell demonstrates MEMORY-SAFE ingestion for large tables.
# #
# # ⭐ WHAT MAKES THIS CELL UNIQUE - BATCHED JDBC:
# # Instead of reading entire result set into memory (crash risk),
# # reads 8,000 rows at a time, unions incrementally.


# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(target_table):
#     """
#     Get the last CDC watermark from Bronze table.
#     Tracks MAX of BOTH insert_date AND date_updated from JSON.
#     """
#     try:
#         result = spark.sql(f"""
#             SELECT 
#                 MAX(insert_date) as max_insert_date,
#                 MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_data, '$.date_updated'))) as max_date_updated
#             FROM {target_table}
#         """).collect()[0]

#         return {
#             'insert_date': result['max_insert_date'],
#             'date_updated': result['max_date_updated']
#         }
#     except Exception:
#         return None  # Table doesn't exist yet → full load

# print("✅ Helper function loaded: get_last_watermark")

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name   = "raw.close_crm_users_raw"
# target_table = "crm_ingestion.bronze.close_crm_users_raw"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE — Build base query
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     base_query = (
#         f"SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp "
#     )

# else:
#     print("🆕 FULL LOAD")
#     base_query = f"SELECT * FROM {table_name}"

# # -----------------------------------------------------------
# # 🔥 BATCHED JDBC INGESTION - Memory-Safe Pattern for Large Tables
# # -----------------------------------------------------------
# # 🧠 AHA! — WHY NOT JUST JDBC READ (like all_payments)?
# # Simple spark.read.jdbc() loads ENTIRE result set into driver memory.
# # For 100K+ row tables = OutOfMemoryError.
# # Batching = read in chunks, union incrementally = no memory spike.
# #
# # ⭐ HOW THIS WORKS:
# # 1. Set batch_size = 8000 rows (tested optimal size)
# # 2. Use LIMIT/OFFSET to read chunks (Postgres pagination)
# # 3. Append each batch to list
# # 4. Union all batches at end
# # 5. Break when batch is empty (no more rows)
# #
# # 🔥 WHY 8000 ROWS (not 1000 or 100K)?
# # Too small = too many round trips to Postgres (slow)
# # Too large = memory pressure (defeats purpose)
# # 8000 = sweet spot tested across multiple tables
# # -----------------------------------------------------------
# batch_size = 8000  # ⭐ Tested optimal batch size
# offset = 0         # 🔥 Starting position (increments by batch_size)
# dfs = []           # 🧠 Accumulator list for batch DataFrames

# # 🔥 Pagination loop - keeps reading until no more rows
# while True:
#     # ⭐ Build paginated query with LIMIT/OFFSET
#     # ORDER BY insert_date = consistent ordering across batches
#     batch_query = f"({base_query} ORDER BY insert_date LIMIT {batch_size} OFFSET {offset}) AS batch"
    
#     # 🧠 Execute JDBC read for this batch only
#     batch_df = spark.read.jdbc(url=jdbc_url, table=batch_query, properties=connection_properties)

#     # 🔥 Check if batch is empty (reached end of data)
#     count = batch_df.count()
#     if count == 0:
#         break  # Exit loop - all data loaded

#     # ⭐ Append batch to list and move offset forward
#     dfs.append(batch_df)
#     offset += batch_size
#     print(f"⭐ Loaded batch {len(dfs)}: {count:,} rows")  # Progress indicator

# # -----------------------------------------------------------
# # 🔥 COMBINE BATCHES - Union all chunks into single DataFrame
# # -----------------------------------------------------------
# if len(dfs) == 0:
#     # 🧠 No changes detected (CDC returned 0 rows)
#     print("⏭️ SKIP: No changes")
#     df = None  # Signal to skip MERGE below
# else:
#     # ⭐ Start with first batch, union remaining batches iteratively
#     # unionByName = matches columns by name (safe for schema evolution)
#     df = dfs[0]
#     for d in dfs[1:]:
#         df = df.unionByName(d)

#     print(f"⭐ Total rows loaded: {df.count():,}")  # Final count across all batches

# # -----------------------------------------------------------
# # WRITE TO DELTA TABLE
# # -----------------------------------------------------------
# if df is not None:
#     # FULL LOAD: Create table
#     if last_watermark is None:
#         df.write \
#             .format("delta") \
#             .mode("overwrite") \
#             .option("mergeSchema", "true") \
#             .saveAsTable(target_table)
#         print(f"✅ TABLE CREATED: {df.count():,} rows in {time.time() - start_time:.1f}s")
    
#     # CDC MODE: Merge into existing table
#     else:
#         df.createOrReplaceTempView("temp_close_crm_users_raw")

#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_close_crm_users_raw s
#             ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)
#         print(f"✅ MERGE complete in {time.time() - start_time:.1f}s")

# COMMAND ----------

# DBTITLE 1,4_custom_activites_raw

# # ============================================================================
# # ⭐🔥🧠 custom_activites_raw - Custom Activity Definitions
# # ============================================================================
# # 🧠 PATTERN: Follows MASTER CDC PATTERN (Cell 5)
# # ⭐ PURPOSE: Custom activity types defined in CRM (beyond standard calls/emails)
# # 🔥 USED BY: Silver layer to enrich activity classifications
# # ============================================================================

# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(target_table):
#     """
#     Get the last CDC watermark from Bronze table.
#     Tracks MAX of BOTH insert_date AND date_updated from JSON.
#     """
#     try:
#         result = spark.sql(f"""
#             SELECT 
#                 MAX(insert_date) as max_insert_date,
#                 MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_data, '$.date_updated'))) as max_date_updated
#             FROM {target_table}
#         """).collect()[0]

#         return {
#             'insert_date': result['max_insert_date'],
#             'date_updated': result['max_date_updated']
#         }
#     except Exception:
#         return None  # Table doesn't exist yet → full load

# print("✅ Helper function loaded: get_last_watermark")

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name   = "raw.custom_activites_raw"
# target_table = "crm_ingestion.bronze.custom_activites_raw"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     query = (
#         f"(SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
#     )

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
#     row_count = df.count()

#     if row_count > 0:
#         df.createOrReplaceTempView("temp_custom_activites_raw")

#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_custom_activites_raw s
#             ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)

#         print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

#     else:
#         print("⏭️ SKIP: No changes")

# # -----------------------------------------------------------
# # FULL LOAD
# # -----------------------------------------------------------
# else:
#     print("🆕 FULL LOAD")
#     df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(target_table)

#     print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")


# COMMAND ----------

# DBTITLE 1,5_lead_activites_raw

# # ============================================================================
# # ⭐🔥🧠 lead_activites_raw - Lead Activity Stream (BATCHED)
# # ============================================================================
# # 🧠 PATTERN: MASTER CDC + BATCHED INGESTION (Cell 8 pattern)
# # ⭐ PURPOSE: Every interaction with every lead (calls, emails, meetings, etc.)
# # 🔥 CRITICAL TABLE: Drives engagement scoring, rep performance, conversion analysis
# # 🧠 WHY BATCHED: Often 100K+ activities, needs memory-safe ingestion
# # ============================================================================

# import json
# import re
# import time
# import uuid
# import pandas as pd
# import numpy as np
# from datetime import datetime
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # -----------------------------------------------------------
# # JDBC Connection Details
# # -----------------------------------------------------------
# jdbc_host = "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com"
# jdbc_port = "5432"
# jdbc_database = "dea_analytics_dev"
# jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_database}"

# connection_properties = {
#     "user": "student_user",
#     "password": "DataEngineer12345",
#     "driver": "org.postgresql.Driver"
# }

# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(target_table):
#     try:
#         result = spark.sql(f"""
#             SELECT 
#                 MAX(insert_date) as max_insert_date,
#                 MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_data, '$.date_updated'))) as max_date_updated
#             FROM {target_table}
#         """).collect()[0]

#         return {
#             'insert_date': result['max_insert_date'],
#             'date_updated': result['max_date_updated']
#         }
#     except Exception:
#         return None  # Table doesn't exist yet → full load

# print("✅ Helper function loaded: get_last_watermark")

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name   = "raw.lead_activites_raw"
# target_table = "crm_ingestion.bronze.lead_activites_raw"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE — Build base query
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     base_query = (
#         f"SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp "
#     )

# else:
#     print("🆕 FULL LOAD")
#     base_query = f"SELECT * FROM {table_name}"

# # -----------------------------------------------------------
# # BATCHED JDBC INGESTION (Memory Safe)
# # -----------------------------------------------------------
# batch_size = 8000
# offset = 0
# dfs = []

# while True:
#     batch_query = f"({base_query} ORDER BY insert_date LIMIT {batch_size} OFFSET {offset}) AS batch"
#     batch_df = spark.read.jdbc(url=jdbc_url, table=batch_query, properties=connection_properties)

#     count = batch_df.count()
#     if count == 0:
#         break

#     dfs.append(batch_df)
#     offset += batch_size
#     print(f"⭐ Loaded batch {len(dfs)}: {count:,} rows")

# # Combine batches
# if len(dfs) == 0:
#     print("⏭️ SKIP: No changes")
#     df = None
# else:
#     df = dfs[0]
#     for d in dfs[1:]:
#         df = df.unionByName(d)

#     print(f"⭐ Total rows loaded: {df.count():,}")

# # -----------------------------------------------------------
# # WRITE TO DELTA TABLE
# # -----------------------------------------------------------
# if df is not None:
#     # FULL LOAD: Create table
#     if last_watermark is None:
#         df.write \
#             .format("delta") \
#             .mode("overwrite") \
#             .option("mergeSchema", "true") \
#             .saveAsTable(target_table)
#         print(f"✅ TABLE CREATED: {df.count():,} rows in {time.time() - start_time:.1f}s")
    
#     # CDC MODE: Merge into existing table
#     else:
#         df.createOrReplaceTempView("temp_lead_activites_raw")

#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_lead_activites_raw s
#             ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)
#         print(f"✅ MERGE complete in {time.time() - start_time:.1f}s")

# print("================================================================================")
# print("📘 SCHEMA — Bronze: lead_activites_raw")
# print("================================================================================")      


# COMMAND ----------

# DBTITLE 1,6_lead_merges
# # ============================================================================
# # ⭐🔥🧠 lead_merges - Lead Merge History Tracking
# # ============================================================================
# # 🧠 PATTERN: Follows MASTER CDC PATTERN from all_payments cell (Cell 5)
# # See Cell 5 for complete inline documentation of this pattern.
# #
# # ⭐ WHAT MAKES THIS TABLE UNIQUE:
# # Tracks when leads get merged in the CRM system.
# # Critical for Silver layer canonical mappings (resolving merged lead IDs).
# #
# # 🔥 WHY IT'S NEEDED:
# # When Lead A merges into Lead B, we need history of both leads.
# # Without this: Data loss, broken relationships, incorrect analytics.
# # With this: Silver layer resolves canonical IDs, maintains full history.
# # ============================================================================


# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(target_table):
#     """
#     Get the last CDC watermark from Bronze table.
#     Tracks MAX of BOTH insert_date AND date_updated from JSON.
#     """
#     try:
#         result = spark.sql(f"""
#             SELECT 
#                 MAX(insert_date) as max_insert_date,
#                 MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_data, '$.date_updated'))) as max_date_updated
#             FROM {target_table}
#         """).collect()[0]
        
#         return {
#             'insert_date': result['max_insert_date'],
#             'date_updated': result['max_date_updated']
#         }
#     except Exception:
#         return None  # Table doesn't exist yet → full load

# print("✅ Helper function loaded: get_last_watermark")

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name      = "raw.lead_merges"
# target_table    = "crm_ingestion.bronze.lead_merges"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     query = (
#         f"(SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
#     )

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
#     row_count = df.count()

#     if row_count > 0:
#         df.createOrReplaceTempView("temp_lead_merges")

#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_lead_merges s
#             ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)

#         print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

#     else:
#         print("⏭️ SKIP: No changes")

# # -----------------------------------------------------------
# # FULL LOAD
# # -----------------------------------------------------------
# else:
#     print("🆕 FULL LOAD")
#     df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(target_table)

#     print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")


# COMMAND ----------

# DBTITLE 1,7_leads_raw
# # ============================================================================
# # ⭐🔥🧠 leads_raw - MAIN CRM TABLE (The Heart of the Pipeline)
# # ============================================================================
# # 🧠 PATTERN: MASTER CDC with COMPOSITE KEY DEDUPLICATION
# # ⭐ THIS IS THE MOST IMPORTANT TABLE IN THE ENTIRE PIPELINE!
# #
# # 🔥 WHY IT'S THE HEART:
# # - Contains ALL lead master data (contacts, opportunities, custom fields)
# # - Deeply nested JSON (arrays within arrays)
# # - Silver layer explodes this into 10+ dimensional tables
# # - Every other table joins back to leads
# #
# # 🧠 UNIQUE PATTERN - COMPOSITE KEY DEDUPE:
# # Uses (id, name, date_updated) instead of just id
# # Prevents duplicate versions when same lead updated multiple times in batch
# # ============================================================================

# import json
# import re
# import time
# import uuid
# import pandas as pd
# import numpy as np 
# from datetime import datetime
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # -----------------------------------------------------------
# # JDBC Connection Details
# # -----------------------------------------------------------
# jdbc_host = "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com"
# jdbc_port = "5432"
# jdbc_database = "dea_analytics_dev"
# jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_database}"

# connection_properties = {
#     "user": "student_user",
#     "password": "DataEngineer12345",
#     "driver": "org.postgresql.Driver"
# }

# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(target_table):
#     try:
#         result = spark.sql(f"""
#             SELECT 
#                 MAX(insert_date) as max_insert_date,
#                 MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_data, '$.date_updated'))) as max_date_updated
#             FROM {target_table}
#         """).collect()[0]
        
#         return {
#             'insert_date': result['max_insert_date'],
#             'date_updated': result['max_date_updated']
#         }
#     except Exception:
#         return None

# print("✅ Helper function loaded: get_last_watermark")

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name      = "raw.leads_raw"
# target_table    = "crm_ingestion.bronze.leads_raw"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     query = (
#         f"(SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
#     )

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
#     row_count = df.count()

#     if row_count > 0:
#         df.createOrReplaceTempView("temp_leads_raw")

#         deduped_df = spark.sql("""
#             SELECT *
#             FROM temp_leads_raw
#             QUALIFY ROW_NUMBER() OVER (
#                 PARTITION BY 
#                     GET_JSON_OBJECT(raw_data, '$.id'),
#                     GET_JSON_OBJECT(raw_data, '$.name'),
#                     GET_JSON_OBJECT(raw_data, '$.date_updated')
#                 ORDER BY insert_date DESC
#             ) = 1
#         """)

#         deduped_df.createOrReplaceTempView("temp_leads_raw_deduped")

#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_leads_raw_deduped s
#             ON GET_JSON_OBJECT(t.raw_data, '$.id') = GET_JSON_OBJECT(s.raw_data, '$.id')
#             AND GET_JSON_OBJECT(t.raw_data, '$.name') = GET_JSON_OBJECT(s.raw_data, '$.name')
#             AND GET_JSON_OBJECT(t.raw_data, '$.date_updated') = GET_JSON_OBJECT(s.raw_data, '$.date_updated')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)

#         print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

#     else:
#         print("⏭️ SKIP: No changes")

# # -----------------------------------------------------------
# # FULL LOAD
# # -----------------------------------------------------------
# else:
#     print("🆕 FULL LOAD")
#     df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(target_table)

#     print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")


# COMMAND ----------

# DBTITLE 1,8_mdl_users_raw
# # ============================================================================
# # ⭐🔥🧠 mdl_users_raw - Moodle/LMS User Data
# # ============================================================================
# # 🧠 PATTERN: MASTER CDC with COMPOSITE KEY (ID + LASTNAME + date_updated)
# # ⭐ PURPOSE: Student/learner data from Learning Management System
# # 🔥 WHY COMPOSITE KEY: LMS allows duplicate names, uses combo for uniqueness
# # 🧠 USED BY: Silver layer to join student engagement with CRM leads
# # ============================================================================

# import json
# import re
# import time
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # -----------------------------------------------------------
# # JDBC Connection Details
# # -----------------------------------------------------------
# jdbc_host = "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com"
# jdbc_port = "5432"
# jdbc_database = "dea_analytics_dev"
# jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_database}"

# connection_properties = {
#     "user": "student_user",
#     "password": "DataEngineer12345",
#     "driver": "org.postgresql.Driver"
# }

# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(table_name):
#     try:
#         df = spark.table(table_name)
#         return df.selectExpr(
#             "max(insert_date) as insert_date",
#             "max(CAST(get_json_object(raw_data, '$.date_updated') AS timestamp)) as date_updated"
#         ).collect()[0]
#     except:
#         return None

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name      = "raw.mdl_users_raw"
# target_table    = "crm_ingestion.bronze.mdl_users_raw"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     query = (
#         f"(SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
#     )

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
#     row_count = df.count()

#     if row_count > 0:

#         # Create temp view
#         df.createOrReplaceTempView("temp_mdl_users_raw")

#         # -----------------------------------------------------------
#         # DEDUPE BLOCK — based on composite key:
#         # ID + LASTNAME + date_updated
#         # -----------------------------------------------------------
#         deduped_df = spark.sql("""
#             SELECT *
#             FROM temp_mdl_users_raw
#             QUALIFY ROW_NUMBER() OVER (
#                 PARTITION BY 
#                     GET_JSON_OBJECT(raw_data, '$.ID'),
#                     GET_JSON_OBJECT(raw_data, '$.LASTNAME'),
#                     GET_JSON_OBJECT(raw_data, '$.date_updated')
#                 ORDER BY insert_date DESC
#             ) = 1
#         """)

#         deduped_df.createOrReplaceTempView("temp_mdl_users_raw_deduped")

#         # -----------------------------------------------------------
#         # MERGE
#         # -----------------------------------------------------------
#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_mdl_users_raw_deduped s
#             ON GET_JSON_OBJECT(t.raw_data, '$.ID') = GET_JSON_OBJECT(s.raw_data, '$.ID')
#             AND GET_JSON_OBJECT(t.raw_data, '$.LASTNAME') = GET_JSON_OBJECT(s.raw_data, '$.LASTNAME')
#             AND GET_JSON_OBJECT(t.raw_data, '$.date_updated') = GET_JSON_OBJECT(s.raw_data, '$.date_updated')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)

#         print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

#     else:
#         print("⏭️ SKIP: No changes")

# # -----------------------------------------------------------
# # FULL LOAD
# # -----------------------------------------------------------
# else:
#     print("🆕 FULL LOAD")
#     df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(target_table)

#     print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")


# COMMAND ----------

# DBTITLE 1,9_student_sentiment
# # ============================================================================
# # ⭐🔥🧠 student_sentiment - Student Feedback & Sentiment Analysis
# # ============================================================================
# # 🧠 PATTERN: MASTER CDC with COMPOSITE KEY (CHANNEL_ID + date_updated)
# # ⭐ PURPOSE: Student satisfaction scores, feedback, NPS data
# # 🔥 CRITICAL FOR: Churn prediction, product improvements, student success metrics
# # 🧠 JOINS TO: Students (mdl_users) and Leads (CRM) for 360-degree view
# # ============================================================================

# import json
# import re
# import time
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # -----------------------------------------------------------
# # JDBC Connection Details
# # -----------------------------------------------------------
# jdbc_host = "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com"
# jdbc_port = "5432"
# jdbc_database = "dea_analytics_dev"
# jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_database}"

# connection_properties = {
#     "user": "student_user",
#     "password": "DataEngineer12345",
#     "driver": "org.postgresql.Driver"
# }

# # -----------------------------------------------------------
# # Helper: Get last watermark from Bronze table
# # -----------------------------------------------------------
# def get_last_watermark(target_table):
#     try:
#         result = spark.sql(f"""
#             SELECT 
#                 MAX(insert_date) as max_insert_date,
#                 MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_data, '$.date_updated'))) as max_date_updated
#             FROM {target_table}
#         """).collect()[0]

#         return {
#             'insert_date': result['max_insert_date'],
#             'date_updated': result['max_date_updated']
#         }
#     except Exception:
#         return None

# print("✅ Helper function loaded: get_last_watermark")

# # -----------------------------------------------------------
# # Ingestion Start
# # -----------------------------------------------------------
# start_time = time.time()
# table_name      = "raw.student_sentiment"
# target_table    = "crm_ingestion.bronze.student_sentiment"

# print(f"\n{'='*80}\n📥 INGESTING: {table_name}\n{'='*80}")

# last_watermark = get_last_watermark(target_table)

# # -----------------------------------------------------------
# # CDC MODE
# # -----------------------------------------------------------
# if last_watermark and (last_watermark["insert_date"] or last_watermark["date_updated"]):

#     max_insert = (
#         last_watermark["insert_date"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["insert_date"] else "1900-01-01"
#     )
#     max_update = (
#         last_watermark["date_updated"].strftime("%Y-%m-%d %H:%M:%S")
#         if last_watermark["date_updated"] else "1900-01-01"
#     )

#     print("🔄 CDC MODE")

#     query = (
#         f"(SELECT * FROM {table_name} "
#         f"WHERE insert_date > '{max_insert}'::timestamp "
#         f"OR (raw_data->>'date_updated')::timestamp > '{max_update}'::timestamp) as cdc"
#     )

#     df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
#     row_count = df.count()

#     if row_count > 0:

#         df.createOrReplaceTempView("temp_student_sentiment")

#         # -----------------------------------------------------------
#         # DEDUPE BLOCK — composite key:
#         # CHANNEL_ID + date_updated
#         # -----------------------------------------------------------
#         deduped_df = spark.sql("""
#             SELECT *
#             FROM temp_student_sentiment
#             QUALIFY ROW_NUMBER() OVER (
#                 PARTITION BY 
#                     GET_JSON_OBJECT(raw_data, '$.CHANNEL_ID'),
#                     GET_JSON_OBJECT(raw_data, '$.date_updated')
#                 ORDER BY insert_date DESC
#             ) = 1
#         """)

#         deduped_df.createOrReplaceTempView("temp_student_sentiment_deduped")

#         # -----------------------------------------------------------
#         # MERGE
#         # -----------------------------------------------------------
#         spark.sql(f"""
#             MERGE INTO {target_table} t
#             USING temp_student_sentiment_deduped s
#             ON GET_JSON_OBJECT(t.raw_data, '$.CHANNEL_ID') = GET_JSON_OBJECT(s.raw_data, '$.CHANNEL_ID')
#             AND GET_JSON_OBJECT(t.raw_data, '$.date_updated') = GET_JSON_OBJECT(s.raw_data, '$.date_updated')
#             WHEN MATCHED THEN UPDATE SET
#                 t.raw_data    = s.raw_data,
#                 t.insert_date = s.insert_date
#             WHEN NOT MATCHED THEN INSERT (raw_data, insert_date)
#             VALUES (s.raw_data, s.insert_date)
#         """)

#         print(f"✅ MERGE: {row_count:,} rows in {time.time() - start_time:.1f}s")

#     else:
#         print("⏭️ SKIP: No changes")

# # -----------------------------------------------------------
# # FULL LOAD
# # -----------------------------------------------------------
# else:
#     print("🆕 FULL LOAD")
#     df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

#     df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("mergeSchema", "true") \
#         .saveAsTable(target_table)

#     print(f"✅ LOADED: {df.count():,} rows in {time.time() - start_time:.1f}s")
