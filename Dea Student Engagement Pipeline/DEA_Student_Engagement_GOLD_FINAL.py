# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DEA Student Engagement GOLD FINAL
# MAGIC
# MAGIC Silver → Gold transformation producing the **Customer Health Profile** per spec.
# MAGIC
# MAGIC ## Core Processed Datasets
# MAGIC
# MAGIC | Dataset | Description |
# MAGIC |---------|-------------|
# MAGIC | `LEADS_PROCESSED` | Flattened lead/contact, account, status, and source-lineage attributes |
# MAGIC | `CLOSE_CRM_USERS_PROCESSED` | Current user dimension with ID, name, email, status, role |
# MAGIC | `LEADS_ACTIVITIES_SUMMARY` | Deduplicated activities by ACTIVITY_ID (DATE_UPDATED → INSERT_DATE) with meeting/email/call fields |
# MAGIC | `SALES_DETAILS` | Latest qualifying sale per LEAD_ID (outcome 7 or 8) with setter/closer, contract value, program |
# MAGIC | `LEADS_ACCOUNT_DETAILS_UPDATES` | One current Coaching Client record per LEAD_ID with engagement metrics, flags, pattern, score, health band |
# MAGIC
# MAGIC ## Engagement Model (3 dimensions)
# MAGIC * **Slack** — `DAYS_SINCE_LAST_MESSAGE_FROM_CLIENT` from student_sentiment (≤14 days = engaged)
# MAGIC * **Platform** — `DAYS_SINCE_LAST_LOGIN` from mdl_users_raw.LASTACCESS (≤14 days = engaged)
# MAGIC * **Meeting** — CRM text 'A new event has been scheduled' + Calendly EVENT_CREATED_AT (≤30 days = engaged)
# MAGIC
# MAGIC ## Health Score Formula
# MAGIC `FINAL_HEALTH_SCORE = base_score(engagement_pattern) + missed_call_adjustment + sentiment_adjustment`
# MAGIC * **Not capped** at 0 or 100
# MAGIC * **Base**: 90/80/75/60/50/40/30/20/NO DATA
# MAGIC * **Missed calls (14d)**: 0→+5, 1-2→0, 3+→-15
# MAGIC * **Sentiment**: POSITIVE→+10, NEUTRAL→+5, NEGATIVE→-25
# MAGIC * **Bands**: NO DATA / GOOD(≥75) / AVERAGE(≥50) / POOR(<50)
# MAGIC
# MAGIC ## Deduplication Rules
# MAGIC | Source | Key | Ordering |
# MAGIC |--------|-----|----------|
# MAGIC | CRM activities | ACTIVITY_ID | DATE_UPDATED → INSERT_DATE |
# MAGIC | Calendly events | EVENT_URI | INVITEE_UPDATED_AT → EVENT_CREATED_AT → INSERT_DATE |
# MAGIC | Platform users | USER_ID | TIMEMODIFIED |
# MAGIC | Student sentiment | CHANNEL_ID | INSERT_DATE |
# MAGIC
# MAGIC ## Excluded per spec
# MAGIC * ❌ No lead-merge logic
# MAGIC * ❌ No payment-processing logic

# COMMAND ----------

# DBTITLE 1,Gold Layer Configuration
# ============================================================================
# GOLD LAYER CONFIGURATION
# ============================================================================
# Source: crm_ingestion.silver (normalized, exploded tables from DEA SILVER FINAL)
# Target: crm_ingestion.gold (dimensional models, facts, aggregations)
# ============================================================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

CATALOG = "crm_ingestion"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# ============================================================================
# HARDCODED BUSINESS CONSTANTS (per spec — do not alter without written approval)
# ============================================================================

# Email filter
EMAIL_TYPE_FILTER = "Email"
EMAIL_STATUS_FILTER = "inbox"

# Email exclusions (case-insensitive substring match)
EMAIL_EXCLUSIONS = [
    "A new event has been scheduled.",
    "is requesting access to the following folder",
    "requests access to an item",
]

# CRM meeting signal (same text excluded from email but included for meetings)
CRM_MEETING_SIGNAL = "A new event has been scheduled."

# Upcoming meeting
UPCOMING_MEETING_TYPE = "Meeting"

# Missed calls
MISSED_CALL_TYPE = "Call"
MISSED_CALL_DIRECTION = "inbound"
MISSED_CALL_DISPOSITION = "no-answer"
MISSED_CALL_LOOKBACK_DAYS = 14

# Slack types
STUDENT_SLACK_TYPE = "student"
TEAM_SLACK_TYPE = "internal_member"
EXCLUDED_SLACK_CHANNEL = "C082U471YMU"

# Engagement thresholds (days)
COMMUNICATION_THRESHOLD = 14   # Slack engagement
PLATFORM_THRESHOLD = 14        # Platform engagement
MEETING_THRESHOLD = 30         # Meeting engagement

# Sentiment
POSITIVE_SENTIMENT = "POSITIVE"
NEUTRAL_SENTIMENT = "NEUTRAL"
NEGATIVE_SENTIMENT = "NEGATIVE"
NO_SENTIMENT = "NO_SENTIMENT"

# Sentiment adjustments
SENTIMENT_ADJ = {"POSITIVE": 10, "NEUTRAL": 5, "NEGATIVE": -25}

# Missed call adjustments
MISSED_CALL_ADJ = {0: 5, 1: 0, 2: 0}  # 0→+5, 1-2→0, 3+→-15

# Health bands
HEALTH_BAND_GOOD = 75
HEALTH_BAND_AVERAGE = 50

# Qualifying sale activity type IDs
SALE_TYPE_IDS = [
    "actitype_3E85vFq3a06LlEzXT2N1kS",   # 7) New Sale
    "actitype_0FNk72Q8eSYX2MVd4A2UFx",   # 8) New Sale [Custom Payment Plan]
]

# Custom field IDs for sale activities
CF_CLOSER = "custom_cf_Lv5lSqLOZwLrNhe5M7kWx2mF8Ge2Z23aw5NUNhbXvVS"
CF_SETTER = "custom_cf_v385AJ8HSgepKQ3rvqo4yOA3nn49eGqz39DOqojJG5M"
CF_DATE_OF_SALE = "custom_cf_duzvav8KQ1PjbJLeAjqZna96ndGp3jO5U2JTfIuGFKi"
CF_PROGRAM = "custom_cf_mXqKxcmjnlEW223wM4lgb04XyYwDm0GJ9v99xWHPWO2"
CF_CONTRACT_VALUE = "custom_cf_vIanPjPEit6ssajmWkcprF2V1nO1itfes8hOSnjmhfT"
CF_CONTACT_NAME = "custom_cf_vFrtpvBHRQmraddhs4Jbd6MZ71fAg71tt4g3VlMbM6K"

print(f"Gold Layer Configuration")
print(f"  Source: {CATALOG}.{SILVER_SCHEMA}")
print(f"  Target: {CATALOG}.{GOLD_SCHEMA}")
print(f"  Hardcoded constants loaded (per spec)")
print("="*80)

# COMMAND ----------

# DBTITLE 1,dim_leads - Canonical Lead Dimension
# ============================================================================
# LEADS_PROCESSED — Flattened lead/contact, account, status, source-lineage
# ============================================================================
# Spec: No lead-merge logic. One record per LEAD_ID.
# ============================================================================

print("\nBuilding LEADS_PROCESSED...")

df_leads_raw = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.leads_raw").select(
    col("id").alias("lead_id"),
    col("name").alias("lead_name"),
    col("display_name").alias("account_name"),
    col("status_id"),
    col("status_label").alias("customer_status"),
    col("date_created").alias("lead_created_date"),
    col("date_updated").alias("lead_updated_date"),
    col("description"),
    col("organization_id"),
    col("created_by").alias("created_by_user_id"),
    col("created_by_name"),
    col("bronze_insert_date").alias("source_insert_date")
)

# Deduplicate by lead_id — keep latest by lead_updated_date then source_insert_date
window_lead = Window.partitionBy("lead_id").orderBy(
    col("lead_updated_date").desc_nulls_last(),
    col("source_insert_date").desc_nulls_last()
)
df_leads_final = df_leads_raw.withColumn(
    "row_num", row_number().over(window_lead)
).filter(col("row_num") == 1).drop("row_num").withColumn(
    "gold_insert_date", current_timestamp()
)

df_leads_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_PROCESSED")

count_leads = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_PROCESSED").count()
print(f"✅ LEADS_PROCESSED: {count_leads:,} leads")

# Also create dim_lead_emails (email → lead_id mapping, NO lead_merges)
print("\nBuilding dim_lead_emails...")
df_emails = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.leads_raw_contacts_emails").select(
    col("leads_raw_id").alias("lead_id"),
    lower(trim(col("email"))).alias("email"),
    col("type").alias("email_type"),
    col("is_unsubscribed"),
    col("bronze_insert_date").alias("source_insert_date")
).filter(col("email").isNotNull() & (col("email") != ""))

# Deduplicate emails — keep one per (lead_id, email)
window_email = Window.partitionBy("lead_id", "email").orderBy(col("source_insert_date").desc_nulls_last())
df_emails = df_emails.withColumn("row_num", row_number().over(window_email)).filter(col("row_num") == 1).drop("row_num")

df_emails.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_lead_emails")

count_emails = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_lead_emails").count()
print(f"✅ dim_lead_emails: {count_emails:,} email mappings")

# COMMAND ----------

# DBTITLE 1,dim_users - User Dimension
# ============================================================================
# CLOSE_CRM_USERS_PROCESSED — Current user dimension
# ============================================================================
# Spec: User ID, name, email, status, role, and user lookup mappings
# ============================================================================

print("\nBuilding CLOSE_CRM_USERS_PROCESSED...")

df_users_raw = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.close_crm_users_raw_data").select(
    col("id").alias("user_id"),
    col("email"),
    lower(trim(col("email"))).alias("email_normalized"),
    col("first_name"),
    col("last_name"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("display_name"),
    col("image"),
    col("date_created").alias("user_created_date"),
    col("date_updated").alias("user_updated_date"),
    col("bronze_insert_date").alias("source_insert_date")
)

# Deduplicate by user_id — keep most recent by date_updated
window_users = Window.partitionBy("user_id").orderBy(col("user_updated_date").desc_nulls_last())
df_dim_users = df_users_raw.withColumn(
    "row_num", row_number().over(window_users)
).filter(col("row_num") == 1).drop("row_num").select(
    col("user_id").alias("user_key"),
    col("email"),
    col("email_normalized").alias("user_email"),
    col("display_name").alias("user_name"),
    col("first_name"),
    col("last_name"),
    col("image"),
    col("user_created_date"),
    col("user_updated_date"),
    current_timestamp().alias("gold_insert_date")
)

df_dim_users.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.CLOSE_CRM_USERS_PROCESSED")

count_users = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.CLOSE_CRM_USERS_PROCESSED").count()
print(f"✅ CLOSE_CRM_USERS_PROCESSED: {count_users:,} unique users")

# COMMAND ----------

# DBTITLE 1,fact_sales - Sales Transactions
# ============================================================================
# SALES_DETAILS — Latest qualifying sale per LEAD_ID by ACTIVITY_AT
# ============================================================================
# Spec: Qualifying outcomes: 7) New Sale and 8) New Sale [Custom Payment Plan]
# Include setter/closer identity, contract value, date of sale, program.
# Do NOT include cash collected.
# Source: silver.lead_activites_raw_data (filtered by custom_activity_type_id)
# ============================================================================

print("\nBuilding SALES_DETAILS...")

# Get the activity type IDs for qualifying sales
sale_type_ids = [row["id"] for row in spark.table(f"{CATALOG}.{SILVER_SCHEMA}.custom_activites_raw_data")
    .filter(col("name").isin("7) New Sale", "8) New Sale [Custom Payment Plan]"))
    .select("id").collect()]
print(f"  Qualifying sale type IDs: {sale_type_ids}")

# Load activities filtered to qualifying sale types
df_sales_acts = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.lead_activites_raw_data").filter(
    col("custom_activity_type_id").isin(sale_type_ids)
).select(
    col("id").alias("activity_id"),
    col("lead_id"),
    col("activity_at"),
    col("date_updated"),
    col("bronze_insert_date").alias("source_insert_date"),
    col("custom_activity_type_id"),
    col(CF_CLOSER).alias("closer_user_id_raw"),
    col(CF_SETTER).alias("setter_user_id_raw"),
    col(CF_DATE_OF_SALE).alias("date_of_sale_raw"),
    col(CF_PROGRAM).alias("program_raw"),
    col(CF_CONTRACT_VALUE).alias("contract_value_raw"),
    col(CF_CONTACT_NAME).alias("contact_name_raw"),
    col("user_id").alias("activity_user_id"),
    col("user_name").alias("activity_user_name"),
    col("created_by").alias("created_by_id"),
    col("created_by_name")
)

# Deduplicate by activity_id — keep latest by DATE_UPDATED then INSERT_DATE
window_sale = Window.partitionBy("activity_id").orderBy(
    col("date_updated").desc_nulls_last(),
    col("source_insert_date").desc_nulls_last()
)
df_sales_dedup = df_sales_acts.withColumn("row_num", row_number().over(window_sale)) \
    .filter(col("row_num") == 1).drop("row_num")

# Get latest qualifying sale per LEAD_ID by ACTIVITY_AT
window_latest_sale = Window.partitionBy("lead_id").orderBy(
    col("activity_at").desc_nulls_last()
)
df_sales_latest = df_sales_dedup.withColumn("sale_rank", row_number().over(window_latest_sale)) \
    .filter(col("sale_rank") == 1).drop("sale_rank")

# Join to CLOSE_CRM_USERS_PROCESSED for setter/closer names
df_users = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.CLOSE_CRM_USERS_PROCESSED")

df_sales_final = df_sales_latest.alias("s").join(
    df_users.alias("closer_u"),
    col("s.closer_user_id_raw") == col("closer_u.user_key"),
    "left"
).join(
    df_users.alias("setter_u"),
    col("s.setter_user_id_raw") == col("setter_u.user_key"),
    "left"
).select(
    col("s.lead_id"),
    col("s.activity_id"),
    col("s.activity_at").alias("sale_activity_at"),
    col("s.date_of_sale_raw").alias("date_of_sale"),
    col("s.program_raw").alias("program"),
    col("s.contract_value_raw").cast("long").alias("contract_value"),
    col("s.closer_user_id_raw").alias("closer_id"),
    col("closer_u.user_name").alias("closer_name"),
    col("closer_u.user_email").alias("closer_email"),
    col("s.setter_user_id_raw").alias("setter_id"),
    col("setter_u.user_name").alias("setter_name"),
    col("setter_u.user_email").alias("setter_email"),
    col("s.custom_activity_type_id"),
    col("s.contact_name_raw").alias("contact_name"),
    col("s.created_by_name"),
    current_timestamp().alias("gold_insert_date")
)

df_sales_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.SALES_DETAILS")

count_sales = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.SALES_DETAILS").count()
print(f"✅ SALES_DETAILS: {count_sales:,} qualifying sales (latest per lead)")

# COMMAND ----------

# DBTITLE 1,fact_activities - Activity Transactions
# ============================================================================
# LEADS_ACTIVITIES_SUMMARY — Deduplicated activity records
# ============================================================================
# Spec: Keep latest version of each ACTIVITY_ID, prioritizing DATE_UPDATED
#       then INSERT_DATE. Include timestamp, type, resolved outcome, meeting
#       fields, email/call attributes, and owner attribution.
# ============================================================================

print("\nBuilding LEADS_ACTIVITIES_SUMMARY...")

df_activities_raw = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.lead_activites_raw_data").select(
    col("id").alias("activity_id"),
    col("lead_id"),
    col("type").alias("activity_type"),
    col("activity_at").cast("timestamp").alias("activity_at"),
    col("direction").alias("direction"),
    col("status").alias("meeting_status"),
    col("disposition").alias("disposition"),
    col("subject").alias("subject"),
    col("body_preview").alias("body_preview"),
    col("text").alias("activity_text"),
    col("starts_at").alias("meeting_starts_at"),
    col("ends_at").alias("meeting_ends_at"),
    col("outcome_id"),
    col("outcome_reason"),
    col("created_by").alias("created_by_id"),
    col("created_by_name"),
    col("user_id").alias("user_id"),
    col("user_name"),
    col("date_created").cast("timestamp").alias("date_created"),
    col("date_updated").cast("timestamp").alias("date_updated"),
    col("note"),
    col("source").alias("activity_source"),
    col("contact_id"),
    col("organization_id"),
    col("custom_activity_type_id"),
    col("bronze_insert_date").alias("insert_date")
)

# Deduplicate by ACTIVITY_ID — keep latest by DATE_UPDATED then INSERT_DATE
window_act = Window.partitionBy("activity_id").orderBy(
    col("date_updated").desc_nulls_last(),
    col("insert_date").desc_nulls_last()
)
df_activities_final = df_activities_raw.withColumn(
    "row_num", row_number().over(window_act)
).filter(col("row_num") == 1).drop("row_num").withColumn(
    "gold_insert_date", current_timestamp()
)

df_activities_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACTIVITIES_SUMMARY")

count_activities = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACTIVITIES_SUMMARY").count()
print(f"✅ LEADS_ACTIVITIES_SUMMARY: {count_activities:,} deduplicated activities")

# COMMAND ----------

# DBTITLE 1,dim_lead_emails - Email to Lead Mapping
# ============================================================================
# PLATFORM USERS DEDUP — mdl_users_raw by USER_ID, latest TIMEMODIFIED
# ============================================================================
# Spec: For duplicate USER_ID records, retain the row with the latest TIMEMODIFIED.
# Join to dim_lead_emails via EMAIL → lead_id for downstream metrics.
# ============================================================================

print("\nBuilding platform_users_dedup...")

df_mdl_raw = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mdl_users_raw").select(
    col("ID").alias("user_id"),
    lower(trim(col("EMAIL"))).alias("email"),
    col("USERNAME"),
    col("FIRSTACCESS").cast("timestamp").alias("first_access"),
    col("LASTACCESS").cast("timestamp").alias("last_access"),
    col("LASTLOGIN").cast("timestamp").alias("last_login_raw"),
    col("TIMEMODIFIED").cast("timestamp").alias("time_modified"),
    col("TIMECREATED").cast("timestamp").alias("time_created"),
    col("DELETED").alias("is_deleted"),
    col("SUSPENDED").alias("is_suspended"),
    col("FIRSTNAME"),
    col("LASTNAME"),
    col("bronze_insert_date")
).filter(col("email").isNotNull() & (col("email") != ""))

# Deduplicate by user_id — keep latest TIMEMODIFIED
window_mdl = Window.partitionBy("user_id").orderBy(col("time_modified").desc_nulls_last())
df_mdl_dedup = df_mdl_raw.withColumn("row_num", row_number().over(window_mdl)) \
    .filter(col("row_num") == 1).drop("row_num")

# Join to dim_lead_emails for lead_id lookup
df_lead_emails = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_lead_emails").select(
    col("lead_id"), col("email")
)

df_platform = df_mdl_dedup.alias("m").join(
    df_lead_emails.alias("le"),
    col("m.email") == col("le.email"),
    "left"
).select(
    col("le.lead_id"),
    col("m.user_id"),
    col("m.email"),
    col("m.USERNAME"),
    col("m.first_access"),
    col("m.last_access"),
    col("m.last_login_raw"),
    col("m.time_modified"),
    col("m.time_created"),
    col("m.is_deleted"),
    col("m.is_suspended"),
    col("m.FIRSTNAME"),
    col("m.LASTNAME"),
    current_timestamp().alias("gold_insert_date")
)

df_platform.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.platform_users_dedup")

count_mdl = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.platform_users_dedup").count()
matched_mdl = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.platform_users_dedup").filter(col("lead_id").isNotNull()).count()
print(f"✅ platform_users_dedup: {count_mdl:,} users, {matched_mdl:,} matched to CRM leads")

# ============================================================================
# STUDENT SENTIMENT DEDUP — by CHANNEL_ID, latest INSERT_DATE
# ============================================================================
# Spec: For duplicate CHANNEL_ID snapshots, retain latest by INSERT_DATE.
# ============================================================================

print("\nBuilding student_sentiment_dedup...")

df_sent_raw = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.student_sentiment").select(
    col("CHANNEL_ID").alias("channel_id"),
    col("CHANNEL_NAME").alias("channel_name"),
    col("DAYS_SINCE_LAST_MESSAGE_FROM_STUDENT").alias("days_since_last_message_from_student"),
    col("DAYS_SINCE_LAST_MESSAGE_FROM_TEAM").alias("days_since_last_message_from_team"),
    col("SENTIMENTS_LAST_30_DAYS").alias("sentiments_last_30_days"),
    col("INSERT_DATE").cast("timestamp").alias("insert_date"),
    col("bronze_insert_date")
)

# Deduplicate by CHANNEL_ID — keep latest INSERT_DATE
window_sent = Window.partitionBy("channel_id").orderBy(col("insert_date").desc_nulls_last())
df_sent_dedup = df_sent_raw.withColumn("row_num", row_number().over(window_sent)) \
    .filter(col("row_num") == 1).drop("row_num")

# Try joining sentiment CHANNEL_ID → platform_users USERNAME → lead_id
df_platform_users = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.platform_users_dedup").select(
    col("lead_id"), col("USERNAME").alias("platform_username")
)

df_sent_final = df_sent_dedup.alias("s").join(
    df_platform_users.alias("p"),
    col("s.channel_id") == col("p.platform_username"),
    "left"
).select(
    col("p.lead_id"),
    col("s.channel_id"),
    col("s.channel_name"),
    col("s.days_since_last_message_from_student"),
    col("s.days_since_last_message_from_team"),
    col("s.sentiments_last_30_days"),
    col("s.insert_date"),
    current_timestamp().alias("gold_insert_date")
)

df_sent_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.student_sentiment_dedup")

count_sent = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.student_sentiment_dedup").count()
matched_sent = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.student_sentiment_dedup").filter(col("lead_id").isNotNull()).count()
print(f"✅ student_sentiment_dedup: {count_sent:,} channels, {matched_sent:,} matched to leads")
if matched_sent == 0:
    print("  ⚠️  0% match — sentiment CHANNEL_ID doesn't match platform USERNAME with masked data")

# COMMAND ----------

# DBTITLE 1,fact_calendly_meetings - Calendly Meeting Events
# ============================================================================
# FACT TABLE: fact_calendly_meetings
# ============================================================================
# Purpose: Calendly scheduled meetings joined to leads by email
# Source: silver.calendly_scheduled_events + gold.dim_lead_emails
# Dedup: For duplicate EVENT_URI, retain latest by INVITEE_UPDATED_AT,
#        then EVENT_CREATED_AT, then INSERT_DATE
# ============================================================================

print("\nBuilding fact_calendly_meetings...")

df_calendly = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.calendly_scheduled_events").select(
    col("EVENT_URI").alias("event_id"),
    col("EVENT_NAME"),
    col("CALENDLY_EVENT_NAME"),
    col("EVENT_TYPE_NAME"),
    col("EVENT_TYPE_URI"),
    col("INVITEE_EMAIL").alias("invitee_email"),
    col("INVITEE_NAME").alias("invitee_name"),
    col("EVENT_HOST_EMAIL").alias("host_email"),
    col("EVENT_HOST_NAME").alias("host_name"),
    col("PROFILE_NAME"),
    col("EVENT_START_TIME").cast("timestamp").alias("meeting_start"),
    col("EVENT_END_TIME").cast("timestamp").alias("meeting_end"),
    col("EVENT_DURATION").alias("duration_minutes"),
    col("EVENT_CREATED_AT").cast("timestamp").alias("event_created_at"),
    col("INVITEE_CREATED_AT").alias("invitee_created_at"),
    col("INVITEE_UPDATED_AT").alias("invitee_updated_at"),
    col("INTERNAL_NOTE"),
    col("INSERT_TIMESTAMP"),
    col("bronze_insert_date")
).filter(col("invitee_email").isNotNull())

# Deduplicate by EVENT_URI — latest INVITEE_UPDATED_AT, then EVENT_CREATED_AT, then INSERT_TIMESTAMP
window_cal = Window.partitionBy("event_id").orderBy(
    col("invitee_updated_at").desc_nulls_last(),
    col("event_created_at").desc_nulls_last(),
    col("INSERT_TIMESTAMP").desc_nulls_last()
)
df_calendly = df_calendly.withColumn("row_num", row_number().over(window_cal)) \
    .filter(col("row_num") == 1).drop("row_num")

# Join to dim_lead_emails to get lead_id
df_lead_emails = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_lead_emails").select(
    col("lead_id"), col("email")
)

df_calendly_resolved = df_calendly.alias("c").join(
    df_lead_emails.alias("le"),
    lower(trim(col("c.invitee_email"))) == col("le.email"),
    "left"
).select(
    col("le.lead_id"),
    col("c.event_id"),
    col("c.event_name"),
    col("c.calendly_event_name"),
    col("c.event_type_name"),
    col("c.invitee_email"),
    col("c.invitee_name"),
    col("c.host_email"),
    col("c.host_name"),
    col("c.profile_name"),
    col("c.meeting_start"),
    col("c.meeting_end"),
    col("c.duration_minutes"),
    col("c.event_created_at"),
    col("c.internal_note"),
    current_timestamp().alias("gold_insert_date")
)

# Print match statistics
total_calendly = df_calendly_resolved.count()
matched = df_calendly_resolved.filter(col("lead_id").isNotNull()).count()
unmatched = total_calendly - matched
print(f"  Calendly events: {total_calendly:,} total, {matched:,} matched to leads, {unmatched:,} unmatched")
if matched == 0:
    print("  ⚠️  WARNING: 0% match rate — Calendly uses real email addresses while")
    print("      CRM contacts use masked emails (user_xxx@masked.com).")
    print("      In production with unmasked CRM data, this join would produce matches.")

df_calendly_resolved.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_calendly_meetings")

count_calendly = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_calendly_meetings").count()
print(f"✅ fact_calendly_meetings: {count_calendly:,} deduplicated meeting records")

# COMMAND ----------

# DBTITLE 1,Step 5: Engagement Metrics
# ============================================================================
# STEP 5: ENGAGEMENT METRICS — All per-lead metrics per spec
# ============================================================================
# Filters to Coaching Client leads, then calculates:
#   DAYS_SINCE_LAST_EMAIL (with 3 text exclusions)
#   DAYS_SINCE_LAST_MEETING (CRM text 'A new event has been scheduled.')
#   DAYS_SINCE_LAST_MEETING_CALENDLY (EVENT_CREATED_AT)
#   UPCOMING_MEETING_DAYS (TYPE=Meeting, MEETING_STARTS_AT >= CURRENT_DATE)
#   DAYS_SINCE_LAST_LOGIN / LAST_LOGIN_DATE (from LASTACCESS)
#   MISSED_CALLS_14D (unique ACTIVITY_ID, 14-day lookback)
#   DAYS_SINCE_LAST_MESSAGE_FROM_CLIENT (from sentiment)
#   DAYS_SINCE_LAST_MESSAGE_FROM_TEAM_MEMBER (from sentiment)
#   SENTIMENTS_LAST_30_DAYS (trimmed, case-insensitive)
# ============================================================================

print("\nBuilding agg_engagement_metrics...")

# Start with Coaching Client leads
df_cc = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_PROCESSED") \
    .filter(col("customer_status") == "Coaching Client") \
    .select(col("lead_id"))

cc_count = df_cc.count()
print(f"  Coaching Client leads: {cc_count:,}")

# --- 1. DAYS_SINCE_LAST_EMAIL ---
# TYPE=Email, MEETING_STATUS=inbox, exclude 3 text patterns (case-insensitive)
df_email = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACTIVITIES_SUMMARY") \
    .filter(col("activity_type") == EMAIL_TYPE_FILTER) \
    .filter(col("meeting_status") == EMAIL_STATUS_FILTER) \
    .filter(
        ~col("activity_text").rlike("(?i)A new event has been scheduled") &
        ~col("activity_text").rlike("(?i)is requesting access to the following folder") &
        ~col("activity_text").rlike("(?i)requests access to an item")
    ) \
    .filter(col("lead_id").isNotNull()) \
    .groupBy("lead_id") \
    .agg(max("activity_at").alias("_last_email_at")) \
    .withColumn("days_since_last_email", datediff(current_date(), col("_last_email_at")))

# --- 2. DAYS_SINCE_LAST_MEETING (CRM) ---
# activity_text contains 'A new event has been scheduled.' (case-insensitive)
df_crm_meeting = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACTIVITIES_SUMMARY") \
    .filter(col("activity_text").rlike("(?i)A new event has been scheduled")) \
    .filter(col("lead_id").isNotNull()) \
    .groupBy("lead_id") \
    .agg(max("activity_at").alias("_last_crm_meeting_at")) \
    .withColumn("days_since_last_meeting", datediff(current_date(), col("_last_crm_meeting_at")))

# --- 3. DAYS_SINCE_LAST_MEETING_CALENDLY ---
# Latest EVENT_CREATED_AT per lead from fact_calendly_meetings
df_cal_meeting = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_calendly_meetings") \
    .filter(col("lead_id").isNotNull()) \
    .groupBy("lead_id") \
    .agg(max("event_created_at").alias("_last_calendly_at")) \
    .withColumn("days_since_last_meeting_calendly", datediff(current_date(), col("_last_calendly_at")))

# --- 4. UPCOMING_MEETING_DAYS ---
# TYPE=Meeting, MEETING_STARTS_AT >= CURRENT_DATE, latest qualifying start
df_upcoming = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACTIVITIES_SUMMARY") \
    .filter(col("activity_type") == UPCOMING_MEETING_TYPE) \
    .filter(col("meeting_starts_at").isNotNull()) \
    .filter(to_date(col("meeting_starts_at")) >= current_date()) \
    .withColumn("_meeting_days", datediff(to_date(col("meeting_starts_at")), current_date())) \
    .groupBy("lead_id") \
    .agg(min("_meeting_days").alias("upcoming_meeting_days"))

# --- 5. Platform: DAYS_SINCE_LAST_LOGIN / LAST_LOGIN_DATE ---
# From platform_users_dedup.LASTACCESS (not LASTLOGIN)
df_platform = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.platform_users_dedup") \
    .filter(col("lead_id").isNotNull()) \
    .filter(col("last_access").isNotNull()) \
    .select(
        col("lead_id"),
        to_date(col("last_access")).alias("last_login_date"),
        datediff(current_date(), to_date(col("last_access"))).alias("days_since_last_login")
    )

# --- 6. MISSED_CALLS_14D ---
# TYPE=Call, DIRECTION=inbound, DISPOSITION=no-answer, 14-day lookback
df_missed = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACTIVITIES_SUMMARY") \
    .filter(col("activity_type") == MISSED_CALL_TYPE) \
    .filter(col("direction") == MISSED_CALL_DIRECTION) \
    .filter(col("disposition") == MISSED_CALL_DISPOSITION) \
    .filter(col("activity_at") >= date_sub(current_date(), MISSED_CALL_LOOKBACK_DAYS)) \
    .filter(col("lead_id").isNotNull()) \
    .groupBy("lead_id") \
    .agg(countDistinct("activity_id").alias("missed_calls_14d"))

# --- 7. Sentiment metrics ---
# From student_sentiment_dedup (already deduped by CHANNEL_ID)
df_sentiment = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.student_sentiment_dedup") \
    .filter(col("lead_id").isNotNull()) \
    .select(
        col("lead_id"),
        col("days_since_last_message_from_student").alias("days_since_last_message_from_client"),
        col("days_since_last_message_from_team").alias("days_since_last_message_from_team_member"),
        trim(upper(col("sentiments_last_30_days"))).alias("sentiment")
    )

# Default missing sentiment to NO_SENTIMENT
df_sentiment = df_sentiment.withColumn(
    "sentiment",
    when(col("sentiment").isNull() | (col("sentiment") == ""), lit(NO_SENTIMENT))
    .otherwise(col("sentiment"))
)

# --- Join all metrics to Coaching Client leads ---
df_metrics = df_cc.alias("cc") \
    .join(df_email.select("lead_id", "days_since_last_email").alias("e"), col("cc.lead_id") == col("e.lead_id"), "left") \
    .join(df_crm_meeting.select("lead_id", "days_since_last_meeting").alias("cm"), col("cc.lead_id") == col("cm.lead_id"), "left") \
    .join(df_cal_meeting.select("lead_id", "days_since_last_meeting_calendly").alias("clm"), col("cc.lead_id") == col("clm.lead_id"), "left") \
    .join(df_upcoming.alias("u"), col("cc.lead_id") == col("u.lead_id"), "left") \
    .join(df_platform.alias("p"), col("cc.lead_id") == col("p.lead_id"), "left") \
    .join(df_missed.alias("mc"), col("cc.lead_id") == col("mc.lead_id"), "left") \
    .join(df_sentiment.alias("s"), col("cc.lead_id") == col("s.lead_id"), "left") \
    .select(
        col("cc.lead_id"),
        col("e.days_since_last_email"),
        col("cm.days_since_last_meeting"),
        col("clm.days_since_last_meeting_calendly"),
        col("u.upcoming_meeting_days"),
        col("p.last_login_date"),
        col("p.days_since_last_login"),
        coalesce(col("mc.missed_calls_14d"), lit(0)).alias("missed_calls_14d"),
        col("s.days_since_last_message_from_client"),
        col("s.days_since_last_message_from_team_member"),
        col("s.sentiment"),
        current_timestamp().alias("gold_insert_date")
    )

df_metrics.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_metrics")

count_metrics = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_metrics").count()
print(f"✅ agg_engagement_metrics: {count_metrics:,} Coaching Client leads with metrics")
# Print sample
df_metrics.limit(5).toPandas().to_string()

# COMMAND ----------

# DBTITLE 1,Steps 6-7: Engagement Flags + Health Score
# ============================================================================
# STEPS 6-7: ENGAGEMENT FLAGS, PATTERN, AND HEALTH SCORE
# ============================================================================
# Step 6: Derive three engagement flags, then apply ordered pattern table
# Step 7: Apply base score, missed-call adjustment, sentiment adjustment, band
# ============================================================================

print("\nBuilding agg_engagement_flags_score...")

df_metrics = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_metrics")

# --- Step 6: Engagement Flags ---
# SLACK_ENGAGED_FLAG: NULL if client-message recency is NULL; 1 if <=14; else 0
df_flags = df_metrics.withColumn(
    "slack_engaged_flag",
    when(col("days_since_last_message_from_client").isNull(), lit(None).cast("int"))
    .when(col("days_since_last_message_from_client") <= COMMUNICATION_THRESHOLD, lit(1))
    .otherwise(lit(0))
)

# PLATFORM_ENGAGED_FLAG: NULL if login recency is NULL; 1 if <=14; else 0
df_flags = df_flags.withColumn(
    "platform_engaged_flag",
    when(col("days_since_last_login").isNull(), lit(None).cast("int"))
    .when(col("days_since_last_login") <= PLATFORM_THRESHOLD, lit(1))
    .otherwise(lit(0))
)

# MEETING_ENGAGED_FLAG: NULL if either meeting-recency source is NULL.
# When both exist, 1 if either is <=30; otherwise 0.
df_flags = df_flags.withColumn(
    "meeting_engaged_flag",
    when(
        col("days_since_last_meeting").isNull() | col("days_since_last_meeting_calendly").isNull(),
        lit(None).cast("int")
    )
    .when(
        (col("days_since_last_meeting") <= MEETING_THRESHOLD) |
        (col("days_since_last_meeting_calendly") <= MEETING_THRESHOLD),
        lit(1)
    )
    .otherwise(lit(0))
)

# --- Engagement Pattern (ordered table per spec) ---
# "Other" means 0 or NULL
slack_1 = col("slack_engaged_flag") == 1
slack_other = (col("slack_engaged_flag") == 0) | col("slack_engaged_flag").isNull()
plat_1 = col("platform_engaged_flag") == 1
plat_other = (col("platform_engaged_flag") == 0) | col("platform_engaged_flag").isNull()
meet_1 = col("meeting_engaged_flag") == 1
meet_other = (col("meeting_engaged_flag") == 0) | col("meeting_engaged_flag").isNull()

df_flags = df_flags.withColumn(
    "engagement_pattern",
    when(slack_1 & plat_1 & meet_1, lit("SLACK + PLATFORM + MEETING"))
    .when(slack_1 & plat_1 & meet_other, lit("SLACK + PLATFORM"))
    .when(slack_other & plat_1 & meet_1, lit("PLATFORM + MEETING"))
    .when(slack_1 & plat_other & meet_1, lit("SLACK + MEETING"))
    .when(slack_1 & plat_other & meet_other, lit("SLACK ONLY"))
    .when(slack_other & plat_1 & meet_other, lit("PLATFORM ONLY"))
    .when(slack_other & plat_other & meet_1, lit("MEETING ONLY"))
    .when((col("slack_engaged_flag") == 0) & (col("platform_engaged_flag") == 0) & (col("meeting_engaged_flag") == 0), lit("NO ENGAGEMENT"))
    .when((col("slack_engaged_flag") == 0) & (col("platform_engaged_flag") == 0) & col("meeting_engaged_flag").isNull(), lit("NO ENGAGEMENT"))
    .when(col("slack_engaged_flag").isNull() & (col("platform_engaged_flag") == 0) & (col("meeting_engaged_flag") == 0), lit("NO ENGAGEMENT"))
    .when((col("slack_engaged_flag") == 0) & col("platform_engaged_flag").isNull() & (col("meeting_engaged_flag") == 0), lit("NO ENGAGEMENT"))
    .otherwise(lit("NO DATA"))
)

# --- Step 7: Health Score ---
# Base score by engagement pattern
df_flags = df_flags.withColumn(
    "base_score",
    when(col("engagement_pattern") == "SLACK + PLATFORM + MEETING", lit(90))
    .when(col("engagement_pattern") == "SLACK + PLATFORM", lit(80))
    .when(col("engagement_pattern") == "PLATFORM + MEETING", lit(75))
    .when(col("engagement_pattern") == "SLACK + MEETING", lit(60))
    .when(col("engagement_pattern") == "PLATFORM ONLY", lit(50))
    .when(col("engagement_pattern") == "MEETING ONLY", lit(40))
    .when(col("engagement_pattern") == "SLACK ONLY", lit(30))
    .when(col("engagement_pattern") == "NO ENGAGEMENT", lit(20))
    .otherwise(lit(None).cast("int"))  # NO DATA → no score
)

# Missed call adjustment (NOT applied to NO DATA)
df_flags = df_flags.withColumn(
    "missed_call_adjustment",
    when(col("engagement_pattern") == "NO DATA", lit(0))
    .when(col("missed_calls_14d") == 0, lit(5))
    .when(col("missed_calls_14d").between(1, 2), lit(0))
    .when(col("missed_calls_14d") >= 3, lit(-15))
    .otherwise(lit(0))
)

# Sentiment adjustment (NOT applied to NO DATA)
df_flags = df_flags.withColumn(
    "sentiment_adjustment",
    when(col("engagement_pattern") == "NO DATA", lit(0))
    .when(col("sentiment") == POSITIVE_SENTIMENT, lit(10))
    .when(col("sentiment") == NEUTRAL_SENTIMENT, lit(5))
    .when(col("sentiment") == NEGATIVE_SENTIMENT, lit(-25))
    .otherwise(lit(0))
)

# FINAL_HEALTH_SCORE = base + missed_call_adj + sentiment_adj (NOT capped at 0 or 100)
df_flags = df_flags.withColumn(
    "final_health_score",
    when(col("engagement_pattern") == "NO DATA", lit(None).cast("int"))
    .otherwise(col("base_score") + col("missed_call_adjustment") + col("sentiment_adjustment"))
)

# Health band
df_flags = df_flags.withColumn(
    "health_band",
    when(col("engagement_pattern") == "NO DATA", lit("NO DATA"))
    .when(col("final_health_score") >= HEALTH_BAND_GOOD, lit("GOOD"))
    .when(col("final_health_score") >= HEALTH_BAND_AVERAGE, lit("AVERAGE"))
    .otherwise(lit("POOR"))
)

df_flags.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_flags_score")

count_flags = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_flags_score").count()
print(f"✅ agg_engagement_flags_score: {count_flags:,} leads")

# Print pattern distribution
print("\n  Engagement Pattern Distribution:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_flags_score") \
    .groupBy("engagement_pattern").count().orderBy(col("count").desc()).show(truncate=False)

# Print health band distribution
print("\n  Health Band Distribution:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_flags_score") \
    .groupBy("health_band").count().orderBy(col("count").desc()).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 8: LEADS_ACCOUNT_DETAILS_UPDATES
# ============================================================================
# STEP 8: LEADS_ACCOUNT_DETAILS_UPDATES — Final Customer Health Profile
# ============================================================================
# One current Coaching Client record per LEAD_ID.
# Supports lookup by Coaching Client email.
#
# Field groups:
#   Account & Lead: lead_id, customer_name, account_name, status, email, CSM
#   Sales Context: sale_date, program, contract_value, setter, closer
#   Engagement Metrics: email_recency, message_recency, meeting_recency,
#                       upcoming_meeting, login_recency, missed_calls, sentiment
#   Health Output: 3 flags, pattern, base_score, adjustments, final_score, band
# ============================================================================

print("\nBuilding LEADS_ACCOUNT_DETAILS_UPDATES...")

# Coaching Client leads
df_cc = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_PROCESSED") \
    .filter(col("customer_status") == "Coaching Client")

# Email lookup — deduplicate to one email per lead_id to avoid row multiplication
df_emails_raw = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_lead_emails") \
    .select(col("lead_id"), col("email").alias("customer_email"))

window_email = Window.partitionBy("lead_id").orderBy(col("customer_email"))
df_emails = df_emails_raw.withColumn("rn", row_number().over(window_email)) \
    .filter(col("rn") == 1).drop("rn")

# Sales context
df_sales = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.SALES_DETAILS")

# Engagement flags + health score
df_flags = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_engagement_flags_score")

# CSM user info (join created_by to users for avatar)
df_users = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.CLOSE_CRM_USERS_PROCESSED") \
    .select(
        col("user_key").alias("csm_user_id"),
        col("user_name").alias("csm_name"),
        col("user_email").alias("csm_email"),
        col("image").alias("csm_avatar")
    )

# Join everything
df_mart = df_cc.alias("l") \
    .join(df_emails.alias("e"), col("l.lead_id") == col("e.lead_id"), "left") \
    .join(df_sales.alias("s"), col("l.lead_id") == col("s.lead_id"), "left") \
    .join(df_flags.alias("f"), col("l.lead_id") == col("f.lead_id"), "left") \
    .join(df_users.alias("u"), col("l.created_by_user_id") == col("u.csm_user_id"), "left") \
    .select(
        # --- Account & Lead ---
        col("l.lead_id"),
        col("l.lead_name").alias("customer_name"),
        col("l.account_name"),
        col("l.customer_status"),
        col("e.customer_email"),
        col("l.created_by_user_id").alias("csm_user_id"),
        col("u.csm_name"),
        col("u.csm_email"),
        col("u.csm_avatar"),
        col("l.lead_created_date"),
        col("l.lead_updated_date"),
        # --- Sales Context ---
        col("s.date_of_sale").alias("latest_sale_date"),
        col("s.program").alias("sale_program"),
        col("s.contract_value"),
        col("s.setter_id"),
        col("s.setter_name"),
        col("s.setter_email"),
        col("s.closer_id"),
        col("s.closer_name"),
        col("s.closer_email"),
        col("s.sale_activity_at"),
        # --- Engagement Metrics ---
        col("f.days_since_last_email"),
        col("f.days_since_last_meeting"),
        col("f.days_since_last_meeting_calendly"),
        col("f.upcoming_meeting_days"),
        col("f.last_login_date"),
        col("f.days_since_last_login"),
        col("f.missed_calls_14d"),
        col("f.days_since_last_message_from_client"),
        col("f.days_since_last_message_from_team_member"),
        col("f.sentiment"),
        # --- Health Output ---
        col("f.slack_engaged_flag"),
        col("f.platform_engaged_flag"),
        col("f.meeting_engaged_flag"),
        col("f.engagement_pattern"),
        col("f.base_score"),
        col("f.missed_call_adjustment"),
        col("f.sentiment_adjustment"),
        col("f.final_health_score"),
        col("f.health_band"),
        current_timestamp().alias("gold_insert_date")
    )

df_mart.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACCOUNT_DETAILS_UPDATES")

count_mart = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACCOUNT_DETAILS_UPDATES").count()
print(f"✅ LEADS_ACCOUNT_DETAILS_UPDATES: {count_mart:,} Coaching Client records")

# Print health band distribution
print("\n  Health Band Distribution:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACCOUNT_DETAILS_UPDATES") \
    .groupBy("health_band").count().orderBy(col("count").desc()).show(truncate=False)

# Print engagement pattern distribution
print("\n  Engagement Pattern Distribution:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACCOUNT_DETAILS_UPDATES") \
    .groupBy("engagement_pattern").count().orderBy(col("count").desc()).show(truncate=False)

# Sample email lookup
print("\n  Sample Email Lookup:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.LEADS_ACCOUNT_DETAILS_UPDATES") \
    .filter(col("customer_email").isNotNull()) \
    .select("customer_email", "customer_name", "engagement_pattern", "final_health_score", "health_band") \
    .limit(5).show(truncate=False)

print("\n" + "="*80)
print("✅ GOLD LAYER COMPLETE — All spec-compliant tables built")
print("="*80)