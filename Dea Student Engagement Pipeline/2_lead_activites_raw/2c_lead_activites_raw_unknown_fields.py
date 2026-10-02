# Databricks notebook source
# DBTITLE 1,Load Silver Engine
%run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"
# COMMAND ----------
# DBTITLE 1,2c_lead_activites_raw_unknown_fields
# ==============================================================================
# EXTRACT UNKNOWN/NEW FIELDS (CDC-BASED SILENT OPERATOR)
# ==============================================================================
# Captures any NEW fields not in Cell 39 or Cell 39b
# Operates silently - only processes NEW/UPDATED records via CDC watermark
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "crm_ingestion"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.lead_activites_raw"
unknown_fields_table = f"{CATALOG}.{SILVER_SCHEMA}.lead_activites_raw_unknown_fields"

# Known fields from Cell 39 explicit schema
known_fields = {
    "id", "lead_id", "user_id", "contact_id", "activity_at", "date_created", "date_updated",
    "direction", "status", "source", "_type", "cost", "text", "note", "note_html",
    "note_date_updated", "user_name", "created_by", "updated_by", "created_by_name",
    "updated_by_name", "organization_id", "sequence_id", "sequence_name",
    "sequence_subscription_id", "template_id", "template_name", "error_message",
    "local_phone", "remote_phone", "local_phone_formatted", "remote_phone_formatted",
    "local_country_iso", "remote_country_iso", "agent_action_reason", "date_sent",
    "date_scheduled", "date_answered", "duration", "call_method", "disposition",
    "has_recording", "recording_url", "voicemail_url", "voicemail_duration",
    "recording_duration", "attachments", "attendees", "envelope", "integrations",
    "send_attempts", "summary", "to", "users", "attached_call_ids", "bcc", "cc",
    "calendar_event_uids", "coach_legs", "conference_links", "mentions", "message_ids",
    "note_mentions", "opens", "provider_calendar_ids", "recording_history", "references",
    "body_html", "body_html_quoted", "body_preview", "body_text", "body_text_quoted",
    "subject", "title", "email_account_id", "connected_account_id", "thread_id",
    "in_reply_to_id", "has_reply", "is_forwarded", "forwarded_to", "send_as_id",
    "sender", "need_smtp_credentials", "opens_summary", "phone", "is_to_group_number",
    "transferred_from", "transferred_from_user_id", "transferred_to",
    "transferred_to_user_id", "dialer_id", "dialer_saved_search_id", "starts_at",
    "ends_at", "actual_duration", "is_recurring", "is_joinable", "location",
    "calendar_event_link", "provider_calendar_event_id", "provider_calendar_type",
    "parent_meeting_id", "notetaker_id", "recording_expires_at", "user_note",
    "user_note_html", "user_note_date_updated", "user_note_mentions",
    "mentions_updated_at", "pinned", "pinned_at", "last_published_at",
    "followup_sequence_id", "followup_sequence_delay", "followup_sequence_add_cc_bcc",
    "bulk_email_action_id", "outcome_id", "outcome_reason", "outcome_autofill_confidence",
    "outcome_autofill_reasoning", "ai_draft", "playbook_id", "playbook_reason",
    "agent_config_id", "conversation_type_id", "conversation_type_reason",
    "source_lead_id", "source_display_name", "destination_lead_id",
    "destination_display_name", "import_id", "merge_status", "custom_activity_type_id"
}

# Get CDC watermark
def get_last_watermark(target_table):
    try:
        result = spark.sql(f"""
            SELECT MAX(insert_date) as max_insert_date
            FROM {target_table}
        """).collect()[0]
        return result['max_insert_date']
    except Exception:
        return None

last_watermark = get_last_watermark(unknown_fields_table)

# Filter bronze for new/updated records only
if last_watermark:
    df_bronze = spark.table(bronze_table).filter(F.col("insert_date") > last_watermark)
else:
    df_bronze = spark.table(bronze_table)

# Parse activities as map
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

# Filter for unknown fields (not in known_fields, not custom.cf_*)
df_unknown = df_exploded.filter(
    ~F.col("field_name").isin(list(known_fields)) &
    ~F.col("field_name").startswith("custom.cf_")
).select(
    F.col("activity_id"),
    F.col("field_name"),
    F.col("field_value"),
    F.col("insert_date").alias("first_seen_at")
)

# Write to table (append mode for CDC)
if df_unknown.count() > 0:
    df_unknown.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(unknown_fields_table)
