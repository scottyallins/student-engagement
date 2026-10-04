# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 2. PIPELINE - lead_activites_raw
# MAGIC
# MAGIC Bronze → Silver pipeline for `lead_activites_raw`. **30 tables** (parent + 27 children + 2 grandchildren): attached_call_ids, attachments, attendees, bcc, body_text_quoted, cc, calendar_event_uids, coach_legs (participation_history), conference_links, envelope (bcc, cc, from, reply_to, sender, to), integrations (artifacts, integration_data, participants), mentions, message_ids, note_mentions, opens, provider_calendar_ids, recording_history, references, send_attempts, to, user_note_mentions, users.
# MAGIC
# MAGIC **PK**: `activity_id` | **Source**: `crm_ingestion.bronze.lead_activites_raw`

# COMMAND ----------

# DBTITLE 1,Load Silver Engine
# MAGIC %run "/Users/scottsbv@gmail.com/student-engagement/Dea Student Engagement Pipeline/silver_engine_utils"

# COMMAND ----------

# DBTITLE 1,Run Pipeline
process_table('lead_activites_raw')