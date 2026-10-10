# DEA Student Engagement Pipeline

A Databricks Medallion Architecture (Bronze → Silver → Gold) pipeline that transforms PostgreSQL CRM and engagement data into a **Customer Health Profile** for coaching clients.

---

## 1. Architecture Overview

The pipeline uses a three-layer medallion architecture:

**Bronze** — Raw data landing zone. 9 PostgreSQL source tables are loaded into Delta tables via JDBC. Only current-day records are loaded each run (daily append, no CDC or MERGE).

**Silver** — Cleaned and normalized data. Each bronze table is processed by a pipeline notebook that calls shared utils to parse JSON, flatten structs, extract custom fields, explode arrays into child tables, and deduplicate snapshots. Additional silver tables were built for specific Gold-layer needs (email body text, custom lead fields).

**Gold** — Business-ready reporting. A base view filters to coaching clients and joins pre-calculated CRM custom fields. Engagement metrics are calculated step-by-step from raw activity data. Currently 2 of the spec's engagement metrics have been self-calculated; the rest remain to be built.

### Data Flow

```
PostgreSQL (raw schema)
    |
    v  JDBC, daily append (WHERE DATE(INSERT_DATE) = CURRENT_DATE)
BRONZE (crm_ingestion.bronze)    9 raw Delta tables
    |
    v  %run utils -> parse_bronze_table -> flatten_structs -> write_silver + explode children
SILVER (crm_ingestion.silver)    9 parent tables + child tables + 2 additional tables
    |
    v  Deduplicate, LEFT JOIN custom fields, calculate engagement metrics from raw activities
GOLD (crm_ingestion.gold)        view_customer_health_base (1,584 coaching clients) + metric queries
```

---

## 2. Folder Structure

```
student-engagement/
+- 00_README_.md                           <- This file
+- utils                                  <- Shared utility notebook (JSON repair, schema discovery, silver writing)
+- Dea Student Engagement Pipeline/
|  +- Silver_table_pipeline_9_raw/        <- 9 silver pipeline notebooks (one per bronze table)
|  |  +- 1_leads_raw                       <- Pipeline 1: leads_raw -> leads_processed + 17 child tables
|  |  +- 2_lead_activites_raw              <- Pipeline 2: lead_activites_raw -> lead_activites_processed + child tables
|  |  +- 3_close_crm_users_raw             <- Pipeline 3: close_crm_users_raw -> close_crm_users_processed
|  |  +- 4_custom_activites_raw            <- Pipeline 4: custom_activites_raw -> custom_activites_processed
|  |  +- 5_all_payments                    <- Pipeline 5
|  |  +- 6_calendly_scheduled_events       <- Pipeline 6
|  |  +- 7_student_sentiment               <- Pipeline 7
|  |  +- 8_mdl_users_raw                   <- Pipeline 8
|  |  +- 9_lead_merges                      <- Pipeline 9
+- 2_Silver/
|  +- 2_1_02_Silver_leads_processed_custom  <- Additional: custom fields extracted from leads_processed
|  +- 2_2_02_Silver_lead_activites_body_text <- Additional: email body text extracted from body_text_quoted
+- 3_Gold/
   +- 3_3_Gold_Customer_Health_Profile      <- Customer Health Profile (current focus)
```

---

## 3. Full Execution Order

```
Step 1: Bronze config + ingestion (9 tables from PostgreSQL)
            |
            v
Step 2: 9 Silver pipeline notebooks (one per bronze table)
            Each calls: %run utils -> process_table('bronze_table', 'silver_table')
            Creates: parent silver table + child tables (arrays exploded, custom fields extracted)
            |
            v
Step 3: Additional silver notebooks (as needed for Gold)
            - 2_1_02: leads_processed_custom (custom fields for health profile)
            - 2_2_02: lead_activites_email_body (email body text for meeting detection)
            |
            v
Step 4: Gold notebook (3_3_Gold_Customer_Health_Profile)
            - Create view_customer_health_base (coaching clients + custom fields)
            - Calculate engagement metrics from raw activities (in progress)
            - Join all metrics to base view (pending)
```

---

## 4. Bronze Layer (9 tables)

| # | Bronze Table | Source | Used Downstream | Description |
|---|---|---|:---:|---|
| 1 | `crm_ingestion.bronze.leads_raw` | `raw.leads_raw` | Yes | Lead/contact/account data |
| 2 | `crm_ingestion.bronze.lead_activites_raw` | `raw.lead_activites_raw` | Yes | CRM activities (Email, Call, Meeting, Note, CustomActivity, SMS) |
| 3 | `crm_ingestion.bronze.close_crm_users_raw` | `raw.close_crm_users_raw` | Yes | CRM user dimension |
| 4 | `crm_ingestion.bronze.custom_activites_raw` | `raw.custom_activites_raw` | Yes | Custom activity metadata (types, fields, outcomes) |
| 5 | `crm_ingestion.bronze.all_payments` | `raw.all_payments` | No | Payment records |
| 6 | `crm_ingestion.bronze.calendly_scheduled_events` | `raw.calendly_scheduled_events` | Yes | Scheduled meeting events |
| 7 | `crm_ingestion.bronze.student_sentiment` | `raw.student_sentiment` | Yes | Sentiment and Slack message data |
| 8 | `crm_ingestion.bronze.mdl_users_raw` | `raw.mdl_users_raw` | Yes | Moodle platform users |
| 9 | `crm_ingestion.bronze.lead_merges` | `raw.lead_merges` | No | Lead merge history |

All bronze tables have 3 columns: `raw_data` (JSON string), `insert_date`, `bronze_updated_at`.

---

## 5. Silver Layer

### 5.1 Utils Notebook (`utils`)

| Field | Value |
|---|---|
| **Type** | Notebook (Python utility library, 10 cells) |
| **Path** | `/Users/scottsbv@gmail.com/student-engagement/utils` |
| **Purpose** | Shared functions for JSON repair, schema discovery, and silver table writing |

**Key functions:**

| Function | Cell | Description |
|---|---|---|
| `repair_json_udf` | 3 | UDF that repairs malformed JSON: handles JSON_OBJECT wrapper, "data" key extraction, single-quote → double-quote conversion, apostrophe preservation, and problem key nulling |
| `parse_bronze_table()` | 4 | Reads bronze table, runs UDF to repair JSON, writes to temp table, infers schema via `schema_of_json()` on 20 sampled records, parses with `from_json()`, explodes top-level array |
| `flatten_structs()` | 5 | Flattens StructType columns into individual columns (skips `custom` struct for separate extraction) |
| `write_silver()` | 7 | Writes DataFrame to silver Delta table with MD5 `record_hash` column; supports CDC mode (delete+insert) |
| `explode_to_child()` | 8 | Explodes array columns into child tables (recurses on nested arrays) |
| `extract_custom()` | 9 | Extracts custom struct + `custom_cf_` fields into child table; builds `custom_fields` MAP column on parent |
| `process_table()` | 10 | Orchestrator: parse → flatten → write parent → explode children → extract custom |

**`_PROBLEM_KEYS`** — The UDF intentionally nulls these free-text keys to prevent JSON parsing failures: `description`, `notes`, `body_text`, `subject`, `text`, `note`. These fields contain single quotes and special characters that break JSON parsing. Note: for `lead_activites_raw` specifically, the UDF takes the "data" path which skips problem key nulling — the fields are lost during schema inference instead, not by problem key nulling.

**Schema discovery limitation** — `schema_of_json()` infers the schema from 20 random records. If those records don't include all activity types, type-specific fields (e.g., email body fields) are missing from the inferred schema. `lead_activites_processed` has 76 of the 135 fields that `parse_bronze_table` discovers — the missing 59 are email-specific fields lost to random sampling.

**Usage:** Pipeline notebooks call `%run /Users/scottsbv@gmail.com/student-engagement/utils` then `process_table('bronze_table', 'silver_table')`.

---

### 5.2 Pipeline Silver Tables (9 parent tables + child tables)

Each pipeline notebook reads one bronze table, parses JSON via utils, and writes a parent silver table plus child tables for array columns and custom fields.

| # | Pipeline Notebook | Bronze Table | Silver Parent Table | Key Child Tables |
|---|---|---|---|---|
| 1 | `1_leads_raw` | `leads_raw` | `leads_processed` | addresses, contacts (emails, phones, urls, integration_links), opportunities (attachments, integration_links), tasks, custom (Add_ons, HADES TYPE, Lead Source, Objections Faced, Reactivation Campaign) — 17 child tables total |
| 2 | `2_lead_activites_raw` | `lead_activites_raw` | `lead_activites_processed` | 27 child tables + 2 grandchildren (attendees, attachments, coach_legs, recording_history, integrations, etc.) |
| 3 | `3_close_crm_users_raw` | `close_crm_users_raw` | `close_crm_users_processed` | (child tables from user arrays) |
| 4 | `4_custom_activites_raw` | `custom_activites_raw` | `custom_activites_processed` | (3,530 rows — activity type metadata) |
| 5 | `5_all_payments` | `all_payments` | `all_payments` | (payment records) |
| 6 | `6_calendly_scheduled_events` | `calendly_scheduled_events` | `calendly_scheduled_events` | (12.2M rows — meeting events) |
| 7 | `7_student_sentiment` | `student_sentiment` | `student_sentiment` | (sentiment + Slack data) |
| 8 | `8_mdl_users_raw` | `mdl_users_raw` | `mdl_users` | (platform users) |
| 9 | `9_lead_merges` | `lead_merges` | `lead_merges` | (lead merge history) |

**`lead_activites_processed` key columns (76 total):** `id`, `lead_id`, `type`, `status`, `activity_at`, `date_updated`, `date_created`, `bronze_insert_date`, `bronze_updated_at`, `note`, `note_html`, `text` (SMS text, NOT email body), `direction`, `duration`, `disposition`, `outcome_id`, `phone`, `users`, `coach_legs`, `recording_history`, `record_hash`, etc.

**Important:** The `text` column in `lead_activites_processed` is SMS text messages (3.2M rows have values). For emails, `text` is always NULL — email body text lives in `body_text_quoted` which was not captured by schema inference.

---

### 5.3 Additional Silver Tables

#### `leads_processed_custom`

| Field | Value |
|---|---|
| **Notebook** | `2_1_02_Silver_leads_processed_custom` |
| **Full Name** | `crm_ingestion.silver.leads_processed_custom` |
| **Source** | `crm_ingestion.silver.leads_processed` (custom fields extracted) |
| **Purpose** | Pre-calculated CRM custom fields for the health profile |

**Key columns:** `leads_processed_id` (FK to `leads_processed.id`), `DAYS_SINCE_LAST_EMAIL`, `DAYS_SINCE_LAST_LOGIN`, `DAYS_SINCE_LAST_MEETING`, `DAYS_SINCE_LAST_MESSAGE_FROM_CLIENT`, `DAYS_SINCE_LAST_MESSAGE_FROM_TEAM_MEMBER`, `SLACK_ENGAGED_FLAG`, `PLATFORM_ENGAGED_FLAG`, `MEETING_ENGAGED_FLAG`, `TOTAL_MISSED_CALLS`, `ENGAGEMENT_PATTERN`, `FINAL_SCORE`, `HEALTH_BAND`, `CSM`, `Contracted_Value`, `Tier`, `SENTIMENTS_LAST_30_DAYS`, `Avatar`.

**Note:** These are Close CRM's pre-calculated values. The Gold layer is building self-calculated versions from raw activity data to validate and replace these.

---

#### `lead_activites_email_body` (NEW)

| Field | Value |
|---|---|
| **Notebook** | `2_2_02_Silver_lead_activites_body_text` |
| **Full Name** | `crm_ingestion.silver.lead_activites_email_body` |
| **Source** | `crm_ingestion.bronze.lead_activites_raw` (parsed via `parse_bronze_table`) |
| **Row count** | 1,553,927 |
| **Purpose** | Extracts email body text from the nested `body_text_quoted` array field that `lead_activites_processed` misses |

**Key columns:** `bronze_insert_date`, `bronze_updated_at`, `activity_id` (FK to `lead_activites_processed.id`), `lead_id`, `activity_at`, `status`, `body_text`, `record_hash`.

**Why this table exists:** `lead_activites_processed` drops `body_text_quoted` during schema inference (76 of 135 fields captured). The `body_text_quoted[0].text` field contains the actual email body, including Calendly's "A new event has been scheduled." message used to detect meetings. This table fills the gap by using `parse_bronze_table` (which DOES discover `body_text_quoted` in its 135-field schema) and extracting `body_text_quoted[0].text` as `body_text`.

---

## 6. Gold Layer (Current State)

### 6.1 Notebook: `3_3_Gold_Customer_Health_Profile`

| Field | Value |
|---|---|
| **Type** | Notebook (SQL + Python) |
| **Path** | `/Users/scottsbv@gmail.com/student-engagement/3_Gold/3_3_Gold_Customer_Health_Profile` |
| **Purpose** | Build the Customer Health Profile step-by-step from raw activity data |

### 6.2 Base View: `view_customer_health_base`

| Field | Value |
|---|---|
| **Full Name** | `crm_ingestion.gold.view_customer_health_base` |
| **Cell** | 5 (CREATE OR REPLACE VIEW) |
| **Row count** | 1,584 coaching clients |
| **Purpose** | Foundation for the health profile — deduplicated coaching clients with pre-calculated CRM custom fields |

**Logic:**
1. Deduplicate `leads_processed` by `id` (ROW_NUMBER, latest by `date_updated DESC, bronze_insert_date DESC`), filter to `status_label = 'Coaching Client'`
2. Deduplicate `leads_processed_custom` by `leads_processed_id` (ROW_NUMBER, latest by `bronze_insert_date DESC`)
3. LEFT JOIN leads to custom fields on `id = leads_processed_id`
4. Clean `\N` values to NULL using `NULLIF` on all custom field columns

**Key columns:** `id`, `status_label`, `date_updated`, `bronze_insert_date`, plus all pre-calculated CRM custom fields (DAYS_SINCE_LAST_EMAIL, DAYS_SINCE_LAST_MEETING, TOTAL_MISSED_CALLS, ENGAGEMENT_PATTERN, FINAL_SCORE, HEALTH_BAND, CSM, Contracted_Value, Tier, etc.)

---

### 6.3 Self-Calculated Engagement Metric: DAYS_SINCE_LAST_EMAIL

| Field | Value |
|---|---|
| **Cell** | 12 |
| **Row count** | 5,497 leads |
| **Source tables** | `lead_activites_processed` + `lead_activites_email_body` (LEFT JOIN) |

**Logic:**
1. Filter `lead_activites_processed` to `type = 'Email' AND status = 'inbox'`
2. LEFT JOIN to `lead_activites_email_body` on `id = activity_id` to get email body text
3. Exclude Calendly/system emails: `body_text NOT ILIKE '%A new event has been scheduled.%'`, `NOT ILIKE '%is requesting access to the following folder%'`, `NOT ILIKE '%requests access to an item%'`
4. Deduplicate by activity `id` (ROW_NUMBER, `PARTITION BY id ORDER BY date_updated DESC, bronze_insert_date DESC`, keep rn=1)
5. Find latest email per lead (ROW_NUMBER, `PARTITION BY lead_id ORDER BY activity_at DESC`, keep rn=1)
6. Calculate `DATEDIFF(CURRENT_DATE, DATE(activity_at)) AS days_since_last_email`

**Impact of exclusions:** Without the `body_text` join, Calendly scheduling emails were counted as "last email" for 1,844 leads. The exclusion reduced the result from 7,341 to 5,497 leads.

---

### 6.4 Self-Calculated Engagement Metric: DAYS_SINCE_LAST_MEETING

| Field | Value |
|---|---|
| **Cell** | 15 |
| **Row count** | 6,226 leads |
| **Source table** | `lead_activites_email_body` |

**Logic:**
1. Filter `lead_activites_email_body` to `body_text ILIKE '%A new event has been scheduled.%'` (Calendly scheduling emails)
2. Deduplicate by `activity_id` (ROW_NUMBER, `PARTITION BY activity_id ORDER BY bronze_updated_at DESC, bronze_insert_date DESC`, keep rn=1)
3. Find latest meeting per lead (ROW_NUMBER, `PARTITION BY lead_id ORDER BY activity_at DESC`, keep rn=1)
4. Calculate `DATEDIFF(CURRENT_DATE, DATE(activity_at)) AS days_since_last_meeting`

**Key finding:** The spec says to search for "A new event has been scheduled" in the activity text. This text does NOT exist in `lead_activites_processed.text` (which is SMS text, NULL for emails). It lives inside `body_text_quoted[0].text` in the raw JSON — a nested array field that was lost during silver schema inference. The `lead_activites_email_body` table was created specifically to extract this text.

---

## 7. Full Table Reference

### Config Layer

| Table | Full Name | Description |
|---|---|---|
| `bronze_config` | `crm_ingestion.config.bronze_config` | Metadata controlling Bronze ingestion |

### Bronze Layer (9 tables)

All bronze tables: `raw_data` (string/JSON), `insert_date` (timestamp), `bronze_updated_at` (timestamp).

| # | Full Name | Source | Used |
|---|---|---|:---:|
| 1 | `crm_ingestion.bronze.leads_raw` | `raw.leads_raw` | Yes |
| 2 | `crm_ingestion.bronze.lead_activites_raw` | `raw.lead_activites_raw` | Yes |
| 3 | `crm_ingestion.bronze.close_crm_users_raw` | `raw.close_crm_users_raw` | Yes |
| 4 | `crm_ingestion.bronze.custom_activites_raw` | `raw.custom_activites_raw` | Yes |
| 5 | `crm_ingestion.bronze.all_payments` | `raw.all_payments` | No |
| 6 | `crm_ingestion.bronze.calendly_scheduled_events` | `raw.calendly_scheduled_events` | Yes |
| 7 | `crm_ingestion.bronze.student_sentiment` | `raw.student_sentiment` | Yes |
| 8 | `crm_ingestion.bronze.mdl_users_raw` | `raw.mdl_users_raw` | Yes |
| 9 | `crm_ingestion.bronze.lead_merges` | `raw.lead_merges` | No |

### Silver Layer (9 parent + child + 2 additional tables)

| # | Full Name | Source | Description |
|---|---|---|---|
| 1 | `crm_ingestion.silver.leads_processed` | `bronze.leads_raw` | Flattened lead dimension + 17 child tables |
| 2 | `crm_ingestion.silver.lead_activites_processed` | `bronze.lead_activites_raw` | Deduplicated activities (76 fields) + 27 child tables |
| 3 | `crm_ingestion.silver.close_crm_users_processed` | `bronze.close_crm_users_raw` | User dimension |
| 4 | `crm_ingestion.silver.custom_activites_processed` | `bronze.custom_activites_raw` | Custom activity metadata |
| 5 | `crm_ingestion.silver.all_payments` | `bronze.all_payments` | Payment records |
| 6 | `crm_ingestion.silver.calendly_scheduled_events` | `bronze.calendly_scheduled_events` | 12.2M meeting events |
| 7 | `crm_ingestion.silver.student_sentiment` | `bronze.student_sentiment` | Sentiment + Slack data |
| 8 | `crm_ingestion.silver.mdl_users` | `bronze.mdl_users_raw` | Platform users |
| 9 | `crm_ingestion.silver.lead_merges` | `bronze.lead_merges` | Lead merge history |
| + | `crm_ingestion.silver.leads_processed_custom` | `silver.leads_processed` | CRM custom fields for health profile |
| + | `crm_ingestion.silver.lead_activites_email_body` | `bronze.lead_activites_raw` | Email body text from `body_text_quoted` (1,553,927 rows) |

### Gold Layer (current)

| # | Full Name | Type | Description |
|---|---|---|---|
| 1 | `crm_ingestion.gold.view_customer_health_base` | VIEW | 1,584 coaching clients with pre-calculated CRM custom fields |
| 2 | *(metric query)* | SQL cell 12 | `DAYS_SINCE_LAST_EMAIL` — 5,497 leads, self-calculated from raw activities |
| 3 | *(metric query)* | SQL cell 15 | `DAYS_SINCE_LAST_MEETING` — 6,226 leads, self-calculated from email body text |

---

## 8. Deduplication Rules

All deduplication uses ROW_NUMBER with `PARTITION BY <primary key> ORDER BY <timestamp> DESC` and keeps `rn = 1` (latest wins).

| Dataset | Primary Key | Ordering (latest wins) |
|---|---|---|
| Leads | `id` | `date_updated DESC` → `bronze_insert_date DESC` |
| CRM Activities (by ID) | `id` / `activity_id` | `date_updated DESC` → `bronze_insert_date DESC` |
| CRM Activities (per lead, latest) | `lead_id` | `activity_at DESC` |
| Custom Fields | `leads_processed_id` | `bronze_insert_date DESC` |
| Calendly Events | `EVENT_URI` | `INVITEE_UPDATED_AT` → `EVENT_CREATED_AT` → `INSERT_DATE` |
| Platform Users | `USER_ID` | `TIMEMODIFIED` |
| Student Sentiment | `CHANNEL_ID` | `INSERT_DATE` |
| CRM Users | `USER_ID` | `date_updated` |

---

## 9. Health Score Logic (Spec — Not Yet Implemented)

The self-calculated metrics will eventually feed into the health score. The spec defines:

**Formula:** `FINAL_HEALTH_SCORE = Base Engagement Score + Missed Call Adjustment + Sentiment Adjustment`

### Engagement Patterns and Base Scores

| Pattern | Base Score |
|---|---|
| SLACK + PLATFORM + MEETING | 90 |
| SLACK + PLATFORM | 80 |
| PLATFORM + MEETING | 75 |
| SLACK + MEETING | 60 |
| PLATFORM ONLY | 50 |
| MEETING ONLY | 40 |
| SLACK ONLY | 30 |
| NO ENGAGEMENT | 20 |

### Health Bands

| Score | Band |
|---|---|
| >= 75 | GOOD |
| >= 50 and < 75 | AVERAGE |
| < 50 | POOR |
| No data | NO DATA |

---

## 10. Current Status

### Completed

* Bronze layer: 9 tables ingested from PostgreSQL
* Silver layer: 9 pipeline notebooks processing all bronze tables via `process_table()`
* Silver layer: `leads_processed_custom` (CRM custom fields for health profile)
* Silver layer: `lead_activites_email_body` (email body text extraction — fills gap in `lead_activites_processed`)
* Gold layer: `view_customer_health_base` (1,584 coaching clients with pre-calculated CRM fields)
* Gold metric: `DAYS_SINCE_LAST_EMAIL` (5,497 leads, self-calculated with Calendly exclusions)
* Gold metric: `DAYS_SINCE_LAST_MEETING` (6,226 leads, self-calculated from Calendly scheduling emails)

### Remaining (per spec)

* `TOTAL_MISSED_CALLS` — count missed inbound calls from `lead_activites_processed` where `type = 'Call'`
* `DAYS_SINCE_LAST_LOGIN` — from `mdl_users` (platform login recency)
* `DAYS_SINCE_LAST_MEETING_CALENDLY` — from `calendly_scheduled_events` (separate from CRM meeting metric)
* `DAYS_SINCE_LAST_MESSAGE_FROM_CLIENT` — from Slack sentiment data
* `DAYS_SINCE_LAST_MESSAGE_FROM_TEAM_MEMBER` — from Slack sentiment data
* `UPCOMING_MEETING_DAYS` — from Calendly (future meetings)
* Join all metrics to `view_customer_health_base` by `lead_id`
* Calculate engagement flags, engagement pattern, health score, and health band
* Write final `gold.leads_account_details_updates` table
* Gold notebooks `3_1_Gold_Leads_Activities_Summary` and `3_2_Gold_Sales_Details` (not yet started)