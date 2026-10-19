# DEA Student Engagement Pipeline

A Databricks Medallion Architecture (Bronze → Silver → Gold) pipeline that transforms PostgreSQL CRM and engagement data into a **Customer Health Profile** for coaching clients.

---

## 1. Architecture Overview

The pipeline uses a three-layer medallion architecture:

**Bronze** — Raw data landing zone. 9 PostgreSQL source tables are loaded into Delta tables via JDBC. Only current-day records are loaded each run (daily append, no CDC or MERGE).

**Silver** — Cleaned and normalized data. Each bronze table is processed by a pipeline notebook that calls shared utils to parse JSON, flatten structs, extract custom fields, explode arrays into child tables, and deduplicate snapshots. Additional silver tables were built for specific Gold-layer needs (email body text, custom lead fields).

**Gold** — Business-ready reporting. Three gold notebooks build: (1) Leads Activities Summary + CRM users, (2) Sales Details with CSM resolution, and (3) the Customer Health Profile with all 7 engagement/sentiment metrics, scoring logic, and health bands. Health is per-source LEAD_ID (no identity consolidation), with email array lookup support.

### Data Flow

```
PostgreSQL (raw schema)
    |
    v  JDBC, daily append (WHERE DATE(INSERT_DATE) = CURRENT_DATE)
BRONZE (crm_ingestion.bronze)    9 raw Delta tables
    |
    v  %run utils -> process_table() -> parse_bronze_table -> flatten_structs -> write_silver
SILVER (crm_ingestion.silver)    11 processing notebooks (parent + child + custom fields)
    |
    v  3 Gold notebooks: metrics CTEs -> flags -> patterns -> scores -> health bands
GOLD (crm_ingestion.gold)        close_crm_users, leads_activities_summary, sales_details,
                                 customer_health_profile (per LEAD_ID, email array lookup)
    |
    v  Databricks Job: 15 tasks, daily 8:00 AM EST (bronze -> 11 silver parallel -> 3 gold sequential)
```

---

## 2. Folder Structure

```
student-engagement/
+- 00_README_.md                              <- This file
+- utils.py                                   <- Shared utility (process_table, parse_bronze_table, etc.)
+- 1_Bronze/
|  +- 1_1_Bronze_config                       <- JDBC connection settings
|  +- 1_2_Bronze_ingestion                    <- Loop over 9 source tables, append-only load
+- 2_Silver/                                  <- 11 processing notebooks (all call %run ../utils)
|  +- 2_1_01_Silver_leads_processed            <- leads_raw -> leads_processed
|  +- 2_1_02_Silver_leads_processed_custom     <- leads_processed -> custom fields extracted
|  +- 2_1_03_01_Silver_leads_processed_contacts <- leads_raw -> contacts array exploded
|  +- 2_1_03_02_Silver_leads_processed_contacts_emails <- contacts -> emails exploded
|  +- 2_2_01_Silver_lead_activites_processed    <- lead_activites_raw -> lead_activites_processed
|  +- 2_2_02_Silver_lead_activites_email_body   <- Email body text from body_text_quoted
|  +- 2_3_Silver_close_crm_users_processed      <- close_crm_users_raw -> close_crm_users_processed
|  +- 2_4_Silver_custom_activites_processed     <- custom_activites_raw -> custom_activites_processed
|  +- 2_5_Silver_calendly_scheduled_events_processed <- calendly events processed
|  +- 2_6_Silver_student_sentiment_processed    <- student sentiment processed
|  +- 2_7_Silver_mdl_users_processed            <- mdl_users_raw -> mdl_users_processed
|  +- 2_8_Silver_user_mapping                   <- Exploratory identity analysis (not in job)
+- 3_Gold/
   +- 3_1_Gold_Leads_Activities_Summary         <- CRM users + leads activities summary
   +- 3_2_Gold_Sales_Details                    <- Sales details with CSM resolution
   +- 3_3_Gold_Customer_Health_Profile           <- Customer Health Profile (all 7 metrics + scoring)
```

---

## 3. Full Execution Order

```
Step 1: Bronze ingestion (1_2_Bronze_ingestion)
            9 PostgreSQL tables -> crm_ingestion.bronze (append-only)
            |
            v
Step 2: 11 Silver notebooks (all run in parallel, depend on Bronze)
            Each calls: %run ../utils -> process_table('bronze_table', 'silver_table')
            - 2_1_01: leads_processed
            - 2_1_02: leads_processed_custom
            - 2_1_03_01/02: contacts + emails
            - 2_2_01: lead_activites_processed
            - 2_2_02: lead_activites_email_body
            - 2_3: close_crm_users_processed
            - 2_4: custom_activites_processed
            - 2_5: calendly_scheduled_events_processed
            - 2_6: student_sentiment_processed
            - 2_7: mdl_users_processed
            |
            v
Step 3: Gold (sequential dependencies)
            3_1_Gold_Leads_Activities_Summary (depends on all silver)
                -> gold.close_crm_users_processed, gold.leads_activities_summary
                |
                v
            3_2_Gold_Sales_Details (depends on 3_1)
                -> gold.sales_details
                |
                v
            3_3_Gold_Customer_Health_Profile (depends on 3_2 + all silver)
                -> gold.customer_health_profile (7 metrics + scoring + health bands)
            |
            v
Orchestrated by: Databricks Job (ID: 299306717398586)
            15 tasks, daily 8:00 AM EST, serverless compute
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

### 5.4 `2_8_Silver_user_mapping` (Exploratory — Not in Pipeline)

| Field | Value |
|---|---|
| **Notebook** | `2_8_Silver_user_mapping` |
| **Full Name** | N/A (outputs to temp views, no persistent table) |
| **Purpose** | Exploratory identity analysis to determine if different LEAD_IDs could be linked to the same person |

**What it does:**
1. Compares leads by email, phone, and display_name to find potential duplicate identities across LEAD_IDs
2. Explores whether a `master_user_id` grouping could consolidate related leads into a single customer record
3. Analyzes match rates between `leads_processed`, `leads_processed_contacts`, `mdl_users`, and `student_sentiment`
4. Tests join feasibility between sentiment data (Channel_ID) and CRM leads (LEAD_ID)

**Why it's NOT used in the pipeline:**
* Project requirements explicitly prohibit merging or canonicalizing Lead IDs — each LEAD_ID must remain independent
* The Customer Health Profile is built per-source LEAD_ID (one row per lead, no grouping or consolidation)
* Health scores and metrics are calculated independently for each LEAD_ID
* Email lookup is handled via `array_contains(emails, email)` on the emails array column in `customer_health_profile` — no identity merge needed
* Direct `student_sentiment` join was found infeasible (Channel_ID formats don't match LEAD_ID or contact IDs), so sentiment metrics use `leads_processed_custom` instead

**Status:** Retained for reference and future exploratory analysis only. NOT included in the Databricks Job (15 tasks) and does NOT produce any silver or gold tables. The notebook documents why identity consolidation was explored and ultimately rejected per spec.

---

## 6. Gold Layer (Complete)

### 6.1 Notebooks (3 total)

| # | Notebook | Purpose | Outputs |
|---|---|---|---|
| 1 | `3_1_Gold_Leads_Activities_Summary` | CRM users + leads activities summary | `gold.close_crm_users_processed`, `gold.leads_activities_summary` |
| 2 | `3_2_Gold_Sales_Details` | Sales context with CSM name resolution | `gold.sales_details` |
| 3 | `3_3_Gold_Customer_Health_Profile` | All 7 metrics + scoring + health bands | `gold.customer_health_profile` |

Dependencies: 3_1 → 3_2 → 3_3 (sequential). All 3 depend on Silver layer.

### 6.2 Engagement Metrics (7 CTEs, Cells 2-8 in 3_3)

All metrics are per-source LEAD_ID (no identity consolidation). Email lookup via `array_contains(emails, email)` on collected emails array.

| # | Metric | Source | Description |
|---|---|---|---|
| 1 | Email recency | `lead_activites_processed` + `lead_activites_email_body` | Days since last inbox email (excludes Calendly/system emails) |
| 2 | Meeting recency (CRM) | `lead_activites_email_body` | Days since last Calendly scheduling email |
| 3 | Meeting recency (Calendly) | `calendly_scheduled_events` | Days since last Calendly event |
| 4 | Missed calls | `lead_activites_processed` | Count of missed inbound calls |
| 5 | Upcoming meetings | `calendly_scheduled_events` | Days to next upcoming meeting |
| 6 | Platform login | `mdl_users` | Days since last login (matched via contacts.name = USERNAME) |
| 7 | Sentiment/Message recency | `leads_processed_custom` | Days since last message from client/team + sentiment |

**Note:** Direct `student_sentiment` join is not possible — Channel_ID formats are incompatible. Sentiment metrics use `leads_processed_custom` (CRM pre-calculated values).

### 6.3 Health Score Logic (Cell 9 in 3_3)

The final query combines all 7 metrics with sales context via multi-level CTE layering:

1. **health_metrics** — Join all metrics + sales details + lead names/emails array
2. **flags** — Binary flags: email_engaged (≤14d), meeting_engaged (≤30d), platform_engaged (≤30d), slack_engaged (sentiment present)
3. **patterns** — Engagement pattern from flag combinations
4. **scores** — Base score + missed call adjustment (-5/call, min 0) + sentiment adjustment

### 6.4 Health Bands

| Score | Band |
|---|---|
| >= 75 | GOOD |
| 50-74 | AVERAGE |
| < 50 | POOR |
| No data | NO DATA |

### 6.5 Table Schema

`gold.customer_health_profile` supports direct `array_contains(email)` lookup:
- `lead_id` — primary key (per-source, no consolidation)
- `lead_name`, `display_name`, `emails` (array) — identity + lookup
- `csm_name` — resolved from `gold.close_crm_users_processed`
- 7 metric columns + engagement flags + pattern + base_score + adjustments
- `final_health_score`, `health_band`, `sentiment` (normalized for display)

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
| + | `crm_ingestion.silver.leads_processed_contacts` | `silver.leads_processed` | Contacts array exploded (one row per contact) |
| + | `crm_ingestion.silver.leads_processed_contacts_emails` | `silver.leads_processed_contacts` | Contact emails exploded (one row per email) |
| + | `crm_ingestion.silver.lead_activites_email_body` | `bronze.lead_activites_raw` | Email body text from `body_text_quoted` (1,553,927 rows) |

### Gold Layer (complete)

| # | Full Name | Type | Description |
|---|---|---|---|
| 1 | `crm_ingestion.gold.close_crm_users_processed` | TABLE | CRM user dimension (from 3_1) |
| 2 | `crm_ingestion.gold.leads_activities_summary` | TABLE | Activities summary per lead (from 3_1) |
| 3 | `crm_ingestion.gold.sales_details` | TABLE | Sales context with CSM resolution (from 3_2) |
| 4 | `crm_ingestion.gold.customer_health_profile` | TABLE | Customer Health Profile — all metrics + scoring (from 3_3) |

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

## 8.5 Databricks Job Orchestration

| Field | Value |
|---|---|
| **Job Name** | Student Engagement Pipeline |
| **Job ID** | 299306717398586 |
| **Schedule** | Daily at 8:00 AM EST (`0 0 8 * * ?`, America/New_York) |
| **Compute** | Serverless (auto-provisioned per task) |
| **Tasks** | 15 total (1 bronze + 11 silver + 3 gold) |
| **Dependencies** | Bronze → 11 silver (parallel) → 3 gold (sequential: 3_1 → 3_2 → 3_3) |
| **Run if** | ALL_SUCCESS (downstream tasks skip if any upstream fails) |
| **Max concurrent runs** | 1 |

**How to run manually:** Click "Run" on the job page, or use `databricks jobs run-now 299306717398586`.

**How to repair a failed run:** `databricks jobs repair-run <run_id> --rerun-all-failed-tasks --rerun-dependent-tasks` — re-runs only failed tasks and their dependents, preserving successful results.

**Note:** The job is a Databricks workspace asset — it is NOT stored in the Git repo. Only the notebook files are version-controlled. The job references notebooks by their workspace paths.

---

## 9. Health Score Logic (Implemented)

The self-calculated metrics feed into the health score. The spec defines:

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

* Bronze layer: 9 tables ingested from PostgreSQL (append-only, 3-cell notebook)
* Silver layer: 11 processing notebooks + `utils.py` (shared `process_table` function)
* Gold layer: 3 notebooks — 3_1 (activities + CRM users), 3_2 (sales details), 3_3 (customer health profile)
* All 7 engagement/sentiment metrics implemented and verified in 3_3 (Cells 2-8)
* Health score logic fully implemented (flags → patterns → base score → adjustments → bands)
* `gold.customer_health_profile` table supports email array lookup
* Databricks Job created (ID: 299306717398586): 15 tasks, daily 8:00 AM EST, serverless compute
* Pipeline successfully tested end-to-end (all 15 tasks succeeded)
* All changes committed and pushed to GitHub (commit `209f3fe`, Oct 18)

### Notes

* Health is per-source LEAD_ID — no identity consolidation per spec requirements
* Sentiment metrics use `leads_processed_custom` — direct `student_sentiment` join not possible (incompatible Channel_ID formats)
* Platform login matches 23 Coaching Clients via `contacts.name = mdl_users.USERNAME` (emails are masked)
* Upcoming meetings metric may return 0 rows if all upcoming events are in the past relative to run date
* `2_8_Silver_user_mapping` notebook (section 5.4) explores whether different LEAD_IDs could be linked to the same person via email/phone/name matching, and tests sentiment-to-lead join feasibility. It is NOT part of the job or deliverables — project requirements prohibit Lead ID consolidation, and the notebook documents why this approach was explored and rejected