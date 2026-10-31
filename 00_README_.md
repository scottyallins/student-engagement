# DEA Student Engagement Pipeline

A Databricks Medallion Architecture (Bronze → Silver → Gold) pipeline that transforms PostgreSQL CRM and engagement data into a **Customer Health Profile** for coaching clients.

---

## 1. Architecture Overview

The pipeline uses a three-layer medallion architecture:

**Bronze** — Raw data landing zone. 9 PostgreSQL source tables are loaded into Delta tables via JDBC. Incremental watermark loads rows after the max insert_date already in bronze, automatically recovering missed days. Optional backfill widgets allow manual date-range loads. No CDC or MERGE. Each of the 9 loads is error-isolated — a failed source logs its error, the remaining tables still land, and the task fails at the end so the alert fires; the next run re-pulls exactly the missed rows.

**Silver** — Cleaned and normalized data. Each bronze table is processed by a pipeline notebook that calls shared utils to parse JSON, flatten structs, extract custom fields, explode arrays into child tables, and deduplicate snapshots. Additional silver tables were built for specific Gold-layer needs (email body text, custom lead fields).

**Gold** — Business-ready star schema. Fifteen notebooks build exactly one table each (detachment — every table can be re-run or recovered independently): two dimensions (`staff_dim`, `lead_dim`), three facts at three grains (`leads_activity_fact` at event grain, `sales_fact` at latest-sale-per-lead, `customer_health_history` as a daily snapshot), seven single-metric tables, a `lead_emails` lookup, a `health_metrics` staging join, and the `customer_health_profile` deliverable (scoring logic, health bands, MERGE SCD Type 1 target). Health is per-source LEAD_ID (no identity consolidation), with email array lookup support.

**Data volume:** Bronze holds 16.1 million rows across 9 tables. Silver expands the parsed result to roughly 48 million rows across 11 tables and their children. Gold distills this into 15 small tables (the health layer is ~1,600 rows per table). The job runs on serverless compute — actual usage from billing: ~3.5–5 DBUs/day at $0.35/DBU, about $1.25–1.75 per day including all development runs. A full 29-task run takes ~20 minutes.

### Data Flow

```
PostgreSQL (raw schema)
    |
    v  JDBC, incremental watermark (insert_date > MAX(insert_date) in bronze), error-isolated loader
BRONZE (crm_ingestion.bronze)    9 raw Delta tables (+ bronze_updated_at CDC stamp)
    |
    v  %run utils -> process_table() -> parse_bronze_table -> flatten_structs -> write_silver
SILVER (crm_ingestion.silver)    11 processing notebooks, one table each (bronze_updated_at watermark CDC)
    |
    v  15 Gold notebooks, one table each: 2 dims + 3 facts + 7 metrics + lookup + staging
GOLD (crm_ingestion.gold)        staff_dim, lead_dim, leads_activity_fact, sales_fact,
                                  7 metric tables, lead_emails, health_metrics,
                                  customer_health_profile (MERGE SCD1), customer_health_history (90-day)
    |
    v  Databricks Job: 29 tasks, daily 8:00 AM EST, serverless, defined as code in databricks.yml
```

---

## 2. Folder Structure

```
student-engagement/
+- 00_README_.md                              <- This file
+- databricks.yml                             <- Job-as-code: the nightly job as a Declarative Automation Bundle
+- DEPLOY.md                                  <- How to deploy the bundle
+- utils.ipynb                                <- Shared utility (process_table, parse_bronze_table, etc.)
+- 1_Bronze/
|  +- 1_1_Bronze_config                       <- Ingestion config table (source list + watermarks)
|  +- 1_2_Bronze_ingestion                    <- Config-driven loader: 9 error-isolated JDBC loads, incremental watermark + backfill widgets
+- 2_Silver/                                  <- 12 notebooks (all call %run ../utils)
|  +- 2_1_01_Silver_leads_processed            <- leads_raw -> leads_processed
|  +- 2_1_02_Silver_leads_processed_custom     <- leads_raw -> leads_processed_custom (custom fields)
|  +- 2_1_03_01_Silver_leads_processed_contacts <- leads_raw -> leads_processed_contacts
|  +- 2_1_03_02_Silver_leads_processed_contacts_emails <- contacts -> contacts_emails
|  +- 2_2_01_Silver_lead_activites_processed    <- lead_activites_raw -> lead_activites_processed
|  +- 2_2_02_Silver_lead_activites_email_body   <- lead_activites_raw -> lead_activites_email_body
|  +- 2_3_Silver_close_crm_users_processed      <- close_crm_users_raw -> close_crm_users_processed
|  +- 2_4_Silver_custom_activites_processed     <- custom_activites_raw -> custom_activites_processed
|  +- 2_5_Silver_calendly_scheduled_events_processed <- calendly -> calendly_scheduled_events_processed
|  +- 2_6_Silver_student_sentiment_processed    <- student_sentiment -> student_sentiment_processed
|  +- 2_7_Silver_mdl_users_processed            <- mdl_users_raw -> mdl_users_processed
|  +- 2_8_Silver_user_mapping                   <- Identity analysis (job task; writes no persistent tables)
+- 3_Gold/                                    <- 15 notebooks, ONE table per notebook (detachment)
   +- 3_01_Gold_staff_dim                      <- staff_dim (coaches, CSMs, setters/closers, partners)
   +- 3_02_Gold_leads_activity_fact            <- leads_activity_fact (one row per activity, event grain)
   +- 3_03_Gold_sales_fact                     <- sales_fact (latest qualifying sale per lead)
   +- 3_04_Gold_lead_dim                       <- lead_dim (names, status, CSM, contract, tier)
   +- 3_05_Gold_days_since_last_email          <- metric tables (one per notebook):
   +- 3_06_Gold_days_since_last_meeting       <-   meeting recency (CRM), Calendly recency,
   +- 3_07_Gold_missed_calls_14_days           <-   missed calls, upcoming meeting,
   +- 3_08_Gold_days_until_next_meeting        <-   login recency, sentiment
   +- 3_09_Gold_days_since_last_login         <-
   +- 3_10_Gold_days_since_last_meeting_calendly <-
   +- 3_11_Gold_sentiment                     <-
   +- 3_12_Gold_lead_emails                    <- lead -> emails array (array_contains lookup)
   +- 3_13_Gold_health_metrics                 <- staging join: lead_dim + sales_fact + staff_dim + all metrics
   +- 3_14_Gold_customer_health_profile        <- scoring, bands, MERGE SCD Type 1 on lead_id
   +- 3_15_Gold_customer_health_history       <- daily full snapshot, 90-day rolling window
```

---

## 3. Full Execution Order

```
Step 1: Bronze (2 tasks)
            1_1_Bronze_config -> config.bronze_config (source list + watermarks)
            1_2_Bronze_ingestion: 9 JDBC loads, error-isolated, incremental watermark
            |
            v
Step 2: Silver (11 tasks, parallel after bronze; contacts_emails waits for contacts)
            Each calls: %run ../utils -> process_table('bronze_table', 'silver_table')
            - 2_1_01: leads_processed
            - 2_1_02: leads_processed_custom
            - 2_1_03_01: leads_processed_contacts  ->  2_1_03_02: contacts_emails
            - 2_2_01: lead_activites_processed
            - 2_2_02: lead_activites_email_body
            - 2_3: close_crm_users_processed
            - 2_4: custom_activites_processed
            - 2_5: calendly_scheduled_events_processed
            - 2_6: student_sentiment_processed
            - 2_7: mdl_users_processed
            - 2_8: user_mapping (after all silver; writes no persistent tables)
            |
            v
Step 3: Gold (15 tasks, one table per notebook)
            Parallel after silver:  3_01 staff_dim, 3_04 lead_dim, 3_05-3_11 metrics, 3_12 lead_emails
            After staff_dim:        3_02 leads_activity_fact, 3_03 sales_fact
            After dims/facts/metrics: 3_13 health_metrics (staging join)
            3_13 -> 3_14 customer_health_profile (scoring + MERGE SCD Type 1 on lead_id)
            3_14 -> 3_15 customer_health_history (daily snapshot, 90-day retention)
            |
            v
Orchestrated by: Databricks Job — 29 tasks, daily 8:00 AM EST, serverless compute,
            defined as code in databricks.yml (see DEPLOY.md)
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
| **Purpose** | Shared functions for JSON detection and repair, schema discovery, silver table writing, and array explosion |

**Key functions:**

| Function | Cell | Description |
|---|---|---|
| `repair_json_udf` | 3 | UDF that detects and repairs malformed JSON using type-based unwrapping: if any value is a string starting with `{` or `[`, treats it as stringified inner JSON, repairs single quotes, and recurses; if a dict has one key with a list value, unwraps the list. Handles JSON_OBJECT double wrapper and data array wrapper without hard-coded key names. Nulls problem keys before quote replacement |
| `parse_bronze_table()` | 4 | Reads bronze table, detects clean vs dirty JSON. Clean JSON bypasses UDF. Dirty rows repaired once via UDF into temp Delta table. Infers schema via `schema_of_json_agg()` on up to 200 random rows, with fallback to single-row `schema_of_json()`. Parses with `from_json()`, explodes top-level array. Includes field-level all-NULL validation that aborts write if all fields are NULL (wrong schema) |
| `flatten_structs()` | 5 | Flattens StructType columns into individual columns (skips `custom` struct for separate extraction) |
| `write_silver()` | 7 | Writes DataFrame to silver Delta table with `overwriteSchema=true`; incremental CDC mode appends only rows newer than MAX(bronze_updated_at) in silver |
| `explode_to_child()` | 8 | Explodes array columns into child tables (recurses on nested arrays) |
| `extract_custom()` | 9 | Extracts custom struct + `custom_cf_` fields into child table; builds `custom_fields` MAP column on parent |
| `process_table()` | 10 | Orchestrator: parse → flatten → write parent → explode children → extract custom |

**`_PROBLEM_KEYS`** — The UDF intentionally nulls these free-text keys to prevent JSON parsing failures: `description`, `notes`, `body_text`, `subject`, `text`, `note`. These fields contain single quotes and special characters that break JSON parsing. The type-based unwrapping handles both the JSON_OBJECT double wrapper and the top-level data array wrapper without hard-coded key names. The email body content that gold needs lives in a separate field (`body_text_quoted`, an array of structs) that is not in the problem-key set and is extracted by a dedicated notebook.

**Schema discovery limitation** — `schema_of_json_agg()` infers the schema from up to 200 random records, with fallback to single-row `schema_of_json()` when the aggregate returns `ARRAY<STRING>` or risks driver OOM. If those records don't include all activity types, type-specific fields (e.g., email body fields) may be missing from the inferred schema. `lead_activites_processed` has 76 of the 135 fields that `parse_bronze_table` discovers — the missing 59 are email-specific fields lost to random sampling.

**Usage:** Pipeline notebooks call `%run /Users/scottsbv@gmail.com/student-engagement/utils` then `process_table('bronze_table', 'silver_table')`.

---

### 5.2 Pipeline Silver Tables (7 parent tables + child tables)

Each pipeline notebook reads one bronze table, parses JSON via utils, and writes a parent silver table plus child tables for array columns and custom fields.

| # | Pipeline Notebook | Bronze Table | Silver Parent Table | Key Child Tables |
|---|---|---|---|---|
| 1 | `2_1_01_Silver_leads_processed` | `leads_raw` | `leads_processed` | addresses, contacts (emails, phones, urls, integration_links), opportunities (attachments, integration_links), tasks, custom (Add_ons, HADES TYPE, Lead Source, Objections Faced, Reactivation Campaign) — 17 child tables total |
| 2 | `2_2_01_Silver_lead_activites_processed` | `lead_activites_raw` | `lead_activites_processed` | 27 child tables + 2 grandchildren (attendees, attachments, coach_legs, recording_history, integrations, etc.) |
| 3 | `2_3_Silver_close_crm_users_processed` | `close_crm_users_raw` | `close_crm_users_processed` | (child tables from user arrays) |
| 4 | `2_4_Silver_custom_activites_processed` | `custom_activites_raw` | `custom_activites_processed` | (3,530 rows — activity type metadata) |
| — | *(not processed — bronze only)* | `all_payments` | *(none)* | (payment records; no downstream use) |
| 5 | `2_5_Silver_calendly_scheduled_events_processed` | `calendly_scheduled_events` | `calendly_scheduled_events_processed` | (12.2M rows — meeting events) |
| 6 | `2_6_Silver_student_sentiment_processed` | `student_sentiment` | `student_sentiment_processed` | (sentiment + Slack data) |
| 7 | `2_7_Silver_mdl_users_processed` | `mdl_users_raw` | `mdl_users_processed` | (platform users) |
| — | *(not processed — bronze only)* | `lead_merges` | *(none)* | (lead merge history; no downstream use) |

**`lead_activites_processed` key columns (76 total):** `id`, `lead_id`, `type`, `status`, `activity_at`, `date_updated`, `date_created`, `bronze_insert_date`, `bronze_updated_at`, `note`, `note_html`, `text` (SMS text, NOT email body), `direction`, `duration`, `disposition`, `outcome_id`, `phone`, `users`, `coach_legs`, `recording_history`, etc.

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

**Note:** These are Close CRM's pre-calculated values, used as a source for the Gold layer's self-calculated engagement metrics (sentiment, message recency, Slack engagement).

---

#### `lead_activites_email_body`

| Field | Value |
|---|---|
| **Notebook** | `2_2_02_Silver_lead_activites_body_text` |
| **Full Name** | `crm_ingestion.silver.lead_activites_email_body` |
| **Source** | `crm_ingestion.bronze.lead_activites_raw` (parsed via `parse_bronze_table`) |
| **Row count** | ~1.9 million (rebuilt incrementally) |
| **Purpose** | Extracts email body text from the nested `body_text_quoted` array field that `lead_activites_processed` misses |

**Key columns:** `bronze_insert_date`, `bronze_updated_at`, `activity_id` (FK to `lead_activites_processed.id`), `lead_id`, `activity_at`, `status`, `body_text`.

**Why this table exists:** `lead_activites_processed` drops `body_text_quoted` during schema inference (76 of 135 fields captured). The `body_text_quoted[0].text` field contains the actual email body, including Calendly's "A new event has been scheduled." message used to detect meetings. This table fills the gap by using `parse_bronze_table` (which DOES discover `body_text_quoted` in its 135-field schema) and extracting `body_text_quoted[0].text` as `body_text`.

---

### 5.4 `2_8_Silver_user_mapping` (Identity Analysis — job task, no tables)

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

**Status:** Runs in the nightly job as the `silver_user_mapping` task (after all silver tasks complete). It produces no persistent tables — the notebook documents why identity consolidation was explored and ultimately rejected per spec.

---

## 6. Gold Layer

The gold layer is a star schema producing the final Customer Health Profile. Fifteen notebooks build exactly one table each (detachment — each table re-runs or recovers independently): dimensions `staff_dim` (3_01) and `lead_dim` (3_04); facts `leads_activity_fact` (3_02, event grain), `sales_fact` (3_03, latest sale per lead), and `customer_health_history` (3_15, daily snapshot); seven single-metric tables (3_05–3_11); the `lead_emails` lookup (3_12); the `health_metrics` staging join (3_13); and `customer_health_profile` (3_14, scoring + MERGE SCD Type 1). The latest run covers 1,629 coaching clients: 1,016 (62.4%) receive a real health score (219 GOOD, 319 AVERAGE, 479 POOR) and 613 (37.6%) are NO DATA.

### 6.1 Notebooks (15 total — one table per notebook)

| # | Notebook | Purpose | Outputs |
|---|---|---|---|
| 1 | `3_01_Gold_staff_dim` | Staff dimension | `gold.staff_dim` |
| 2 | `3_02_Gold_leads_activity_fact` | Activity fact, event grain, staff attribution | `gold.leads_activity_fact` |
| 3 | `3_03_Gold_sales_fact` | Latest qualifying sale per lead | `gold.sales_fact` |
| 4 | `3_04_Gold_lead_dim` | Lead/student dimension | `gold.lead_dim` |
| 5 | `3_05_Gold_days_since_last_email` | Metric: email recency | `gold.days_since_last_email` |
| 6 | `3_06_Gold_days_since_last_meeting` | Metric: CRM meeting recency | `gold.days_since_last_meeting` |
| 7 | `3_07_Gold_missed_calls_14_days` | Metric: missed calls | `gold.missed_calls_14_days` |
| 8 | `3_08_Gold_days_until_next_meeting` | Metric: upcoming meeting | `gold.days_until_next_meeting` |
| 9 | `3_09_Gold_days_since_last_login` | Metric: platform login recency | `gold.days_since_last_login` |
| 10 | `3_10_Gold_days_since_last_meeting_calendly` | Metric: Calendly meeting recency | `gold.days_since_last_meeting_calendly` |
| 11 | `3_11_Gold_sentiment` | Metric: sentiment/message recency | `gold.sentiment` |
| 12 | `3_12_Gold_lead_emails` | Lead → emails array lookup | `gold.lead_emails` |
| 13 | `3_13_Gold_health_metrics` | Staging join: dims + sales_fact + all metrics | `gold.health_metrics` |
| 14 | `3_14_Gold_customer_health_profile` | Scoring + bands + MERGE SCD Type 1 | `gold.customer_health_profile` |
| 15 | `3_15_Gold_customer_health_history` | Daily snapshot, 90-day retention | `gold.customer_health_history` |

Dependencies: staff_dim → leads_activity_fact / sales_fact; all dims, facts, metrics, and lead_emails → health_metrics → customer_health_profile → customer_health_history. All depend on Silver layer.

### 6.2 Engagement Metrics (7 notebooks, 3_05–3_11)

All metrics are per-source LEAD_ID (no identity consolidation). Email lookup via `array_contains(emails, email)` on collected emails array.

| # | Metric | Source | Description |
|---|---|---|---|
| 1 | Email recency | `lead_activites_processed` + `lead_activites_email_body` | Days since last inbox email (excludes Calendly/system emails) |
| 2 | Meeting recency (CRM) | `lead_activites_email_body` | Days since last Calendly scheduling email |
| 3 | Meeting recency (Calendly) | `calendly_scheduled_events` | Days since last Calendly event |
| 4 | Missed calls | `lead_activites_processed` | Count of missed inbound calls |
| 5 | Upcoming meetings | `calendly_scheduled_events` | Days to next upcoming meeting |
| 6 | Platform login | `mdl_users` | Days since last login (matched via UNION of USERNAME, FIRSTNAME, LASTNAME — 356 clients recovered, up from 23 with single-column match) |
| 7 | Sentiment/Message recency | `leads_processed_custom` | Days since last message from client/team + sentiment |

**Note:** Direct `student_sentiment` join is not possible — Channel_ID formats are incompatible. Sentiment metrics use `leads_processed_custom` (CRM pre-calculated values).

### 6.3 Health Score Logic (3_13 join, 3_14 scoring)

`gold.health_metrics` (3_13) joins all metrics with sales context and dimensions; 3_14 scores via multi-level CTE layering:

1. **health_metrics** (3_13) — Join all metrics + sales_fact + lead_dim (names/emails) + staff_dim (CSM)
2. **flags** — Three engagement flags: `slack_engaged` (sentiment data present), `platform_engaged` (days_since_last_login ≤ 30), `meeting_engaged` (NULL if either CRM or Calendly meeting recency is NULL — OR logic requiring both sources; engaged if both present and ≤ 30 days)
3. **patterns** — Eleven ordered rules produce nine engagement categories from the three flags (base scores 90–20); incomplete flag combinations not covered by the ordered rules yield NO DATA
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
- `csm_name` — resolved from `gold.staff_dim`
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

### Silver Layer (11 tables — one per silver job task)

| # | Full Name | Source | Job task | Description |
|---|---|---|---|---|
| 1 | `crm_ingestion.silver.leads_processed` | `bronze.leads_raw` | silver_leads | Flattened lead dimension |
| 2 | `crm_ingestion.silver.leads_processed_custom` | `bronze.leads_raw` | silver_leads_custom | CRM custom fields + `custom_fields` MAP for health profile |
| 3 | `crm_ingestion.silver.leads_processed_contacts` | `bronze.leads_raw` | silver_contacts | Contacts array exploded (one row per contact) |
| 4 | `crm_ingestion.silver.leads_processed_contacts_emails` | `silver.leads_processed_contacts` | silver_contacts_emails | Contact emails exploded (one row per email, rebuilt daily) |
| 5 | `crm_ingestion.silver.lead_activites_processed` | `bronze.lead_activites_raw` | silver_activities | Deduplicated activities (76 fields) |
| 6 | `crm_ingestion.silver.lead_activites_email_body` | `bronze.lead_activites_raw` | silver_email_body | Email body text from `body_text_quoted` (1,696,168 rows) |
| 7 | `crm_ingestion.silver.close_crm_users_processed` | `bronze.close_crm_users_raw` | silver_crm_users | User dimension |
| 8 | `crm_ingestion.silver.custom_activites_processed` | `bronze.custom_activites_raw` | silver_custom_activities | Custom activity metadata |
| 9 | `crm_ingestion.silver.calendly_scheduled_events_processed` | `bronze.calendly_scheduled_events` | silver_calendly | 12.2M meeting events |
| 10 | `crm_ingestion.silver.student_sentiment_processed` | `bronze.student_sentiment` | silver_sentiment | Sentiment + Slack data |
| 11 | `crm_ingestion.silver.mdl_users_processed` | `bronze.mdl_users_raw` | silver_mdl_users | Platform users |

**Not processed to silver:** `bronze.all_payments` and `bronze.lead_merges` are ingested to Bronze only (no downstream use).

### Gold Layer (15 tables + 1 view)

| # | Full Name | Type | Description |
|---|---|---|---|
| 1 | `crm_ingestion.gold.staff_dim` | TABLE (DIM) | Staff dimension — coaches, CSMs, setters/closers, partners with real names/emails (3_01) |
| 2 | `crm_ingestion.gold.leads_activity_fact` | TABLE (FACT) | One row per activity, latest version, staff attribution (3_02) |
| 3 | `crm_ingestion.gold.sales_fact` | TABLE (FACT) | Latest qualifying sale per lead with setter/closer attribution (3_03) |
| 4 | `crm_ingestion.gold.lead_dim` | TABLE (DIM) | Lead/student dimension — names, status, CSM, contract, tier (3_04) |
| 5 | `crm_ingestion.gold.days_since_last_email` | TABLE | Metric: email recency (3_05) |
| 6 | `crm_ingestion.gold.days_since_last_meeting` | TABLE | Metric: CRM meeting recency (3_06) |
| 7 | `crm_ingestion.gold.missed_calls_14_days` | TABLE | Metric: missed inbound calls, 14-day lookback (3_07) |
| 8 | `crm_ingestion.gold.days_until_next_meeting` | TABLE | Metric: upcoming meeting (3_08) |
| 9 | `crm_ingestion.gold.days_since_last_login` | TABLE | Metric: platform login recency (3_09) |
| 10 | `crm_ingestion.gold.days_since_last_meeting_calendly` | TABLE | Metric: Calendly meeting recency (3_10) |
| 11 | `crm_ingestion.gold.sentiment` | TABLE | Metric: sentiment/message recency (3_11) |
| 12 | `crm_ingestion.gold.lead_emails` | TABLE | Lead → emails array for array_contains lookup (3_12) |
| 13 | `crm_ingestion.gold.health_metrics` | TABLE | Staging join of dims + sales_fact + all metrics (3_13) |
| 14 | `crm_ingestion.gold.customer_health_profile` | TABLE (FACT) | Customer Health Profile — MERGE SCD Type 1 target (3_14) |
| 15 | `crm_ingestion.gold.customer_health_history` | TABLE (FACT) | Daily full snapshot, 90-day rolling window (3_15) |
| + | `crm_ingestion.gold.view_customer_health_base` | VIEW | View over the health base used by scoring |

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
| **Tasks** | 29 total (2 bronze + 12 silver + 15 gold) |
| **Dependencies** | bronze_config → bronze_ingestion → silver (contacts_emails after contacts) → user_mapping → gold dims/metrics/lookups in parallel → facts after staff_dim → health_metrics → profile → history |
| **Run if** | ALL_SUCCESS (downstream tasks skip if any upstream fails) |
| **Max concurrent runs** | 1 |

**How to run manually:** Click "Run" on the job page, or `databricks jobs run-now <job-id>` (the job ID is on the job page; it is created by the bundle deploy in DEPLOY.md).

**How to repair a failed run:** `databricks jobs repair-run <run_id> --rerun-all-failed-tasks --rerun-dependent-tasks` — re-runs only failed tasks and their dependents, preserving successful results.

**Note:** The job is defined as code in `databricks.yml` (Declarative Automation Bundle) and deployed with the Databricks CLI — see `DEPLOY.md`. Both the notebooks and the orchestration are version-controlled in this repo.

---

## 9. Health Score Logic

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

### Production

* Bronze layer: 9 tables ingested from PostgreSQL (incremental watermark, config-driven JDBC, backfill widgets, error-isolated loads)
* Silver layer: 11 processing notebooks + shared `utils` notebook (type-based JSON repair, 200-row schema discovery, field-level NULL validation, selective array explosion)
* Gold layer: 15 notebooks, one table each — star schema with staff_dim + lead_dim dimensions; leads_activity_fact, sales_fact, and customer_health_history facts; 7 metric tables; lead_emails; health_metrics; and customer_health_profile (MERGE SCD Type 1 on lead_id)
* All 7 engagement/sentiment metrics implemented and verified (3_05–3_11)
* Health score logic fully implemented (three flags → eleven ordered rules → base score → adjustments → bands)
* `gold.customer_health_profile` table supports email array lookup via `array_contains`
* Databricks Job: 29 tasks, daily 8:00 AM EST, serverless compute, defined as code in `databricks.yml` (Declarative Automation Bundle — see DEPLOY.md)
* Latest run: 1,629 coaching clients — 1,016 scored (219 GOOD, 319 AVERAGE, 479 POOR), 613 NO DATA (37.6%)
* Moodle platform join recovers 356 clients via three-column UNION (up from 23 with single-column match — 15× increase, moving 333 clients from unscoreable to scored)
* Pipeline in production since mid-October 2026

### Notes

* Health is per-source LEAD_ID — no identity consolidation per spec requirements
* Sentiment metrics use `leads_processed_custom` — direct `student_sentiment` join not possible (incompatible Channel_ID formats)
* Platform login matches 356 Coaching Clients via UNION of `USERNAME`, `FIRSTNAME`, `LASTNAME` from `mdl_users` against the `user_` hash in contacts (emails are masked; single-column USERNAME match recovered only 23 clients — the three-column UNION recovered 356, a 15× increase that moved 333 clients from unscoreable into scored bands)
* `meeting_engaged_flag` is NULL when either CRM meeting recency or Calendly recency is NULL — this OR logic is spec-mandated (cross-validation from both meeting sources required)
* Upcoming meetings metric may return 0 rows if all upcoming events are in the past relative to run date
* `2_8_Silver_user_mapping` notebook (section 5.4) explores whether different LEAD_IDs could be linked to the same person via email/phone/name matching, and tests sentiment-to-lead join feasibility. It runs as the silver_user_mapping job task but produces no deliverables — project requirements prohibit Lead ID consolidation, and the notebook documents why this approach was explored and rejected