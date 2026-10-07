# DEA Student Engagement Pipeline

A Databricks Medallion Architecture (Bronze → Silver → Gold) pipeline that transforms PostgreSQL CRM and engagement data into a **Customer Health Profile** for coaching clients.

---

## 1. Architecture Overview

The pipeline uses a three-layer medallion architecture:

**Bronze** — Raw data landing zone. PostgreSQL source tables are loaded into Delta tables via JDBC with no business transformations. Only current-day records are loaded each run, providing a lightweight incremental load without CDC or MERGE.

**Silver** — Cleaned and normalized data. JSON payloads are parsed, nested structures are flattened, custom fields are resolved, and source snapshots are deduplicated. Three analytics-ready datasets are produced.

**Gold** — Business-ready reporting. Engagement metrics, health scores, and sales context are computed. The final output is a single Customer Health Profile per coaching client, lookupable by email.

### Data Flow

```
PostgreSQL (raw schema)
    |
    v  JDBC, daily append (WHERE DATE(INSERT_DATE) = CURRENT_DATE)
BRONZE (crm_ingestion.bronze)    9 raw Delta tables - append-only landing zone
    |
    v  JSON repair, flatten structs, extract custom fields, deduplicate
SILVER (crm_ingestion.silver)    3 normalized datasets
    |
    v  Engagement metrics, health scoring, sales context
GOLD (crm_ingestion.gold)        2 reporting tables -> Customer Health Profile
```

---

## 2. Folder Structure

```
Dea Student Engagement Pipeline/
+- 0_READme/
|  +- readme                            <- Project overview notebook (spec source of truth)
+- 1_Bronze/
|  +- Bronze_config                     <- Creates ingestion metadata table
|  +- Bronze_ingestion                  <- Loads 9 PostgreSQL tables into Bronze
+- 2_Silver/
|  +- leads_processed                   <- Flattened lead dimension
|  +- close_crm_users_processed          <- User dimension
|  +- leads_activities_summary          <- Deduplicated activity records
+- 3_Gold/
|  +- sales_details                     <- Sales context per coaching client
|  +- leads_account_details_updates     <- Final Customer Health Profile
+- silver_engine_utils                  <- Shared utility functions (JSON repair, schema discovery, silver writing)
+- README.md                            <- This file
```

---

## 3. Full Execution Order

The pipeline must run in this order due to table dependencies:

```
Step 1: 1_Bronze/Bronze_config
            | Creates: crm_ingestion.config.bronze_config
            v
Step 2: 1_Bronze/Bronze_ingestion
            | Creates: 9 Bronze Delta tables
            v
Step 3: 2_Silver/leads_processed              <- Reads: bronze.leads_raw
Step 4: 2_Silver/close_crm_users_processed     <- Reads: bronze.close_crm_users_raw
Step 5: 2_Silver/leads_activities_summary      <- Reads: bronze.lead_activites_raw, custom_activites_raw, close_crm_users_raw
            | Creates: 3 Silver Delta tables
            v
Step 6: 3_Gold/sales_details                  <- Reads: silver.leads_activities_summary, close_crm_users_processed
Step 7: 3_Gold/leads_account_details_updates    <- Reads: all 3 silver tables + bronze engagement tables
            | Creates: Final Customer Health Profile
```

Steps 3-5 can run in parallel (no interdependencies). Step 6 must complete before Step 7.

---

## 4. File-by-File Documentation

### 4.1 00_READme

| Field | Value |
|---|---|
| **Type** | File |
| **Purpose** | Project overview and specification document |

This is the source of truth for the project. It defines the architecture, source tables, silver datasets, gold datasets, deduplication rules, health score logic, and final deliverable. All other notebooks should be consistent with what is defined here.

---

### 4.2 1_Bronze/Bronze_config

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | Metadata defined within the notebook |
| **Writes to** | `crm_ingestion.config.bronze_config` |
| **Purpose** | Creates the metadata table that controls Bronze ingestion |

**Logic (plain English):**

1. Create the `crm_ingestion.config` schema if it does not exist
2. Create the `bronze_config` table with columns for source table, target table, active flag, and load order
3. Insert source-to-target mappings for all 9 PostgreSQL tables
4. Validate the configuration by querying the table

**Key output columns:**

| Column | Description |
|---|---|
| `source_table` | PostgreSQL source table name (e.g., `raw.leads_raw`) |
| `target_table` | Bronze Delta table name (e.g., `bronze.leads_raw`) |
| `active` | Boolean flag indicating whether the table should be loaded |
| `load_order` | Integer controlling the order of ingestion |

**Business meaning:** This table drives the Bronze ingestion process. Adding or removing a source table only requires updating this config table, not changing ingestion code.

---

### 4.3 1_Bronze/Bronze_ingestion

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | `crm_ingestion.config.bronze_config`, PostgreSQL `raw.*` schema |
| **Writes to** | `crm_ingestion.bronze.*` (9 Delta tables) |
| **Purpose** | Loads PostgreSQL source tables into Bronze Delta tables |

**Logic (plain English):**

1. Configure JDBC connection to PostgreSQL (host, port, database, credentials)
2. Read active ingestion metadata from `bronze_config` (where `active = true`)
3. Define a reusable `load_postgres_table(source_table, target_table)` function that:
   - Builds a SQL query: `SELECT * FROM {source_table} WHERE DATE(INSERT_DATE) = CURRENT_DATE`
   - Reads via Spark JDBC into a DataFrame
   - Appends to a Delta table using `mode("append")`
4. Execute the function for each active table in load order

**Load rule:** Only current-day records are loaded (`WHERE DATE(INSERT_DATE) = CURRENT_DATE`). This provides a lightweight incremental load without MERGE or CDC. Each run grabs only today's rows and appends them.

**Key output tables (9 total):**

| # | Bronze Table | Source | Purpose |
|---|---|---|---|
| 1 | `bronze.leads_raw` | `raw.leads_raw` | Lead/contact/account data |
| 2 | `bronze.lead_activites_raw` | `raw.lead_activites_raw` | CRM activities (Email, Call, Meeting, Note, CustomActivity) |
| 3 | `bronze.close_crm_users_raw` | `raw.close_crm_users_raw` | CRM user dimension |
| 4 | `bronze.custom_activites_raw` | `raw.custom_activites_raw` | Custom activity metadata (types, fields, outcomes) |
| 5 | `bronze.all_payments` | `raw.all_payments` | Payment records (loaded but not used downstream) |
| 6 | `bronze.calendly_scheduled_events` | `raw.calendly_scheduled_events` | Scheduled meeting events |
| 7 | `bronze.student_sentiment` | `raw.student_sentiment` | Sentiment and Slack message data |
| 8 | `bronze.mdl_users_raw` | `raw.mdl_users_raw` | Moodle platform users |
| 9 | `bronze.lead_merges` | `raw.lead_merges` | Lead merge history (loaded but not used downstream) |

**Business meaning:** This is the raw landing zone. Data is stored exactly as it appears in PostgreSQL with no transformations. The 9 tables provide the source material for all downstream silver and gold processing.

---

### 4.4 2_Silver/leads_processed

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | `crm_ingestion.bronze.leads_raw` |
| **Writes to** | `crm_ingestion.silver.leads_processed` |
| **Purpose** | Creates a flattened lead dimension with customer, account, contact, and status information |

**Logic (plain English):**

1. Read `bronze.leads_raw`
2. Parse and repair malformed JSON payloads (single-quoted JSON, Python literals like `None`)
3. Flatten nested structures (contacts array, account details, custom fields)
4. Extract custom fields into a usable format
5. Deduplicate by `LEAD_ID` — keep the latest record using `DATE_UPDATED` then `INSERT_DATE` as tiebreaker
6. Write to `silver.leads_processed`

**Key output columns:**

| Column | Description |
|---|---|
| `LEAD_ID` | Unique lead identifier |
| `NAME` | Lead name (masked in source) |
| `STATUS_LABEL` | Lead status (e.g., "Coaching Client") |
| `EMAIL` | Lead email (masked in source) |
| `ACCOUNT_NAME` | Account/display name |
| `CUSTOMER_NAME` | Customer display name |
| `CSM` | Customer Success Manager |
| `lead_created_date` | Lead creation timestamp |
| `lead_updated_date` | Lead last update timestamp |
| Custom fields | Program, contract value, date of sale, closer, setter, etc. |

**Business meaning:** This is the lead dimension table. It carries the account information needed for the final health profile. The `STATUS_LABEL` field is used to filter to Coaching Clients in the gold layer.

---

### 4.5 2_Silver/close_crm_users_processed

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | `crm_ingestion.bronze.close_crm_users_raw` |
| **Writes to** | `crm_ingestion.silver.close_crm_users_processed` |
| **Purpose** | Creates the user dimension used for ownership and attribution |

**Logic (plain English):**

1. Read `bronze.close_crm_users_raw`
2. Parse and repair JSON payloads
3. Flatten nested structures
4. Deduplicate by `USER_ID` — keep the latest record using `DATE_UPDATED`
5. Write to `silver.close_crm_users_processed`

**Key output columns:**

| Column | Description |
|---|---|
| `USER_ID` | Unique user identifier |
| `FIRST_NAME` | User first name |
| `LAST_NAME` | User last name |
| `EMAIL` | User email |
| `ROLE` | User role (e.g., Setter, Closer, CSM) |
| `STATUS` | User status (active/inactive) |
| `date_updated` | Last update timestamp |

**Business meaning:** This dimension resolves who owns each lead and activity. It is used to attribute sales to setters and closers and to identify Customer Success Managers in the gold layer.

---

### 4.6 2_Silver/leads_activities_summary

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | `crm_ingestion.bronze.lead_activites_raw`, `bronze.custom_activites_raw`, `bronze.close_crm_users_raw` |
| **Writes to** | `crm_ingestion.silver.leads_activities_summary` |
| **Purpose** | Creates one current record per `ACTIVITY_ID` with flattened activity data and resolved activity outcomes |

**Logic (plain English):**

1. Read `bronze.lead_activites_raw`
2. Parse and repair JSON payloads (using the `parse_activity_final` SQL UDF or Python equivalent)
3. Flatten nested structures (attendees, contacts, body text, envelope data)
4. Join with `custom_activites_raw` to resolve activity type metadata and custom field definitions
5. Join with `close_crm_users_raw` to resolve user names (replacing masked user IDs with real names)
6. Deduplicate by `ACTIVITY_ID` — keep the latest record using `DATE_UPDATED` then `INSERT_DATE` as tiebreaker
7. Write to `silver.leads_activities_summary`

**Key output columns:**

| Column | Description |
|---|---|
| `ACTIVITY_ID` | Unique activity identifier |
| `LEAD_ID` | Associated lead |
| `ACTIVITY_AT` | Activity timestamp |
| `TYPE` | Activity type (Email, Call, Meeting, Note, CustomActivity) |
| `OUTCOME` | Activity outcome (New Sale, No-answer, etc.) |
| `OWNER` | Activity owner name (resolved from user dimension) |
| `USER` | Activity user name (resolved from user dimension) |
| `date_updated` | Last update timestamp (used for deduplication) |
| `insert_date` | Source insert timestamp (tiebreaker for deduplication) |

**Deduplication rule:** Keep the latest version of each `ACTIVITY_ID` using `DATE_UPDATED` then `INSERT_DATE` (latest wins).

**Business meaning:** This is the activity fact table. It feeds engagement metrics (email recency, meeting recency, missed calls) and sales qualification (outcome = "New Sale"). Resolving user names here avoids joins in the gold layer.

---

### 4.7 3_Gold/sales_details

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | `crm_ingestion.silver.leads_activities_summary`, `crm_ingestion.silver.close_crm_users_processed` |
| **Writes to** | `crm_ingestion.gold.sales_details` |
| **Purpose** | Creates sales context for each coaching client |

**Logic (plain English):**

1. Read `silver.leads_activities_summary`
2. Filter to qualifying sales outcomes only:
   - "New Sale"
   - "New Sale [Custom Payment Plan]"
3. Join with `close_crm_users_processed` to resolve setter and closer names from user IDs
4. Extract sale date, program, and contract value from custom fields
5. Keep the latest sale per lead (if multiple sales exist)
6. Write to `gold.sales_details`

**Key output columns:**

| Column | Description |
|---|---|
| `LEAD_ID` | Associated lead |
| `SALE_DATE` | Date of sale |
| `PROGRAM` | Program type |
| `CONTRACT_VALUE` | Contract value |
| `SETTER` | Sales setter name |
| `CLOSER` | Sales closer name |

**Business rule:** Only outcomes "New Sale" and "New Sale [Custom Payment Plan]" qualify as sales.

**Business meaning:** This table provides the sales context for each coaching client: when they bought, what program, how much, and who sold it. This is joined into the final health profile.

---

### 4.8 3_Gold/leads_account_details_updates

| Field | Value |
|---|---|
| **Type** | Notebook (Python) |
| **Reads from** | `silver.leads_processed`, `silver.close_crm_users_processed`, `silver.leads_activities_summary`, `gold.sales_details`, `bronze.calendly_scheduled_events`, `bronze.mdl_users_raw`, `bronze.student_sentiment` |
| **Writes to** | `crm_ingestion.gold.leads_account_details_updates` |
| **Purpose** | Creates the final Customer Health Profile with engagement metrics, health score, and health band |

**Logic (plain English):**

1. Start with `silver.leads_processed`, filter to Coaching Clients only
2. Join with `gold.sales_details` for sales context (sale date, program, contract value, setter, closer)
3. Join with `silver.close_crm_users_processed` to resolve CSM name
4. Compute engagement metrics from multiple sources:
   - **CRM activities** (`silver.leads_activities_summary`) -> days since last email, days since last meeting, missed calls in 14-day window
   - **Calendly** (`bronze.calendly_scheduled_events`) -> days since last Calendly meeting, days until next scheduled meeting
   - **Platform** (`bronze.mdl_users_raw`) -> days since last platform login
   - **Sentiment** (`bronze.student_sentiment`) -> sentiment label, Slack engagement flag
5. Compute engagement flags:
   - `SLACK_ENGAGED_FLAG` — sentiment activity within threshold
   - `PLATFORM_ENGAGED_FLAG` — platform login within threshold
   - `MEETING_ENGAGED_FLAG` — meeting activity within threshold
6. Determine engagement pattern (8 possible combinations of the 3 flags, plus NO DATA)
7. Calculate final health score: `Base Engagement Score + Missed Call Adjustment + Sentiment Adjustment`
8. Assign health band: GOOD (>=75), AVERAGE (>=50 and <75), POOR (<50), or NO DATA
9. Write the final profile to `gold.leads_account_details_updates`

**Key output columns:**

**Account Information:**

| Column | Description |
|---|---|
| `LEAD_ID` | Unique lead identifier |
| `CUSTOMER_NAME` | Customer display name |
| `ACCOUNT_NAME` | Account name |
| `STATUS` | Lead status (Coaching Client) |
| `CSM` | Customer Success Manager |
| `EMAIL` | Email for lookup |

**Sales Information:**

| Column | Description |
|---|---|
| `SALE_DATE` | Date of sale |
| `PROGRAM` | Program type |
| `CONTRACT_VALUE` | Contract value |
| `SETTER` | Sales setter |
| `CLOSER` | Sales closer |

**Engagement Metrics:**

| Column | Description |
|---|---|
| `DAYS_SINCE_LAST_EMAIL` | Days since last CRM email activity |
| `DAYS_SINCE_LAST_MEETING` | Days since last CRM meeting activity |
| `DAYS_SINCE_LAST_MEETING_CALENDLY` | Days since last Calendly event |
| `UPCOMING_MEETING_DAYS` | Days until next scheduled meeting |
| `DAYS_SINCE_LAST_LOGIN` | Days since last platform login |
| `MISSED_CALLS_14_DAYS` | Count of missed inbound calls in 14-day window |
| `SENTIMENT` | Sentiment label from Slack channels |

**Health Metrics:**

| Column | Description |
|---|---|
| `SLACK_ENGAGED_FLAG` | Boolean: sentiment activity within threshold |
| `PLATFORM_ENGAGED_FLAG` | Boolean: platform login within threshold |
| `MEETING_ENGAGED_FLAG` | Boolean: meeting activity within threshold |
| `ENGAGEMENT_PATTERN` | One of 8 patterns (or NO DATA) |
| `FINAL_HEALTH_SCORE` | Numeric score (20-90, adjusted for missed calls and sentiment) |
| `HEALTH_BAND` | GOOD, AVERAGE, POOR, or NO DATA |

**Business meaning:** This is the primary business-facing dataset. It provides a complete health profile for each coaching client, supporting email lookup and proactive outreach decisions. The health score and band enable prioritization of at-risk clients.

---

### 4.9 silver_engine_utils

| Field | Value |
|---|---|
| **Type** | Notebook (Python utility library) |
| **Reads from** | Nothing (utility functions only) |
| **Writes to** | Nothing (utility functions only) |
| **Purpose** | Shared utility functions for JSON repair, schema discovery, and silver table writing |

**Usage:** Called via `%run` by silver notebooks. Provides:

- **JSON Repair UDF** — handles single-quoted JSON, Python literals (`None` to `null`), and stray quotes in free-text fields
- **Schema Discovery** — auto-detects JSON structure using `schema_of_json()` on sampled rows
- **`write_silver()`** — writes a DataFrame to a silver Delta table with `overwriteSchema`
- **`process_table()`** — orchestrator function that runs the full Bronze to Silver flow for a single table
- **`add_record_hash()`** — computes MD5 hash of record for deduplication

**How it fits:** Silver notebooks call `%run` on this notebook to load these functions, then call `process_table()` or individual functions as needed.

---

## 5. Full Table Reference

### Config Layer

| Table | Full Name | Description |
|---|---|---|
| `bronze_config` | `crm_ingestion.config.bronze_config` | Metadata table controlling Bronze ingestion (source-to-target mappings, active flags, load order) |

### Bronze Layer (9 tables)

| # | Table | Full Name | Source | Used Downstream | Description |
|---|---|---|---|:---:|---|
| 1 | `leads_raw` | `crm_ingestion.bronze.leads_raw` | `raw.leads_raw` | Yes | Lead/contact/account data |
| 2 | `lead_activites_raw` | `crm_ingestion.bronze.lead_activites_raw` | `raw.lead_activites_raw` | Yes | CRM activities (Email, Call, Meeting, Note, CustomActivity) |
| 3 | `close_crm_users_raw` | `crm_ingestion.bronze.close_crm_users_raw` | `raw.close_crm_users_raw` | Yes | CRM user dimension |
| 4 | `custom_activites_raw` | `crm_ingestion.bronze.custom_activites_raw` | `raw.custom_activites_raw` | Yes | Custom activity metadata (types, fields, outcomes) |
| 5 | `all_payments` | `crm_ingestion.bronze.all_payments` | `raw.all_payments` | No | Payment records (loaded but excluded from Gold per spec) |
| 6 | `calendly_scheduled_events` | `crm_ingestion.bronze.calendly_scheduled_events` | `raw.calendly_scheduled_events` | Yes | Scheduled meeting events (used in Gold for engagement metrics) |
| 7 | `student_sentiment` | `crm_ingestion.bronze.student_sentiment` | `raw.student_sentiment` | Yes | Sentiment and Slack message data (used in Gold for engagement metrics) |
| 8 | `mdl_users_raw` | `crm_ingestion.bronze.mdl_users_raw` | `raw.mdl_users_raw` | Yes | Moodle platform users (used in Gold for engagement metrics) |
| 9 | `lead_merges` | `crm_ingestion.bronze.lead_merges` | `raw.lead_merges` | No | Lead merge history (loaded but excluded from Gold per spec) |

### Silver Layer (3 tables)

| # | Table | Full Name | Source(s) | Description |
|---|---|---|---|---|
| 1 | `leads_processed` | `crm_ingestion.silver.leads_processed` | `bronze.leads_raw` | Flattened lead dimension (one row per LEAD_ID) |
| 2 | `close_crm_users_processed` | `crm_ingestion.silver.close_crm_users_processed` | `bronze.close_crm_users_raw` | User dimension (one row per USER_ID) |
| 3 | `leads_activities_summary` | `crm_ingestion.silver.leads_activities_summary` | `bronze.lead_activites_raw`, `custom_activites_raw`, `close_crm_users_raw` | Deduplicated activities (one current row per ACTIVITY_ID) |

### Gold Layer (2 tables)

| # | Table | Full Name | Source(s) | Description |
|---|---|---|---|---|
| 1 | `sales_details` | `crm_ingestion.gold.sales_details` | `silver.leads_activities_summary`, `silver.close_crm_users_processed` | Sales context per coaching client (latest qualifying sale) |
| 2 | `leads_account_details_updates` | `crm_ingestion.gold.leads_account_details_updates` | All 3 silver tables + `gold.sales_details` + `bronze.calendly_scheduled_events` + `bronze.mdl_users_raw` + `bronze.student_sentiment` | Final Customer Health Profile (one row per Coaching Client) |

---

## 6. Deduplication Rules

| Dataset | Primary Key | Ordering (latest wins) |
|---|---|---|
| CRM Activities | `ACTIVITY_ID` | `DATE_UPDATED` -> `INSERT_DATE` |
| Calendly Events | `EVENT_URI` | `INVITEE_UPDATED_AT` -> `EVENT_CREATED_AT` -> `INSERT_DATE` |
| Platform Users | `USER_ID` | `TIMEMODIFIED` |
| Student Sentiment | `CHANNEL_ID` | `INSERT_DATE` |
| Leads | `LEAD_ID` | `lead_updated_date` -> `source_insert_date` |
| CRM Users | `USER_ID` | `date_updated` |

---

## 7. Health Score Logic

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

## 8. Final Deliverable

The final output of this pipeline is a single **Customer Health Profile** (`crm_ingestion.gold.leads_account_details_updates`) that supports lookup by Coaching Client email and provides:

* **Customer information** — name, account, status, CSM
* **Sales context** — sale date, program, contract value, setter, closer
* **Communication engagement** — email recency, meeting recency, missed calls
* **Meeting engagement** — Calendly recency, upcoming meetings
* **Platform engagement** — login recency
* **Sentiment metrics** — Slack sentiment label
* **Health scoring** — engagement pattern, final score, health band

This is the primary business-facing dataset produced by the DEA Student Engagement Pipeline.