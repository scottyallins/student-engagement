# DEA Student Engagement Pipeline

A Databricks Medallion Architecture pipeline (Bronze → Silver → Gold) that turns five source systems — Close CRM, Calendly, the Moodle learning platform, Slack-derived sentiment, and lead-merge history, all exported from PostgreSQL — into two decision products for a coaching business:

1. **`gold.customer_health_profile`** — the Customer Health Profile: one row per client per engagement *version*, scoring each client's engagement and tracking exactly when it changed (SCD Type 2).
2. **`gold.coaching_score_by_client_health`** — the company's own report card: one row per client measuring whether the company kept its service promises (reply speed, delivery, initiation).

Everything the pipeline does lives in this repository: the Bronze ingestion, Silver processing, and Gold build notebooks, plus the nightly job itself, defined as code in `databricks.yml` (a Declarative Automation Bundle) and deployed with the Databricks CLI.

The job runs **36 tasks, daily at 8:00 AM Eastern, on serverless compute**, completing in roughly 20–25 minutes for about $1.30/day at list pricing.

---

## 1. How to Run the Pipeline

The pipeline is a single Databricks Job — **Student Engagement Pipeline** — with an explicit task DAG (see §8). It runs itself every morning; nothing needs to be started by hand.

**Run it now (manual trigger):**
* In the Jobs UI, open *Student Engagement Pipeline* and click **Run now**, or
* From the CLI: `databricks jobs run-now <job-id>`

**Repair a failed run** (re-runs only the failed tasks and their dependents, preserving all successful results):

```
databricks jobs repair-run <run_id> --rerun-all-failed-tasks --rerun-dependent-tasks
```

**Ship a change:** edit notebooks in this Git folder → commit + push → locally `git pull` → `databricks bundle validate --target prod` → `databricks bundle deploy --target prod`. The `DEPLOY_prod` notebook holds the same commands formatted for the Databricks web terminal. The nightly job runs the **deployed copies** under `.bundle/student_engagement_pipeline/prod/files/`, not the Git folder — a change does not take effect until it is deployed. Full setup and migration instructions are in `DEPLOY.md`.

**Re-run safety:** every task is idempotent.
* Bronze loads are incremental — the watermark is `MAX(insert_date)` already in bronze, so a re-run re-pulls exactly the missed rows and never duplicates.
* Silver tasks are CDC on `bronze_updated_at` — a re-run reprocesses only rows that changed.
* Gold tables are `CREATE OR REPLACE`, or idempotent MERGE / DELETE+INSERT patterns (the SCD2 profile and the daily history both delete-then-insert today's rows, so re-running a day is safe).

**Secrets:** the PostgreSQL connection credentials live in the Databricks secret scope `dea-rds` (keys: `host`, `port`, `database`, `username`, `password`). No connection values are stored in code or git.

---

## 2. Architecture Overview

**Bronze — land everything, exactly as exported.** Nine PostgreSQL tables are loaded over JDBC into Delta tables with three columns each: `raw_data` (the source row as a JSON string), `insert_date`, and `bronze_updated_at`. Loads are config-driven from `config.bronze_config`, incremental on `insert_date`, error-isolated per table (one failed source logs its error and the remaining tables still land; the task then fails so the alert fires), and batched with parallel JDBC partitions so large pulls are OOM-safe and paced against the source database.

**Silver — parse, normalize, and identify.** A shared engine (the `utils` notebook) reads each bronze table, repairs malformed JSON, deduplicates snapshot re-dumps, infers the schema, parses the JSON, and writes the silver parent tables — all under a CDC watermark so only changed rows are reprocessed. Array fields become child tables through dedicated child notebooks. The layer finishes with the **identity spine** (`silver.user_identity_map`): one row per *person* across all systems, carrying every identity hash, email, and lead attribution. The spine is what makes cross-system metrics (Calendly bookings, platform logins) possible — the CRM export masks emails and names as `user_…` hashes, so no system can be joined to another directly.

**Gold — the business star schema.** Nineteen notebooks build twenty tables (every table can be re-run or recovered independently — detachment; `3_01` builds two: the staff dimension and its account view): two staff tables, the lead dimension, two facts, seven single-metric tables, the client email view, a staff-email blocklist, a staging join, the SCD2 health profile, its daily history, two communication tables, and the coaching score. `lead_id` is the shared foreign key across the layer; facts keep their natural event grain.

**Data volume (current counts):** Bronze holds ≈19.8M rows across 9 tables — the source re-dumps full tables on every export, and bronze faithfully keeps every dump. Silver distills this to ≈12.9M rows across 15 tables (current-state children plus CDC version history on parents). Gold lands at ≈230K rows across 20 tables, with the health layer at exactly **1,374 active coaching clients** per table.

### Data Flow

```
PostgreSQL (raw schema — 9 source tables)
    |
    v  JDBC, config-driven (config.bronze_config), incremental on insert_date,
       error-isolated per table, batched + parallel partitions
BRONZE  crm_ingestion.bronze          9 raw Delta tables (raw_data JSON, insert_date, bronze_updated_at)
    |
    v  %run ../utils → process_table(): JSON repair → dedup re-dumps → schema inference
       → parse → write parent (CDC on bronze_updated_at); child notebooks explode arrays
SILVER  crm_ingestion.silver          15 tables: 7 parents + 7 children/grandchild + identity spine
    |   silver.user_identity_map (2_8) — one row per person: hashes, emails, lead_ids
    v
GOLD    crm_ingestion.gold            20 tables, 19 notebooks (3_01 builds two):
       star schema     staff_dim, staff_account_dim, lead_dim, leads_activity_fact, sales_fact
       identity views  staff_emails (blocklist), lead_emails (client emails)
       metrics         7 single-metric tables (3_05–3_11)
       health chain    health_metrics → customer_health_profile (SCD2) → customer_health_history
       communication   communication_state, communication_recency
       scorecard       coaching_score_by_client_health
    |
    v
Databricks Job — 36 tasks, daily 8:00 AM ET, serverless, defined as code in databricks.yml
```

---

## 3. Repository Layout

```
student-engagement/
+- 00_README_.md                                  <- this file
+- databricks.yml                                 <- the nightly job as code (Declarative Automation Bundle)
+- DEPLOY.md                                      <- how to deploy the bundle
+- DEPLOY_prod                                    <- deploy commands formatted for the web terminal
+- utils                                          <- shared Silver engine (JSON repair, parsing, CDC writes)
+- 1_Bronze/
|  +- 1_1_Bronze_config                           <- creates crm_ingestion.config.bronze_config (source list)
|  +- 1_2_Bronze_ingestion                        <- config-driven JDBC loader: 9 error-isolated incremental loads
+- 2_Silver/                                      <- 15 notebooks (all call %run ../utils)
|  +- 2_1_01_Silver_leads_processed                       -> leads_processed (leads parent)
|  +- 2_1_02_Silver_leads_processed_custom               -> leads_processed_custom (custom fields + identity keys)
|  +- 2_1_03_01_Silver_leads_processed_contacts         -> leads_processed_contacts (contacts array, from parent)
|  +- 2_1_03_02_Silver_leads_processed_contacts_emails  -> leads_processed_contacts_emails (email grandchild)
|  +- 2_2_01_Silver_lead_activites_processed            -> lead_activites_processed (activities parent)
|  +- 2_2_02_Silver_lead_activites_email_body           -> lead_activites_email_body (email body text)
|  +- 2_2_03_Silver_lead_activites_attendees            -> lead_activites_processed_attendees (current state)
|  +- 2_2_04_Silver_lead_activites_processed_envelope   -> lead_activites_processed_envelope (current state)
|  +- 2_2_05_Silver_lead_activites_processed_to         -> lead_activites_processed_to (current state)
|  +- 2_3_Silver_close_crm_users_processed              -> close_crm_users_processed (CRM users)
|  +- 2_4_Silver_custom_activites_processed            -> custom_activites_processed (activity type metadata)
|  +- 2_5_Silver_calendly_scheduled_events_processed    -> calendly_scheduled_events_processed
|  +- 2_6_Silver_student_sentiment_processed            -> student_sentiment_processed
|  +- 2_7_Silver_mdl_users_processed                   -> mdl_users_processed (platform users)
|  +- 2_8_Silver_user_mapping                          -> user_identity_map (the identity spine)
+- 3_Gold/                                        <- 19 notebooks, one table per notebook (3_01 builds two)
   +- 3_00_Gold_staff_emails                        -> staff_emails (staff blocklist)
   +- 3_01_Gold_staff_dim                           -> staff_dim + staff_account_dim
   +- 3_02_Gold_leads_activity_fact                 -> leads_activity_fact
   +- 3_03_Gold_sales_fact                          -> sales_fact
   +- 3_04_Gold_lead_dim                           -> lead_dim (Coaching Client population)
   +- 3_05_Gold_days_since_last_email               -> days_since_last_email
   +- 3_06_Gold_days_since_last_meeting             -> days_since_last_meeting
   +- 3_07_Gold_missed_calls_14_days                -> missed_calls_14_days
   +- 3_08_Gold_days_until_next_meeting             -> days_until_next_meeting
   +- 3_09_Gold_days_since_last_login               -> days_since_last_login
   +- 3_10_Gold_days_since_last_meeting_calendly    -> days_since_last_meeting_calendly
   +- 3_11_Gold_sentiment                          -> sentiment
   +- 3_12_Gold_lead_emails                        -> lead_emails (client email view)
   +- 3_13_Gold_health_metrics                     -> health_metrics (staging join)
   +- 3_14_Gold_customer_health_profile             -> customer_health_profile (SCD2)
   +- 3_15_Gold_customer_health_history             -> customer_health_history (daily snapshot)
   +- 3_16_Gold_communication_state                 -> communication_state (all leads)
   +- 3_17_Gold_communication_recency               -> communication_recency (clients)
   +- 3_18_Gold_coaching_score_by_client_health    -> coaching_score_by_client_health
```

---

## 4. Bronze Layer (9 tables)

| # | Bronze Table | Source | Used Downstream | Rows | Description |
|---|---|---|:---:|---:|---|
| 1 | `crm_ingestion.bronze.leads_raw` | `raw.leads_raw` | Yes | 531,475 | Lead/contact/account data |
| 2 | `crm_ingestion.bronze.lead_activites_raw` | `raw.lead_activites_raw` | Yes | 443,211 | CRM activities (Email, Call, Meeting, Note, SMS, CustomActivity) |
| 3 | `crm_ingestion.bronze.close_crm_users_raw` | `raw.close_crm_users_raw` | Yes | 90,706 | CRM user directory (staff) |
| 4 | `crm_ingestion.bronze.custom_activites_raw` | `raw.custom_activites_raw` | Yes | 159 | Custom activity type metadata |
| 5 | `crm_ingestion.bronze.all_payments` | `raw.all_payments` | No | 268,940 | Payment records (bronze only — no silver consumer) |
| 6 | `crm_ingestion.bronze.calendly_scheduled_events` | `raw.calendly_scheduled_events` | Yes | 15,451,143 | Scheduled meeting events (full re-dump each export) |
| 7 | `crm_ingestion.bronze.student_sentiment` | `raw.student_sentiment` | Yes | 227,427 | Sentiment + Slack message data |
| 8 | `crm_ingestion.bronze.mdl_users_raw` | `raw.mdl_users_raw` | Yes | 2,465,573 | Moodle platform users |
| 9 | `crm_ingestion.bronze.lead_merges` | `raw.lead_merges` | No | 347,320 | Lead merge history (bronze only — no silver consumer) |

Every bronze table has the same three columns: `raw_data` (JSON string), `insert_date`, `bronze_updated_at`.

**Ingestion mechanics (`1_2_Bronze_ingestion`):**
* **Config-driven** — the loader iterates over active rows in `config.bronze_config` (`source_table`, `target_table`, `incremental_column`, `load_type`, `active`). Adding a source is a config row.
* **Incremental watermark** — each load reads `insert_date > MAX(insert_date)` already in the bronze target. A failed or interrupted run leaves the watermark at the last successful row, so the next run re-pulls exactly the missed rows — no duplicates, no manual recovery.
* **Error isolation** — the nine sources have no interdependency, so one failed JDBC load never stops the others: load everything possible, print a per-table summary, then raise so the job alert fires.
* **OOM-safe batching** — JDBC fetch size 10,000 (the driver default buffers the entire result set in memory); large pulls are split into batches sized from `pg_stats` row-width estimates, batches over 64 MB are fetched in parallel across 8 JDBC connections, with a pause between batches to protect the source database.

---

## 5. Silver Layer (15 tables)

### 5.1 The Shared Engine (`utils`)

All silver notebooks start with `%run ../utils`, then call `process_table('bronze_table', 'silver_table')` (or use the engine's building blocks directly for child tables).

| Function | Purpose |
|---|---|
| `repair_json_udf` | Detects and repairs malformed JSON: type-based wrapper unwrapping (stringified inner JSON, single-key data-array wrappers), quote repair, recursion. An escalation ladder — direct parse first, surgery only when needed. Free-text keys that break parsing (`description`, `notes`, `body_text`, `subject`, `text`, `note`) are nulled before repair. |
| `parse_bronze_table()` | Reads a bronze table, deduplicates snapshot re-dumps, detects the wrapper format, repairs dirty rows once into a temp table, infers the schema from up to 200 sampled rows (`schema_of_json_agg` with single-row fallback), parses with `from_json`, and explodes the top-level array. Aborts the write if every field parses to NULL (wrong-schema guard). |
| `get_silver_watermark()` | Returns `MAX(bronze_updated_at)` from a silver table — the CDC watermark, or None on first run. |
| `write_silver()` | Writes a DataFrame to silver. In CDC mode (watermark set) it deletes rows newer than the watermark and appends — the delete window matches the read filter to the microsecond. First run is a full overwrite. |
| `flatten_structs()` | Flattens StructType columns into individual columns (the `custom` struct is kept for separate extraction). |
| `explode_to_child()` | Explodes an array column into a child table; `dry_run=True` prints the lineage without writing. |
| `extract_custom()` | Extracts the `custom` struct and `custom_cf_` fields into a child table and a `custom_fields` MAP on the parent. |
| `process_table()` | Orchestrator: parse → optional flatten → custom extraction → write parent → print the discovered lineage tree (children are printed in dry-run; child tables are built by the dedicated child notebooks). |

**Version history, not destructive updates:** bronze receives a full re-dump from the source on every export, so silver parents accumulate one row per source version (`bronze_updated_at` distinguishes them). Silver is append-CDC: only rows newer than the watermark are reprocessed. **Every gold consumer takes the latest version per key** — `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <timestamp> DESC)` keeps `rn = 1`.

**Schema discovery note:** schema inference samples up to 200 random rows, so fields that appear only in rare activity types can be missed by the parent parse. `lead_activites_processed` captures 76 of the 135 source fields this way; the email body fields are among the missing 59 — which is why `lead_activites_email_body` re-parses bronze with its own targeted extraction.

### 5.2 Parent Tables (7)

| Notebook | Bronze Source | Silver Table | Rows | Contents |
|---|---|---|---:|---|
| `2_1_01` | `leads_raw` | `leads_processed` | 343,820 | Flattened leads: `id`, `name` (identity hash), `display_name`, `status_label`, `date_updated`, + the raw arrays (`contacts`, `opportunities`, `tasks`, `custom` struct) |
| `2_2_01` | `lead_activites_raw` | `lead_activites_processed` | 7,558,812 | Activities: `id`, `lead_id`, `type`, `activity_at`, `direction`, `status`, `disposition`, `note`, `text` (SMS body — email body is NOT here), `attendees` array, `custom_fields` MAP, … 76 fields |
| `2_3` | `close_crm_users_raw` | `close_crm_users_processed` | 164,756 | CRM users/staff (real names, emails) |
| `2_4` | `custom_activites_raw` | `custom_activites_processed` | 1,757 | Custom activity types, fields, outcomes |
| `2_5` | `calendly_scheduled_events` | `calendly_scheduled_events_processed` | 104,716 | Meeting events: `EVENT_URI`, `INVITEE_NAME`/`INVITEE_EMAIL` (real emails), `EVENT_START_TIME`, `EVENT_CREATED_AT`, `INVITEE_UPDATED_AT` |
| `2_6` | `student_sentiment` | `student_sentiment_processed` | 140,771 | Sentiment + Slack data (`CHANNEL_ID`, `CHANNEL_NAME`) |
| `2_7` | `mdl_users_raw` | `mdl_users_processed` | 29,455 | Platform users: `USERNAME`/`FIRSTNAME`/`LASTNAME` are identity hashes, `LASTACCESS` |

### 5.3 Child Tables (7)

Two build patterns, both CDC:

* **Lead children** (`2_1_02`, `2_1_03_01`, `2_1_03_02`) — explode arrays from the silver *parent* (`leads_processed`), reading only parent rows newer than the child's own watermark. They inherit the parent's version history; consumers deduplicate.
* **Activity children** (`2_2_03`, `2_2_04`, `2_2_05`) — read the activity parent's changed rows, keep the **latest version per activity**, and replace exactly the affected activity's rows (an exact-key DELETE, because a timestamp-only delete would leave stale siblings). These tables represent **current state**. First run, or a legacy schema without `bronze_updated_at`, triggers a full rebuild.
* `2_2_02` re-parses bronze directly (the parent parse misses the body fields — see §5.1).

| Notebook | Silver Table | Rows | One row per… |
|---|---|---:|---|
| `2_1_02` | `leads_processed_custom` | 343,820 | Lead × custom fields: `CSM`, `Contracted_Value`, `Tier`, `Avatar`, `SENTIMENTS_LAST_30_DAYS`, `DAYS_SINCE_LAST_*` (CRM pre-calculated), `Channel_ID` |
| `2_1_03_01` | `leads_processed_contacts` | 420,471 | Lead × contact: `id` (cont_id), `name` (identity hash), emails/phones/urls arrays |
| `2_1_03_02` | `leads_processed_contacts_emails` | 508,148 | Lead × contact × email: `email`, `type`, `is_unsubscribed` (mostly masked `user_…@masked.com`) |
| `2_2_02` | `lead_activites_email_body` | 2,593,027 | Email activity × body text: `body_text` extracted from the nested `body_text_quoted` array |
| `2_2_03` | `lead_activites_processed_attendees` | 61,201 | Activity × attendee: `contact_id`, real `email` (the only real client email in the CRM export), `is_organizer`, `user_id` (48-char staff login hash), `name`, `status` — current state |
| `2_2_04` | `lead_activites_processed_envelope` | 427,155 | Email activity envelope (from/to masked addresses) — current state |
| `2_2_05` | `lead_activites_processed_to` | 124,446 | Email activity recipients — current state |

**Why `lead_activites_email_body` exists:** `body_text_quoted[0].text` carries the actual email body — including Calendly's 'A new event has been scheduled.' message, which `3_06` uses as meeting evidence. The `text` column on `lead_activites_processed` is SMS text only; emails have NULL there.

### 5.4 The Identity Spine (`2_8` → `silver.user_identity_map`)

One row per **person** across all systems. This is the piece that makes cross-system gold metrics possible: the CRM masks emails/names as `user_<32-hex>` hashes, each source system hashes differently, and real emails appear only in activities' attendee records, Calendly invites, and CRM contact records. The spine links all of them.

**Build (6 steps in the notebook):**
1. **Collect** every `user_32digit` hash + email from all silver tables: contacts names, attendees (real personal emails via the `contact_id` bridge), organizers (48-char login hashes → staff email), Calendly `INVITEE_EMAIL`/`EVENT_HOST_EMAIL`, `mdl_users` emails — deduplicated before joins to prevent row multiplication.
2. **Group by email** — one row per email with every linked hash, lead, and contact attribution.
3. **Pivot** back to source-column shapes (each source table joins on its own column name).
4. **Create `master_user_id`** — bundle hashes that share the same **real** email into one person; masked emails stay for lookup but cannot group. Two guards prevent false bundles: a hash carrying more than 10 real emails (shared/system hash) and an email carrying more than 5 distinct hashes (community/shared inbox) are excluded from grouping but remain searchable.
5. **Materialize** `silver.user_identity_map`: `master_user_id`, `user_32digits`, `real_emails`, `masked_emails`, `lead_ids`, `cont_ids`, `source_tables` — one row per person (≈131,000 currently; ≈42K with a real email, ≈46K linked to leads).
6. **Validate** spine coverage against the gold meeting signals (future bookings, any bookings).

**Canonical email contract:** every email is normalized exactly once, in Step 1's union wrapper — `NULLIF(LOWER(TRIM(email)), '')`. All spine outputs are canonical by construction; gold tables join on emails without re-normalizing.

`master_user_id` is a person key across all systems — NOT a lead_id. Most people have no CRM lead. Joins downstream remain on `lead_id`; the spine exposes each person's `lead_ids` array, and gold consumers `explode()` it. Project spec: lead IDs are never merged or consolidated — the spine is a lookup, not a rewrite.

---

## 6. Gold Layer (20 tables)

Star schema keyed on `lead_id`. Every consumer-facing table is one row per client; the facts keep their natural event grain.

| # | Notebook | Table | Grain / Key | Purpose |
|---|---|---|---|---|
| 0 | `3_00` | `staff_emails` | staff person (`master_user_id`) | Staff email blocklist (see §6.2) |
| 1 | `3_01` | `staff_dim` | staff (`csm_id`) | Coaches, CSMs, setters/closers with real names/emails |
| 1b | `3_01` | `staff_account_dim` | staff person (`master_user_id`) | Full staff account view: identity + CRM profile + role attribution (§6.2) |
| 2 | `3_02` | `leads_activity_fact` | activity (`activity_id`) | One row per activity, latest version, staff-attributed |
| 3 | `3_03` | `sales_fact` | lead (latest sale) | Latest qualifying sale per lead with setter/closer |
| 4 | `3_04` | `lead_dim` | client (`lead_id`) | The Coaching Client population + commercial attributes |
| 5–11 | `3_05`–`3_11` | 7 metric tables | client (`lead_id`) | Single engagement metrics (§6.3) |
| 12 | `3_12` | `lead_emails` | lead (`lead_id`) | Client email view + primary email (§6.2) |
| 13 | `3_13` | `health_metrics` | client (`lead_id`) | Staging join: lead_dim + sales + staff + all 7 metrics |
| 14 | `3_14` | `customer_health_profile` | client × version | SCD2 engagement versions — scoring, bands, hash (§6.4) |
| 15 | `3_15` | `customer_health_history` | run_date × client | Daily snapshot of open versions, 90-day window |
| 16 | `3_16` | `communication_state` | lead (`lead_id`) | Attempt-vs-contact state for ALL leads (44,742) |
| 17 | `3_17` | `communication_recency` | client (`lead_id`) | Per-channel recency + whose-turn for clients (context only) |
| 18 | `3_18` | `coaching_score_by_client_health` | client (`lead_id`) | The company's service report card (§6.5) |

### 6.1 The Population (`lead_dim`)

`lead_dim` defines who the health layer scores: leads whose **latest** status is `Coaching Client`. All versions of a lead are ranked first (`date_updated DESC`), and only then is the latest status checked — so a lead whose status changed away from Coaching Client leaves the population, and a lead that returned re-enters it. Current population: **1,374 clients**. Columns: `lead_id`, `lead_name` (the identity hash), `lead_display_name`, `status_label`, `date_updated`, `csm_id`, `Contracted_Value`, `Tier`, `Avatar`.

### 6.2 Identity, Staff, and Client Emails

**`staff_emails` (3_00)** — one row per staff person from the spine. A person is staff if ANY of: (a) they carry a 48-char organizer-login hash (`attendees.user_id` — only a logged-in staff member can organize an activity), (b) any real email on `@dataengineeracademy.com`, (c) any real email registered in `close_crm_users`. Whole-master rule: one marker makes the **entire** person staff — every hash, real email, and masked email. `all_emails` is the complete staff blocklist (189 staff persons currently).

**`lead_emails` (3_12)** — the client email view, built on the spine. `customer_emails` = every email attributed to the lead's persons **minus** the staff blocklist. `primary_customer_email` picks THE one client email by priority: (1) a real email from the lead's own CRM contact record, (2) any real non-staff email, (3) a masked email from the lead's own record, (4) any masked non-staff email — alphabetical tie-break. 55,254 leads carry email arrays; over 99% of clients have a primary. Search pattern: `email = primary_customer_email OR array_contains(customer_emails, email)`. The subtraction is validated leak-proof: no staff email — not even one of the 443 CRM directory addresses — appears among the customer email arrays.

**`staff_account_dim` (3_01)** — one row per staff person (189): the full staff account view, joining the `staff_emails` identity with the `staff_dim` CRM directory profile through the unique-email bridge (the spine's 32-char hashes and the CRM's 48-char login hashes are disjoint id spaces, so email is the only join key). Role attribution keeps labels separate — `csm_lead_ids`/`n_csm_leads`, `setter_lead_ids`/`n_setter_sales`, `closer_lead_ids`/`n_closer_sales` — guarded against fanout: `n_crm_matches` flags a person matching several CRM directory rows, and a directory email shared across staff persons attributes nothing (unique-email rule). A leaf dimension for per-staff analysis (health of a CSM's book, setter/closer deal attribution); nothing in the build chain joins on it.

**Intern clients** — flagged in `health_metrics` when the lead's own identity hash belongs to a staff person (a former client who now works here). They stay in the table, flagged; they are not part of the client book. Attention flags: `identity_unlinked` (the lead's hash exists nowhere in the spine) and `no_client_email` (every email on the lead is staff-owned or absent).

### 6.3 Engagement Metrics (3_05–3_11)

All metrics are per-source `lead_id` — no identity consolidation. Cross-system metrics join through the spine.

| # | Table | Source | Definition |
|---|---|---|---|
| 1 | `days_since_last_email` (3_05) | activities + email_body | Days since the client's last **inbox** email, excluding Calendly scheduling notifications and file-access-request system emails |
| 2 | `days_since_last_meeting` (3_06) | email_body | Days since the last Calendly 'A new event has been scheduled.' email (CRM meeting evidence) |
| 3 | `missed_calls_14_days` (3_07) | activities | Count of inbound `no-answer` calls in the **trailing 14 days** (a window metric, not a lifetime total) |
| 4 | `days_until_next_meeting` (3_08) | Calendly + spine | Days to the client's next **future** Calendly event. The CRM never records future meetings — Calendly is the only source; clients matched via spine hash or real email |
| 5 | `days_since_last_login` (3_09) | mdl_users + two hash bridges | Days since last platform login — the contact's hash AND the lead record's own hash, each matched against `USERNAME` ∪ `FIRSTNAME` ∪ `LASTNAME`. The platform's emails are 100% masked with zero overlap against the CRM's, so email matching is impossible — name-hash equality is the only bridge. Users whose Moodle `LASTACCESS` is the epoch default (1970) have never logged in; they score flag 0. Coverage: 633 clients; the remainder are unlinkable by any key in the export. |
| 6 | `days_since_last_meeting_calendly` (3_10) | Calendly + spine | Days since the client's latest **booking creation** (`EVENT_CREATED_AT` — booking recency, not event start; future bookings count) |
| 7 | `sentiment` (3_11) | `leads_processed_custom` | Sentiment label + days since last message from client / from team. The raw `student_sentiment` table cannot be joined to leads (its `CHANNEL_ID` is a different identifier type than any CRM key), so the CRM's pre-calculated fields carry this metric; one Slack channel is excluded by spec |

**Calendly dedup (spec):** duplicate `EVENT_URI`s resolve by `INVITEE_UPDATED_AT → EVENT_CREATED_AT → insert date`, latest wins.

### 6.4 Health Scoring (3_13 join → 3_14 SCD2 profile)

**`health_metrics` (3_13)** — one row per client: `lead_dim` LEFT JOIN `lead_emails`, `sales_fact`, `staff_dim` (CSM), and all seven metric tables, plus the intern and attention flags. This is the staging join everything downstream reads. Commercial context rides on the same row: `contract_start_date` reads the CRM's stored `Contract Start Date` custom field (latest snapshot per lead), falling back to the qualifying sale date when the field is empty — 1,329 of 1,374 clients carry one; the rest have neither field nor sale on file (a source-side gap, not a pipeline one). `date_of_sale` (the New Sale activity, per the spec) stays a separate column — the two dates are different facts.

**Scoring (`3_14`):** three engagement flags → engagement pattern → base score → adjustments → band.

* `slack_engaged` — sentiment record present and last client message ≤ 14 days (NULL when no sentiment record at all)
* `platform_engaged` — `days_since_last_login ≤ 14`
* `meeting_engaged` — both meeting sources (CRM email evidence and Calendly) must be present (cross-validation; NULL if either is missing); engaged when both are within 30 days

| Engagement Pattern | Base Score |
|---|---:|
| SLACK + PLATFORM + MEETING | 90 |
| SLACK + PLATFORM | 80 |
| PLATFORM + MEETING | 75 |
| SLACK + MEETING | 60 |
| PLATFORM ONLY | 50 |
| MEETING ONLY | 40 |
| SLACK ONLY | 30 |
| NO ENGAGEMENT | 20 |

`FINAL_SCORE = base + missed-call adjustment (−5 per missed call, floored at 0) + sentiment adjustment`. NO DATA leads get no adjustments. Scores are not capped.

| Band | Rule | Current |
|---|---|---:|
| GOOD | ≥ 75 | 205 |
| AVERAGE | 50–74 | 289 |
| POOR | < 50 | 417 |
| NO DATA | no engagement evidence at all | 463 |

**SCD Type 2 versioning (`customer_health_profile`):** one row per client **per engagement version**. A change-detection hash covers engagement state + identity + contract value — clock metrics (the `days_since_*` columns) are deliberately excluded so the hash does not churn daily. Each run:

1. **MERGE step 1** — open versions whose hash changed get `VRSN_END_DATE = today`; unchanged open versions get their clock metrics refreshed.
2. **INSERT step 2** — new leads, and leads whose version just closed, get a fresh open version (`VRSN_START_DATE = today`).
3. **UPDATE step 3** — leads that left the population close their open version; if they return later, step 2 opens a new one.

`VRSN_END_DATE IS NULL` = the current version. Read the current state with `WHERE VRSN_END_DATE IS NULL` — currently 1,374 open versions.

**`customer_health_history` (3_15)** — daily full snapshot of the open versions: one row per active client per day, all version columns + `run_date`, idempotent (deletes today's rows first), with a rolling **90-day retention**. The profile is the change log (what changed, when); the history is the trend log (band transitions, score drift).

### 6.5 Communication Tables and the Coaching Score

**`communication_state` (3_16)** — direction-aware state for **every** lead with contact history (44,742), built on the core distinction between an **attempt** and **contact made**: an outbound call is an attempt (only `disposition = 'answered'` is contact); a client inbound call counts as the client reaching out even if we missed it; sent email/SMS and Slack count as contact. Key columns: per-channel in/out recency (email, call, SMS, Slack), `client_reach_days` (their last contact or attempt), `we_connected_days` (our last contact made), `we_attempted_days` (our last attempt of any kind), `whose_turn` (`US` / `CLIENT` / `CURRENT`), and `all_out_attempted` (no outbound attempt of any kind in the last 14 days — the only windowed rule; `whose_turn` itself compares raw dates and never goes stale).

**`communication_recency` (3_17)** — the same logic narrowed to the client population (one row per client), with the reply-debt view: `last_contact_in_days` (the client's last inbound on **any** channel — a missed call counts), `whose_turn` (`THEIR TURN` / `OUR TURN` / `MUTUAL` / `NO CONTACT` / `NEVER REACHED`), and `exhausted_outreach_flag`. **Pure context table — not an input to any score.**

**`coaching_score_by_client_health` (3_18)** — grades the **company**, not the client: did we reply, did we deliver, did we initiate? One row per client, joining the open profile versions with communication recency and commercial context. The standard: a paying client who reaches out gets a reply within 24 hours (2 days is the last acceptable day); 3+ days unanswered = slipping; 30+ days with no attempt = abandoned; a paying client with zero meetings ever = contract without delivery; no company-initiated contact in 30+ days on a paying client = minimal service.

The promise ladder (`service_verdict`), current run:

| Verdict | Clients |
|---|---:|
| PROMISE KEPT | 1,257 |
| WITHIN THE PROMISE (0–2 days) | 58 |
| PROMISE SLIPPING (3–7 days) | 29 |
| PROMISE BROKEN (8–30 days) | 15 |
| PROMISE ABANDONED (31+ days) | 12 |

`company_service_score = 100 − (10 per day past the 2-day reply promise, capped at 40) − 20 (no attempt since their last reach) − 40 (abandoned) − 30 (contract without delivery) − 10 (minimal service)`, floored at 0. Thresholds are provisional constants — the business can retune them without touching the pipeline. Column comments in the table carry the customer-voice definitions ('Every day here is a day the client feels deprioritized'). Like 3_17, this is a context table — not an input to any score. Dollar columns self-mark as floors (contract value on file, not measured loss).

---

## 7. Deduplication & Key Rules

All deduplication is latest-wins: `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <timestamp> DESC)`, keep `rn = 1`.

| Dataset | Key | Ordering (latest wins) |
|---|---|---|
| Leads | `id` | `date_updated` → `bronze_insert_date` |
| Lead custom fields | `leads_processed_id` | `bronze_insert_date` |
| CRM activities | `id` / `activity_id` | `date_updated` → `bronze_insert_date` |
| Activity children (attendees / envelope / to) | `activity_id` | `bronze_updated_at` → `bronze_insert_date` |
| Calendly events | `EVENT_URI` | `INVITEE_UPDATED_AT` → `EVENT_CREATED_AT` → `bronze_insert_date` |
| CRM users | `USER_ID` / `id` | `date_updated` → `bronze_insert_date` |
| Platform users | `USER_ID` | `TIMEMODIFIED` |
| Student sentiment | `CHANNEL_ID` | `INSERT_DATE` |
| Identity spine | `group_key` (data-derived, stable across rebuilds) | `master_user_id` is a per-build sequence |

Client email lookup: `email = primary_customer_email OR array_contains(customer_emails, email)` on `gold.lead_emails`.

---

## 8. Job Orchestration (36 tasks)

| Field | Value |
|---|---|
| Job name | Student Engagement Pipeline |
| Schedule | Daily 8:00 AM Eastern (`0 0 8 * * ?`, America/New_York) |
| Compute | Serverless, `PERFORMANCE_OPTIMIZED`, one task per notebook |
| Max concurrent runs | 1 |
| Defined in | `databricks.yml` (Declarative Automation Bundle), deployed per `DEPLOY.md` |
| Run condition | ALL_SUCCESS — downstream tasks skip if any upstream fails |
| Typical duration | 20–25 minutes, ~3.5–4.5 DBUs/day (≈$1.30/day at list rate) |

**Execution waves** (the DAG in `databricks.yml`):

```
Wave 1   bronze_config → bronze_ingestion                                     (2 tasks)
Wave 2   Silver parents, parallel: silver_leads, silver_activities,
         silver_crm_users, silver_custom_activities, silver_calendly,
         silver_sentiment, silver_mdl_users                                  (7)
Wave 3   Silver children (each waits for ALL wave-2 parents): silver_leads_custom,
         silver_contacts, silver_email_body, silver_activities_attendees,
         silver_activities_envelope, silver_activities_to                    (6)
Wave 4   silver_contacts_emails (grandchild — waits for the children)         (1)
Wave 5   silver_user_mapping (2_8 spine — the LAST silver task)               (1)
Wave 6   Gold set 1, parallel — all depend ONLY on the spine (strict barrier:
         no gold runs before ALL silver is complete): gold_lead_dim,
          gold_staff_emails, gold_days_since_last_email,
         gold_days_since_last_meeting, gold_missed_calls,
         gold_days_until_next_meeting, gold_days_since_last_login,
         gold_calendly_metric, gold_sentiment                                (9)
Wave 7   gold_staff_dim — waits for staff_emails + lead_dim (3_01 builds
          staff_dim AND staff_account_dim from both)                       (1)
Wave 8   Gold set 2 (wait for ALL of set 1): gold_lead_emails,
         gold_leads_activity_fact, gold_sales_fact                         (3)
Wave 9   gold_health_metrics (waits for both facts + lead_emails)            (1)
Wave 10  gold_health_profile + gold_communication_state (wait for metrics)   (2)
Wave 11  gold_health_history + gold_communication_recency
         (wait for profile + state)                                         (2)
Wave 12  gold_coaching_score (waits for history + recency) — final task      (1)
```

The gold waves are intentionally sequenced around attribute completeness: `leads_activity_fact` and `sales_fact` enrich staff names, so they wait for `staff_dim`; `lead_emails` subtracts the staff blocklist, so it waits for `staff_emails`; `staff_dim` additionally builds `staff_account_dim` — the person-grain staff account view — from the staff identity and the client population, so it waits for `staff_emails` and `lead_dim`.

**Run economics** (measured): a normal nightly run costs ~3.5–4.5 DBUs; a full silver rebuild day is the expensive outlier (~15 DBUs, ~$5). Duration varies ±25% on serverless due to compute allocation while DBUs stay stable — monitor cost per run, not wall time.

---

## 9. Full Table Reference

Catalog: `crm_ingestion`. Schemas: `config` (1 table), `bronze` (9), `silver` (15), `gold` (20) — 45 tables, every one produced by a notebook in this repository.

### Config
| Table | Purpose |
|---|---|
| `config.bronze_config` | Bronze ingestion control: source, target, watermark column, load type, active flag |

### Bronze (9) — sources and row counts in §4
`leads_raw`, `lead_activites_raw`, `close_crm_users_raw`, `custom_activites_raw`, `all_payments`, `calendly_scheduled_events`, `student_sentiment`, `mdl_users_raw`, `lead_merges`

### Silver (15) — row counts and schemas in §5
`leads_processed`, `leads_processed_custom`, `leads_processed_contacts`, `leads_processed_contacts_emails`, `lead_activites_processed`, `lead_activites_email_body`, `lead_activites_processed_attendees`, `lead_activites_processed_envelope`, `lead_activites_processed_to`, `close_crm_users_processed`, `custom_activites_processed`, `calendly_scheduled_events_processed`, `student_sentiment_processed`, `mdl_users_processed`, `user_identity_map`

### Gold (20)
| Table | Type | Rows |
|---|---|---:|
| `staff_dim` | dimension | 443 |
| `staff_account_dim` | dimension (staff person) | 189 |
| `lead_dim` | dimension | 1,374 |
| `staff_emails` | identity view | 189 |
| `lead_emails` | identity view | 55,254 |
| `leads_activity_fact` | fact (event grain) | 94,459 |
| `sales_fact` | fact (latest sale per lead) | 43 |
| `days_since_last_email` | metric | 6,597 |
| `days_since_last_meeting` | metric | 7,442 |
| `missed_calls_14_days` | metric | 202 |
| `days_until_next_meeting` | metric | 416 |
| `days_since_last_login` | metric | 633 |
| `days_since_last_meeting_calendly` | metric | 1,380 |
| `sentiment` | metric | 1,674 |
| `health_metrics` | staging join | 1,374 |
| `customer_health_profile` | SCD2 fact | 2,889 (1,374 open) |
| `customer_health_history` | daily snapshot | rolling (4,057) |
| `communication_state` | context (all leads) | 44,742 |
| `communication_recency` | context (clients) | 1,371 |
| `coaching_score_by_client_health` | scorecard | 1,371 |

---

## 10. Operating Notes

* **Health is per-source `lead_id`** — project spec prohibits lead-ID consolidation. The identity spine maps people to their lead IDs; it never merges them.
* **Masked identifiers** — the CRM export masks emails and names as `user_…` hashes, and each source system hashes differently. Cross-system joins go through the identity spine; never join across systems on a raw hash or masked email.
* **The CRM never records future meetings** — all future-dated meeting evidence lives in the Calendly export. `days_until_next_meeting` is Calendly-only, matched through the spine.
* **`student_sentiment` cannot join to leads directly** — its `CHANNEL_ID` is a different identifier type than any CRM key. Sentiment reaches gold via the CRM's pre-calculated custom fields.
* **Clock metrics vs. state metrics** — `days_since_*` values refresh daily on unchanged SCD2 versions (they are excluded from the change hash); engagement flags, patterns, bands, and scores change only when the underlying state changes.
* **NO DATA is a real band, not an error** — a client with no engagement evidence on any channel scores NO DATA (463 of 1,374 currently). The `identity_unlinked` and `no_client_email` flags in `health_metrics` mark the leads that need human review.
* **Cost** — a normal nightly run is ~$1.30 at list rate (~$0.65 at the promo rate). Full silver rebuilds are the rare, deliberate outlier (~$5).
