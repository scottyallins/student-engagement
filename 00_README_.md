# DEA Student Engagement Pipeline

A Databricks Medallion Architecture pipeline (Bronze → Silver → Gold) that turns five source systems — Close CRM, Calendly, the Moodle learning platform, Slack-derived sentiment, and lead-merge history, all exported from PostgreSQL — into two decision products for a coaching business:

1. **`gold.customer_health_profile`** — the Customer Health Profile: one row per client per engagement *version*, scoring each client's engagement and tracking exactly when it changed (SCD Type 2).
2. **`gold.coaching_score_by_client_health`** — the company's own report card: one row per client measuring whether the company kept its service promises (reply speed, delivery, initiation).

Everything the pipeline does lives in this repository: the Bronze ingestion, Silver processing, and Gold build notebooks, plus the nightly job itself, defined as code in `databricks.yml` (a Declarative Automation Bundle) and deployed with the Databricks CLI.

The job runs **36 tasks, daily at 8:00 AM Eastern, on serverless compute**, completing in roughly 25–30 minutes for about $1.30/day at list pricing.

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
* Silver tasks are CDC on `bronze_updated_at` — a re-run reprocesses only rows that changed, upserting them by primary key (no duplicates, no version churn).
* Gold tables are `CREATE OR REPLACE`, or idempotent MERGE / DELETE+INSERT patterns (the SCD2 profile and the daily history both delete-then-insert today's rows, so re-running a day is safe).

**Secrets:** the PostgreSQL connection credentials live in the Databricks secret scope `dea-rds` (keys: `host`, `port`, `database`, `username`, `password`). No connection values are stored in code or git.

---

## 2. Architecture Overview

**Bronze — land everything, exactly as exported.** Nine PostgreSQL tables are loaded over JDBC into Delta tables with three columns each: `raw_data` (the source row as a JSON string), `insert_date`, and `bronze_updated_at`. Loads are config-driven from `config.bronze_config`, incremental on `insert_date`, error-isolated per table (one failed source logs its error and the remaining tables still land; the task then fails so the alert fires), and batched with parallel JDBC partitions so large pulls are OOM-safe and paced against the source database.

**Silver — parse, normalize, and identify.** A shared engine (the `utils` notebook) reads each bronze table, repairs malformed JSON, deduplicates snapshot re-dumps, infers the schema, parses the JSON, and upserts the silver parent tables — all under a CDC watermark so only changed rows are reprocessed. Parents are MERGE-by-primary-key (one row per id), and each CDC batch's columns are first reconciled to the target table's Delta types, so per-batch schema inference can never drift a nested column into a shape the write would reject. Array fields become child tables through dedicated child notebooks. The layer finishes with the **identity spine** (`silver.user_identity_map`): one row per *person* across all systems, carrying every identity hash, email, and lead attribution. The spine is what makes cross-system metrics (Calendly bookings, platform logins) possible — the CRM export masks emails and names as `user_…` hashes, so no system can be joined to another directly.

**Gold — the business star schema.** Eighteen notebooks build nineteen tables (every table can be re-run or recovered independently — detachment; `3_01` builds two: the staff dimension and its account view): two staff tables, the all-leads dimension (real names + client email attribution), two facts, seven single-metric tables, a staff-email blocklist, a staging join, the SCD2 health profile, its daily history, two communication tables, and the coaching score. `lead_id` is the shared foreign key across the layer; facts keep their natural event grain.

**Data volume (current counts):** Bronze holds ≈21.1M rows across 9 source tables — the source re-dumps full tables on every export, and bronze faithfully keeps every dump. Silver distills this to ≈2.6M rows across 16 tables (one row per id on parents, exploded children, identity spine). Gold lands at ≈1.4M rows across 19 tables, with the health layer at exactly **1,384 active coaching clients** per table.

### Data Flow

```
PostgreSQL (raw schema — 9 source tables)
    |
    v  JDBC, config-driven (config.bronze_config), incremental on insert_date,
       error-isolated per table, batched + parallel partitions
BRONZE  crm_ingestion.bronze          9 raw Delta tables (raw_data JSON, insert_date, bronze_updated_at)
    |
    v  %run ../utils → process_table(): JSON repair → dedup re-dumps → schema inference
       → parse → upsert parent by PK (CDC on bronze_updated_at, batch schema
         reconciled to the target's Delta types); child notebooks explode arrays
SILVER  crm_ingestion.silver          16 tables: 7 parents + 8 children/grandchild + identity spine
    |   silver.user_identity_map (2_8) — one row per person: hashes, emails, lead_ids
    v
GOLD    crm_ingestion.gold            19 tables, 18 notebooks (3_01 builds two):
       star schema     staff_dim, staff_account_dim, lead_dim (all-leads spine:
                       real names + client email attribution), leads_activity_fact, sales_fact
       identity        staff_emails (staff blocklist)
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
+- 2_Silver/                                      <- 16 nightly notebooks (all call %run ../utils),
|  |                                                 + 2 on-demand + 2 maintenance notebooks
|  +- 2_1_01_Silver_leads_processed                       -> leads_processed (leads parent)
|  +- 2_1_02_Silver_leads_processed_custom               -> leads_processed_custom (custom fields + identity keys)
|  +- 2_1_03_01_Silver_leads_processed_contacts         -> leads_processed_contacts (contacts array, from parent)
|  +- 2_1_03_02_Silver_leads_processed_contacts_emails  -> leads_processed_contacts_emails (email grandchild)
|  +- 2_2_01_Silver_lead_activites_processed            -> lead_activites_processed (activities parent)
|  +- 2_2_02_Silver_lead_activites_email_body           -> lead_activites_email_body (email body text)
|  +- 2_2_03_Silver_lead_activites_attendees            -> lead_activites_processed_attendees (current state)
|  +- 2_2_04_Silver_lead_activites_processed_envelope   -> lead_activites_processed_envelope (current state)
|  +- 2_2_05_Silver_lead_activites_processed_to         -> lead_activites_processed_to (current state)
|  +- 2_2_06_Silver_lead_activites_processed_custom     -> lead_activites_processed_custom (on-demand extract)
|  +- 2_2_07_lead_activites_processed_custom_fields      -> lead_activites_processed_custom_fields (on-demand)
|  +- 2_3_Silver_close_crm_users_processed              -> close_crm_users_processed (CRM users)
|  +- 2_4_01_Silver_custom_activites_processed          -> custom_activites_processed (activity type metadata)
|  +- 2_4_02_Silver_custom_activites_processed_fields   -> custom_activites_processed_fields (cf_ field dictionary)
|  +- 2_5_Silver_calendly_scheduled_events_processed    -> calendly_scheduled_events_processed
|  +- 2_6_Silver_student_sentiment_processed            -> student_sentiment_processed
|  +- 2_7_Silver_mdl_users_processed                   -> mdl_users_processed (platform users)
|  +- 2_8_Silver_user_mapping                          -> user_identity_map (the identity spine)
|  +- rebuild_silver_parents_latest_per_id            <- one-off maintenance: full-history parent rebuild, latest-per-id
|  +- rebuild_silver_children_from_parents            <- one-off maintenance: rebuild all children from parents
+- 3_Gold/                                        <- 18 notebooks, one table per notebook (3_01 builds two)
   +- 3_00_Gold_staff_emails                        -> staff_emails (staff blocklist)
   +- 3_01_Gold_staff_dim                           -> staff_dim + staff_account_dim
   +- 3_02_Gold_leads_activity_fact                 -> leads_activity_fact
   +- 3_03_Gold_sales_fact                          -> sales_fact
   +- 3_04_Gold_lead_dim                           -> lead_dim (all-leads spine: real names, client email attribution)
   +- 3_05_Gold_days_since_last_email               -> days_since_last_email
   +- 3_06_Gold_days_since_last_meeting             -> days_since_last_meeting
   +- 3_07_Gold_missed_calls_14_days                -> missed_calls_14_days
   +- 3_08_Gold_days_until_next_meeting             -> days_until_next_meeting
   +- 3_09_Gold_days_since_last_login               -> days_since_last_login
   +- 3_10_Gold_days_since_last_meeting_calendly    -> days_since_last_meeting_calendly
   +- 3_11_Gold_sentiment                          -> sentiment
   +- 3_13_Gold_health_metrics                     -> health_metrics (staging join)
   +- 3_14_Gold_customer_health_profile             -> customer_health_profile (SCD2)
   +- 3_15_Gold_customer_health_history             -> customer_health_history (daily snapshot)
   +- 3_16_Gold_communication_state                 -> communication_state (all leads)
   +- 3_17_Gold_communication_recency               -> communication_recency (clients)
   +- 3_18_Gold_coaching_score_by_client_health    -> coaching_score_by_client_health
```

---

## 4. Bronze Layer (9 source tables + source inventory)

| # | Bronze Table | Source | Used Downstream | Rows | Description |
|---|---|---|:---:|---:|---|
| 1 | `crm_ingestion.bronze.leads_raw` | `raw.leads_raw` | Yes | 602,440 | Lead/contact/account data |
| 2 | `crm_ingestion.bronze.lead_activites_raw` | `raw.lead_activites_raw` | Yes | 502,632 | CRM activities (Email, Call, Meeting, Note, SMS, CustomActivity) |
| 3 | `crm_ingestion.bronze.close_crm_users_raw` | `raw.close_crm_users_raw` | Yes | 95,862 | CRM user directory (staff) |
| 4 | `crm_ingestion.bronze.custom_activites_raw` | `raw.custom_activites_raw` | Yes | 167 | Custom activity type metadata |
| 5 | `crm_ingestion.bronze.all_payments` | `raw.all_payments` | No | 282,644 | Payment records (bronze only — no silver consumer) |
| 6 | `crm_ingestion.bronze.calendly_scheduled_events` | `raw.calendly_scheduled_events` | Yes | 16,302,380 | Scheduled meeting events (full re-dump each export) |
| 7 | `crm_ingestion.bronze.student_sentiment` | `raw.student_sentiment` | Yes | 245,518 | Sentiment + Slack message data |
| 8 | `crm_ingestion.bronze.mdl_users_raw` | `raw.mdl_users_raw` | Yes | 2,675,697 | Moodle platform users |
| 9 | `crm_ingestion.bronze.lead_merges` | `raw.lead_merges` | No | 376,815 | Lead merge history (bronze only — no silver consumer) |

Every bronze source table has the same three columns: `raw_data` (JSON string), `insert_date`, `bronze_updated_at`. A tenth table, `source_inventory`, is a source audit log — one row per raw source table (column count, in-pipeline flag, inventory date).

**Ingestion mechanics (`1_2_Bronze_ingestion`):**
* **Config-driven** — the loader iterates over active rows in `config.bronze_config` (`source_table`, `target_table`, `incremental_column`, `load_type`, `active`). Adding a source is a config row.
* **Incremental watermark** — each load reads `insert_date > MAX(insert_date)` already in the bronze target. A failed or interrupted run leaves the watermark at the last successful row, so the next run re-pulls exactly the missed rows — no duplicates, no manual recovery.
* **Error isolation** — the nine sources have no interdependency, so one failed JDBC load never stops the others: load everything possible, print a per-table summary, then raise so the job alert fires.
* **OOM-safe batching** — JDBC fetch size 10,000 (the driver default buffers the entire result set in memory); large pulls are split into batches sized from `pg_stats` row-width estimates, batches over 64 MB are fetched in parallel across 8 JDBC connections, with a pause between batches to protect the source database.

---

## 5. Silver Layer (16 tables)

### 5.1 The Shared Engine (`utils`)

All silver notebooks start with `%run ../utils`, then call `process_table('bronze_table', 'silver_table')` (or use the engine's building blocks directly for child tables).

| Function | Purpose |
|---|---|
| `repair_json_udf` | Detects and repairs malformed JSON: type-based wrapper unwrapping (stringified inner JSON, single-key data-array wrappers), quote repair, recursion. An escalation ladder — direct parse first, surgery only when needed. Free-text keys that break parsing (`description`, `notes`, `body_text`, `subject`, `text`, `note`) are nulled before repair. |
| `parse_bronze_table()` | Reads a bronze table, deduplicates snapshot re-dumps, detects the wrapper format, repairs dirty rows once into a temp table, infers the schema from up to 200 sampled rows (`schema_of_json_agg` with single-row fallback), parses with `from_json`, and explodes the top-level array. Aborts the write if every field parses to NULL (wrong-schema guard). |
| `get_silver_watermark()` | Returns `MAX(bronze_updated_at)` from a silver table — the CDC watermark, or None on first run. |
| `write_silver()` | Writes a DataFrame to silver. In CDC mode (watermark set): the seven parents upsert by their pinned primary key — `_merge_silver` MERGEs by id (existing ids UPDATE in place, new ids INSERT), with the batch's columns first reconciled to the target's Delta types (`_reconcile_to_target` → `_cast_tree`; per-batch schema inference drifts on nested types, and Delta rejects a MERGE whose source struct/array types differ from the target's) and any new top-level columns ALTER-added before the MERGE; child tables delete+append exactly the rows newer than the watermark (delete window matches the read filter to the microsecond). First run is a full overwrite. |
| `flatten_structs()` | Flattens StructType columns into individual columns (the `custom` struct is kept for separate extraction). |
| `explode_to_child()` | Explodes an array column into a child table; `dry_run=True` prints the lineage without writing. |
| `extract_custom()` | Extracts the `custom` struct and `custom_cf_` fields into a child table and a `custom_fields` MAP on the parent. |
| `process_table()` | Orchestrator: parse → optional flatten → custom extraction → write parent → print the discovered lineage tree (children are printed in dry-run; child tables are built by the dedicated child notebooks). |

**One row per id, not version history:** bronze receives a full re-dump from the source on every export, so the engine deduplicates byte-identical re-dumps and anti-joins on a payload hash to skip source versions silver already holds, then upserts each id's latest version into the parent. Silver is CDC: only rows newer than the watermark are reprocessed. Gold consumers still take the latest per key — `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <timestamp> DESC)` keeps `rn = 1` — a guard that also covers the exploded child arrays.

**Schema discovery note:** schema inference is type-aware — sample rows are drawn per `_type`, so fields that appear only in rare activity types (the email body arrays, meeting timestamps) are captured by the parent parse, which carries 139 payload fields. The email-body child table exists for grain, not for missing fields: `body_text_quoted` arrives as an array of quoted fragments, and `lead_activites_email_body` explodes it to one row per activity × fragment.

### 5.2 Parent Tables (7)

| Notebook | Bronze Source | Silver Table | Rows | Contents |
|---|---|---|---:|---|
| `2_1_01` | `leads_raw` | `leads_processed` | 66,936 | Flattened leads (one row per id): `id`, `name` (identity hash), `display_name`, `status_label`, `date_updated`, + the raw arrays (`contacts`, `opportunities`, `tasks`, `custom` struct) |
| `2_2_01` | `lead_activites_raw` | `lead_activites_processed` | 1,060,845 | Activities: `id`, `lead_id`, `type`, `activity_at`, `direction`, `status`, `disposition`, `note`, `text` (SMS body — email body is NOT here), `attendees` array, `custom_fields` MAP, … 139 fields |
| `2_3` | `close_crm_users_raw` | `close_crm_users_processed` | 444 | CRM users/staff (real names, emails) |
| `2_4` | `custom_activites_raw` | `custom_activites_processed` | 30 | Custom activity types, fields, outcomes |
| `2_5` | `calendly_scheduled_events` | `calendly_scheduled_events_processed` | 102,774 | Meeting events: `EVENT_URI`, `INVITEE_NAME`/`INVITEE_EMAIL` (real emails), `EVENT_START_TIME`, `EVENT_CREATED_AT`, `INVITEE_UPDATED_AT` |
| `2_6` | `student_sentiment` | `student_sentiment_processed` | 2,269 | Sentiment + Slack data (`CHANNEL_ID`, `CHANNEL_NAME`) |
| `2_7` | `mdl_users_raw` | `mdl_users_processed` | 26,288 | Platform users: `USERNAME`/`FIRSTNAME`/`LASTNAME` are identity hashes, `LASTACCESS` |

### 5.3 Child Tables (8)

Two build patterns, both CDC:

* **Lead children** (`2_1_02`, `2_1_03_01`, `2_1_03_02`) — explode arrays from the silver *parent* (`leads_processed`), reading only parent rows newer than the child's own watermark. With one-row-per-id parents they hold one row per id × child key.
* **Activity children** (`2_2_03`, `2_2_04`, `2_2_05`) — read the activity parent's changed rows, keep the **latest version per activity**, and replace exactly the affected activity's rows (an exact-key DELETE, because a timestamp-only delete would leave stale siblings). These tables represent **current state**. First run, or a legacy schema without `bronze_updated_at`, triggers a full rebuild.
* `2_2_02` re-parses bronze directly for the per-fragment email body grain (see §5.1).
* `2_4_02` explodes the custom-activities parent's `fields` array into the `cf_…` key → field-name dictionary that `sales_fact` resolves Setter/Closer/Contract Value/Program through.
* `2_2_06` / `2_2_07` (on-demand, not in the nightly DAG) hold the activities parent's custom-field extract.

| Notebook | Silver Table | Rows | One row per… |
|---|---|---:|---|
| `2_1_02` | `leads_processed_custom` | 92,473 | Lead × custom-field snapshot (gold reads the latest per lead — §7): `CSM`, `Contracted_Value`, `Tier`, `Avatar`, `SENTIMENTS_LAST_30_DAYS`, `DAYS_SINCE_LAST_*` (CRM pre-calculated), `Channel_ID` + ~160 custom fields |
| `2_1_03_01` | `leads_processed_contacts` | 102,059 | Lead × contact: `id` (cont_id), `name` (identity hash), emails/phones/urls arrays |
| `2_1_03_02` | `leads_processed_contacts_emails` | 120,933 | Lead × contact × email: `email`, `type`, `is_unsubscribed` (mostly masked `user_…@masked.com`) |
| `2_2_02` | `lead_activites_email_body` | 188,353 | Email activity × body text: `body_text` extracted from the nested `body_text_quoted` array |
| `2_2_03` | `lead_activites_processed_attendees` | 64,893 | Activity × attendee: `contact_id`, real `email` (the only real client email in the CRM export), `is_organizer`, `user_id` (48-char staff login hash), `name`, `status` — current state |
| `2_2_04` | `lead_activites_processed_envelope` | 475,833 | Email activity envelope (from/to masked addresses) — current state |
| `2_2_05` | `lead_activites_processed_to` | 140,004 | Email activity recipients — current state |
| `2_4_02` | `custom_activites_processed_fields` | 941 | Activity type × field definition: the `cf_…` key → field-name dictionary (`Setter`, `Closer`, `Contract Value`, `Program`) that `sales_fact` resolves through |

**Why `lead_activites_email_body` exists:** `body_text_quoted[0].text` carries the actual email body — including Calendly's 'A new event has been scheduled.' message, which `3_06` uses as meeting evidence. The `text` column on `lead_activites_processed` is SMS text only; emails have NULL there.

### 5.4 The Identity Spine (`2_8` → `silver.user_identity_map`)

One row per **person** across all systems. This is the piece that makes cross-system gold metrics possible: the CRM masks emails/names as `user_<32-hex>` hashes, each source system hashes differently, and real emails appear only in activities' attendee records, Calendly invites, and CRM contact records. The spine links all of them.

**Build (6 steps in the notebook):**
1. **Collect** every `user_32digit` hash + email from all silver tables: contacts names, attendees (real personal emails via the `contact_id` bridge), organizers (48-char login hashes → staff email), Calendly `INVITEE_EMAIL`/`EVENT_HOST_EMAIL`, `mdl_users` emails — deduplicated before joins to prevent row multiplication.
2. **Group by email** — one row per email with every linked hash, lead, and contact attribution.
3. **Pivot** back to source-column shapes (each source table joins on its own column name).
4. **Create `master_user_id`** — bundle hashes that share the same **real** email into one person; masked emails stay for lookup but cannot group. Two guards prevent false bundles: a hash carrying more than 10 real emails (shared/system hash) and an email carrying more than 5 distinct hashes (community/shared inbox) are excluded from grouping but remain searchable.
5. **Materialize** `silver.user_identity_map`: `master_user_id`, `user_32digits`, `real_emails`, `masked_emails`, `lead_ids`, `cont_ids`, `source_tables` — one row per person (136,132 currently; ≈42K with a real email, ≈55K linked to leads).
6. **Validate** spine coverage against the gold meeting signals (future bookings, any bookings).

**Canonical email contract:** every email is normalized exactly once, in Step 1's union wrapper — `NULLIF(LOWER(TRIM(email)), '')`. All spine outputs are canonical by construction; gold tables join on emails without re-normalizing.

`master_user_id` is a person key across all systems — NOT a lead_id. Most people have no CRM lead. Joins downstream remain on `lead_id`; the spine exposes each person's `lead_ids` array, and gold consumers `explode()` it. Project spec: lead IDs are never merged or consolidated — the spine is a lookup, not a rewrite.

---

## 6. Gold Layer (19 tables)

Star schema keyed on `lead_id`. The health and communication tables are one row per client; `lead_dim`, `missed_calls_14_days`, and `sentiment` are one row per lead (all leads); the facts keep their natural event grain.

| # | Notebook | Table | Grain / Key | Purpose |
|---|---|---|---|---|
| 0 | `3_00` | `staff_emails` | staff person (`master_user_id`) | Staff email blocklist (see §6.2) |
| 1 | `3_01` | `staff_dim` | staff (`csm_id`) | Coaches, CSMs, setters/closers with real names/emails |
| 1b | `3_01` | `staff_account_dim` | staff person (`master_user_id`) | Full staff account view: identity + CRM profile + role attribution (§6.2) |
| 2 | `3_02` | `leads_activity_fact` | activity (`activity_id`) | One row per activity, latest version, staff-attributed |
| 3 | `3_03` | `sales_fact` | lead (latest sale) | Latest qualifying sale per lead with setter/closer |
| 4 | `3_04` | `lead_dim` | lead (`lead_id`) | The all-leads spine: real names, client email attribution, commercial attributes (§6.1) |
| 5–11 | `3_05`–`3_11` | 7 metric tables | lead (`lead_id`) | Single engagement metrics (§6.3) |
| 12 | `3_13` | `health_metrics` | client (`lead_id`) | Staging join: the client book of lead_dim + sales + staff + all 7 metrics + real client names |
| 13 | `3_14` | `customer_health_profile` | client × version | SCD2 engagement versions — scoring, bands, hash (§6.4) |
| 14 | `3_15` | `customer_health_history` | run_date × client | Daily snapshot of open versions, 90-day window |
| 15 | `3_16` | `communication_state` | lead (`lead_id`) | Attempt-vs-contact state for ALL leads (51,298) |
| 16 | `3_17` | `communication_recency` | client (`lead_id`) | Per-channel recency + whose-turn for clients (context only) |
| 17 | `3_18` | `coaching_score_by_client_health` | client (`lead_id`) | The company's service report card (§6.5) — plus the change-data contracts cell (§6.6) |

### 6.1 The All-Leads Dimension (`lead_dim`) and the Client Population

`lead_dim` is one row per lead across **all** statuses — currently 66,936 — and serves two roles: the population-independent metrics (`missed_calls_14_days`, `sentiment`) cover every lead in it, and the health layer selects its client book from it. Columns: `lead_id`, `LEAD_NAME` (the real name, see below), `lead_user_hash_name` (the identity hash), `lead_display_name`, `status_label`, `date_updated`, `csm_id`, `Contracted_Value`, `Tier`, `Avatar`, plus the client email attribution (§6.2).

The client book: leads whose **latest** status is `Coaching Client` — all versions of a lead are ranked (`date_updated DESC`) and only then is the latest status checked, so a lead whose status changed away leaves the population and re-enters on return. Current population: **1,384 clients**, scored by everything from `health_metrics` down.

Real names: the CRM export masks every structured person-name field as a `user_…` hash. `LEAD_NAME` recovers the real name from free text where names appear unmasked, tried in order — the Calendly invitee line quoted in email notification bodies (`body_text_quoted` on `lead_activites_processed`), the greeting line of outbound SMS texts, and the greeting line of outgoing email bodies (`all_names` / `text_all_names` / `email_all_names` hold the per-rung candidates; distinct names per lead are joined alphabetically with ` / `) — falling back to the identity hash when no rung matches. Currently 1,165 of 1,384 clients carry a real name. Identity-keyed logic (intern flags, unlinked flags) still runs on the hash.

### 6.2 Identity, Staff, and Client Emails

**`staff_emails` (3_00)** — one row per staff person from the spine. A person is staff if ANY of: (a) they carry a 48-char organizer-login hash (`attendees.user_id` — only a logged-in staff member can organize an activity), (b) any real email on `@dataengineeracademy.com`, (c) any real email registered in `close_crm_users`. Whole-master rule: one marker makes the **entire** person staff — every hash, real email, and masked email. `all_emails` is the complete staff blocklist (189 staff persons currently).

**Client email attribution (`lead_dim`)** — built on the spine inside `lead_dim`. `customer_emails` = every email attributed to the lead's persons **minus** the whole-person staff blocklist. `primary_customer_email` picks THE one client email, with the lead's own CRM contact record's real email first; `non_primary_customer_emails` carries the rest. 66,132 of 66,936 leads carry email arrays; 1,382 of 1,384 clients have a primary. Search pattern: `email = primary_customer_email OR array_contains(customer_emails, email)`. The subtraction is validated leak-proof: no staff email appears among the customer email arrays.

**`staff_account_dim` (3_01)** — one row per staff person (189): the full staff account view, joining the `staff_emails` identity with the `staff_dim` CRM directory profile through the unique-email bridge (the spine's 32-char hashes and the CRM's 48-char login hashes are disjoint id spaces, so email is the only join key). Role attribution keeps labels separate — `csm_lead_ids`/`n_csm_leads`, `setter_lead_ids`/`n_setter_sales`, `closer_lead_ids`/`n_closer_sales` — guarded against fanout: `n_crm_matches` flags a person matching several CRM directory rows, and a directory email shared across staff persons attributes nothing (unique-email rule). A leaf dimension for per-staff analysis (health of a CSM's book, setter/closer deal attribution); nothing in the build chain joins on it.

**Intern clients** — flagged in `health_metrics` when the lead's own identity hash belongs to a staff person (a former client who now works here). They stay in the table, flagged; they are not part of the client book. Attention flags: `identity_unlinked` (the lead's hash exists nowhere in the spine) and `no_client_email` (every email on the lead is staff-owned or absent).

### 6.3 Engagement Metrics (3_05–3_11)

All metrics are per-source `lead_id` — no identity consolidation. Cross-system metrics join through the spine.

| # | Table | Source | Definition |
|---|---|---|---|
| 1 | `days_since_last_email` (3_05) | activities + email_body | Days since the client's last **inbox** email, excluding Calendly scheduling notifications and file-access-request system emails |
| 2 | `days_since_last_meeting` (3_06) | email_body | Days since the last Calendly 'A new event has been scheduled.' email (CRM meeting evidence) |
| 3 | `missed_calls_14_days` (3_07) | activities + `lead_dim` | Count of inbound `no-answer` calls in the **trailing 14 days** (a window metric, not a lifetime total), covering every lead in the all-leads spine |
| 4 | `days_until_next_meeting` (3_08) | Calendly + spine | Days to the client's next **future** Calendly event. The CRM never records future meetings — Calendly is the only source; clients matched via spine hash or real email |
| 5 | `days_since_last_login` (3_09) | mdl_users + two hash bridges | Days since last platform login — the contact's hash AND the lead record's own hash, each matched against `USERNAME` ∪ `FIRSTNAME` ∪ `LASTNAME`. The platform's emails are 100% masked with zero overlap against the CRM's, so email matching is impossible — name-hash equality is the only bridge. Users whose Moodle `LASTACCESS` is the epoch default (1970) have never logged in; they score flag 0. Coverage: 630 clients; the remainder are unlinkable by any key in the export. |
| 6 | `days_since_last_meeting_calendly` (3_10) | Calendly + spine | Days since the client's latest **booking creation** (`EVENT_CREATED_AT` — booking recency, not event start; future bookings count) |
| 7 | `sentiment` (3_11) | `leads_processed_custom` + `lead_dim` | Sentiment label + days since last message from client / from team, covering every lead in the all-leads spine. The raw `student_sentiment` table cannot be joined to leads (its `CHANNEL_ID` is a different identifier type than any CRM key), so the CRM's pre-calculated fields carry this metric; one Slack channel is excluded by spec |

**Calendly dedup (spec):** duplicate `EVENT_URI`s resolve by `INVITEE_UPDATED_AT → EVENT_CREATED_AT → insert date`, latest wins.

### 6.4 Health Scoring (3_13 join → 3_14 SCD2 profile)

**`health_metrics` (3_13)** — one row per client: the client book of `lead_dim` LEFT JOIN `sales_fact`, `staff_dim` (CSM), and all seven metric tables, plus the intern and attention flags. This is the staging join everything downstream reads. `lead_name` carries the client's **real name** straight from `lead_dim`'s `LEAD_NAME` (the three-rung recovery — §6.1; 1,165 of 1,384 clients carry one, the rest keep the identity hash). Identity-keyed logic (intern flag, unlinked flag) still runs on the hash. Commercial context rides on the same row: `contract_start_date` reads the CRM's stored `Contract Start Date` custom field (latest snapshot per lead), falling back to the qualifying sale date when the field is empty — 1,335 of 1,384 clients carry one; the rest have neither field nor sale on file (a source-side gap, not a pipeline one). `date_of_sale` (the New Sale activity, per the spec) stays a separate column — the two dates are different facts.

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
| GOOD | ≥ 75 | 169 |
| AVERAGE | 50–74 | 321 |
| POOR | < 50 | 430 |
| NO DATA | no engagement evidence at all | 464 |

**SCD Type 2 versioning (`customer_health_profile`):** one row per client **per engagement version**. A change-detection hash covers engagement state + identity + contract value — clock metrics (the `days_since_*` columns) are deliberately excluded so the hash does not churn daily. Each run:

1. **MERGE step 1** — open versions whose hash changed get `VRSN_END_DATE = today`; unchanged open versions get their clock metrics refreshed.
2. **INSERT step 2** — new leads, and leads whose version just closed, get a fresh open version (`VRSN_START_DATE = today`).
3. **UPDATE step 3** — leads that left the population close their open version; if they return later, step 2 opens a new one.

`VRSN_END_DATE IS NULL` = the current version. Read the current state with `WHERE VRSN_END_DATE IS NULL` — currently 1,384 open versions (7,291 versions in total).

**`customer_health_history` (3_15)** — daily full snapshot of the open versions: one row per active client per day, all version columns + `run_date`, idempotent (deletes today's rows first), with a rolling **90-day retention**. The profile is the change log (what changed, when); the history is the trend log (band transitions, score drift).

### 6.5 Communication Tables and the Coaching Score

**`communication_state` (3_16)** — direction-aware state for **every** lead with contact history (51,298), built on the core distinction between an **attempt** and **contact made**: an outbound call is an attempt (only `disposition = 'answered'` is contact); a client inbound call counts as the client reaching out even if we missed it; sent email/SMS and Slack count as contact. Key columns: per-channel in/out recency (email, call, SMS, Slack), `client_reach_days` (their last contact or attempt), `we_connected_days` (our last contact made), `we_attempted_days` (our last attempt of any kind), `whose_turn` (`US` / `CLIENT` / `CURRENT`), and `all_out_attempted` (no outbound attempt of any kind in the last 14 days — the only windowed rule; `whose_turn` itself compares raw dates and never goes stale).

**`communication_recency` (3_17)** — the same logic narrowed to the client population (one row per client), with the reply-debt view: `last_contact_in_days` (the client's last inbound on **any** channel — a missed call counts), `whose_turn` (`THEIR TURN` / `OUR TURN` / `MUTUAL` / `NO CONTACT` / `NEVER REACHED`), and `exhausted_outreach_flag`. **Pure context table — not an input to any score.**

**`coaching_score_by_client_health` (3_18)** — grades the **company**, not the client: did we reply, did we deliver, did we initiate? One row per client, joining the open profile versions with communication recency and commercial context. The standard: a paying client who reaches out gets a reply within 24 hours (2 days is the last acceptable day); 3+ days unanswered = slipping; 30+ days with no attempt = abandoned; a paying client with zero meetings ever = contract without delivery; no company-initiated contact in 30+ days on a paying client = minimal service.

The promise ladder (`service_verdict`), current run:

| Verdict | Clients |
|---|---:|
| PROMISE KEPT | 1,252 |
| WITHIN THE PROMISE (0–2 days) | 62 |
| PROMISE SLIPPING (3–7 days) | 39 |
| PROMISE BROKEN (8–30 days) | 19 |
| PROMISE ABANDONED (31+ days) | 12 |

`company_service_score = 100 − (10 per day past the 2-day reply promise, capped at 40) − 20 (no attempt since their last reach) − 40 (abandoned) − 30 (contract without delivery) − 10 (minimal service)`, floored at 0. Thresholds are provisional constants — the business can retune them without touching the pipeline. Column comments in the table carry the customer-voice definitions ('Every day here is a day the client feels deprioritized'). Like 3_17, this is a context table — not an input to any score. Dollar columns self-mark as floors (contract value on file, not measured loss).

### 6.6 Change-Data Contracts

Eight tables carry enforced constraints — PRIMARY KEY, NOT NULL, and CHECK — applied by the contracts cell at the end of `3_18`, the **final task of the nightly run**, so the four CTAS-rebuilt gold tables regain their constraints every night:

* **Silver parents** — PRIMARY KEY on `id` (`lead_activites_processed`, `close_crm_users_processed`): one row per id, enforced.
* **Gold one-row-per-lead tables** — PRIMARY KEY on `lead_id`: `health_metrics`, `communication_state`, `communication_recency`, `coaching_score_by_client_health`.
* **`customer_health_profile`** — composite PRIMARY KEY (`LEAD_ID`, `VRSN_START_DATE`, `INSERT_DATE`), NOT NULL on the version columns, and a CHECK that a version never ends before it starts (`VRSN_END_DATE IS NULL OR VRSN_END_DATE >= VRSN_START_DATE`); the one-open-version-per-lead invariant is re-verified on every run (it is not expressible as a column CHECK).
* **`customer_health_history`** — composite PRIMARY KEY (`run_date`, `LEAD_ID`), NOT NULL on both.

The guards double as data-quality tripwires: a PRIMARY KEY is never applied over violating rows — NULL ids or duplicate keys block loudly instead of passing silently — so corruption surfaces the day it lands, not at query time.

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

Client email lookup: `email = primary_customer_email OR array_contains(customer_emails, email)` on `gold.lead_dim`.

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
| Typical duration | 25–30 minutes, ~3.5–4.5 DBUs/day (≈$1.30/day at list rate) |

**Execution waves** (the DAG in `databricks.yml`):

```
Wave 1   bronze_config → bronze_ingestion                                     (2 tasks)
Wave 2   Silver parents, parallel: silver_leads, silver_activities,
         silver_crm_users, silver_custom_activities, silver_calendly,
         silver_sentiment, silver_mdl_users                                  (7)
   +     silver_custom_activities_fields (needs only its parent) and
          gold_calendly_metric (needs only silver_calendly) start as soon
          as those parents land, alongside the rest of silver          (2 tasks)
Wave 3   Silver children (each waits for ALL wave-2 parents): silver_leads_custom,
         silver_contacts, silver_email_body, silver_activities_attendees,
         silver_activities_envelope, silver_activities_to                    (6)
Wave 4   silver_contacts_emails (grandchild — waits for the children)         (1)
Wave 5   silver_user_mapping (2_8 spine — the LAST silver task)               (1)
Wave 6   Gold set 1 — rooted at the spine (strict barrier: nothing in gold
          but the Calendly metric runs before ALL silver is complete):
          gold_staff_emails, gold_days_since_last_email,
          gold_days_since_last_meeting, gold_days_until_next_meeting,
          gold_days_since_last_login                                        (5)
Wave 7   gold_lead_dim — waits for gold_staff_emails (its email attribution
          subtracts the staff blocklist)                                     (1)
Wave 8   gold_staff_dim (needs staff_emails + lead_dim; 3_01 builds staff_dim
          AND staff_account_dim from both), gold_missed_calls +
          gold_sentiment (cover lead_dim's all-leads spine)                (3)
Wave 9   Gold set 2 (wait for ALL of set 1 + staff_dim + lead_dim):
          gold_leads_activity_fact, gold_sales_fact                         (2)
Wave 10  gold_health_metrics (waits for both facts + lead_dim)               (1)
Wave 11  gold_health_profile + gold_communication_state (wait for metrics)   (2)
Wave 12  gold_health_history + gold_communication_recency
          (wait for profile + state)                                         (2)
Wave 13  gold_coaching_score (waits for history + recency) — the final
          task; its last cell re-applies the change-data contracts (§6.6)      (1)
```

The gold waves are intentionally sequenced around attribute completeness: `lead_dim`'s email attribution subtracts the whole-person staff blocklist, so it waits for `staff_emails`; `missed_calls_14_days` and `sentiment` cover the all-leads spine, so they wait for `lead_dim`; `leads_activity_fact` and `sales_fact` enrich staff names, so they wait for `staff_dim`. Everything else in gold set 1 reads only silver, so it starts the moment the spine completes.

**Run economics** (measured): a normal nightly run costs ~3.5–4.5 DBUs; a full silver rebuild day is the expensive outlier (~15 DBUs, ~$5). Duration varies ±25% on serverless due to compute allocation while DBUs stay stable — monitor cost per run, not wall time.

---

## 9. Full Table Reference

Catalog: `crm_ingestion`. Schemas: `config` (1 table), `bronze` (10), `silver` (21), `gold` (20) — 52 tables, 45 of them produced by the nightly DAG. The rest are the source-audit log (`bronze.source_inventory`), engine auxiliaries (`silver._key_join_map`, the activities parent's on-demand custom-field extract, the custom-activities permission children), and `gold.lead_emails`, retired since its email attribution moved into `lead_dim` (it still stands but is no longer refreshed).

### Config
| Table | Purpose |
|---|---|
| `config.bronze_config` | Bronze ingestion control: source, target, watermark column, load type, active flag |

### Bronze (10) — sources and row counts in §4
`leads_raw`, `lead_activites_raw`, `close_crm_users_raw`, `custom_activites_raw`, `all_payments`, `calendly_scheduled_events`, `student_sentiment`, `mdl_users_raw`, `lead_merges`, `source_inventory`

### Silver (21) — row counts and schemas in §5 (16 in the DAG)
`leads_processed`, `leads_processed_custom`, `leads_processed_contacts`, `leads_processed_contacts_emails`, `lead_activites_processed`, `lead_activites_email_body`, `lead_activites_processed_attendees`, `lead_activites_processed_envelope`, `lead_activites_processed_to`, `close_crm_users_processed`, `custom_activites_processed`, `custom_activites_processed_fields`, `calendly_scheduled_events_processed`, `student_sentiment_processed`, `mdl_users_processed`, `user_identity_map` + the auxiliaries above

### Gold (20, 19 in the DAG)
| Table | Type | Rows |
|---|---|---:|
| `staff_dim` | dimension | 444 |
| `staff_account_dim` | dimension (staff person) | 189 |
| `lead_dim` | dimension (all leads) | 66,936 |
| `staff_emails` | identity view | 189 |
| `lead_emails` | identity view — RETIRED, no longer refreshed | 66,571 |
| `leads_activity_fact` | fact (event grain) | 1,060,843 |
| `sales_fact` | fact (latest sale per lead) | 246 |
| `days_since_last_email` | metric | 6,932 |
| `days_since_last_meeting` | metric | 7,830 |
| `missed_calls_14_days` | metric (all leads) | 66,936 |
| `days_until_next_meeting` | metric | 392 |
| `days_since_last_login` | metric | 630 |
| `days_since_last_meeting_calendly` | metric (all leads with bookings) | 51,039 |
| `sentiment` | metric (all leads) | 66,936 |
| `health_metrics` | staging join | 1,384 |
| `customer_health_profile` | SCD2 fact | 7,291 (1,384 open) |
| `customer_health_history` | daily snapshot | rolling (10,963) |
| `communication_state` | context (all leads) | 51,298 |
| `communication_recency` | context (clients) | 1,384 |
| `coaching_score_by_client_health` | scorecard | 1,384 |

---

## 10. Operating Notes

* **Health is per-source `lead_id`** — project spec prohibits lead-ID consolidation. The identity spine maps people to their lead IDs; it never merges them.
* **Masked identifiers** — the CRM export masks emails and names as `user_…` hashes, and each source system hashes differently. Cross-system joins go through the identity spine; never join across systems on a raw hash or masked email.
* **The CRM never records future meetings** — all future-dated meeting evidence lives in the Calendly export. `days_until_next_meeting` is Calendly-only, matched through the spine.
* **`student_sentiment` cannot join to leads directly** — its `CHANNEL_ID` is a different identifier type than any CRM key. Sentiment reaches gold via the CRM's pre-calculated custom fields.
* **Clock metrics vs. state metrics** — `days_since_*` values refresh daily on unchanged SCD2 versions (they are excluded from the change hash); engagement flags, patterns, bands, and scores change only when the underlying state changes.
* **NO DATA is a real band, not an error** — a client with no engagement evidence on any channel scores NO DATA (464 of 1,384 currently). The `identity_unlinked` and `no_client_email` flags in `health_metrics` mark the leads that need human review.
* **Cost** — a normal nightly run is ~$1.30 at list rate (~$0.65 at the promo rate). Full silver rebuilds are the rare, deliberate outlier (~$5).
* **Constraints are self-healing** — the four CTAS-rebuilt gold tables drop their PRIMARY KEYs on every rebuild, and the contracts cell at the end of `3_18` (the final task of the nightly run) re-applies them the same morning (§6.6). Only a manual rebuild outside the job needs the cell re-run by hand.
