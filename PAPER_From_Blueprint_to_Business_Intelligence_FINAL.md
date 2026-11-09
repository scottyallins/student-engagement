# From Blueprint to Business Intelligence
## A Customer Health and Communication Intelligence Platform on Databricks

### From blueprints to data

Before transitioning into data engineering, I spent nearly three decades in construction, progressing from carpenter to foreman, managing multimillion-dollar projects from blueprint to delivery. The work was never just concrete and schedules. It was understanding the objective, designing the system, coordinating crews, controlling quality, and delivering something the client could trust.

Data engineering turned out to be the same job with different tools. The concrete became Delta tables, the crews became pipelines, the blueprint became a data model. The fundamentals never changed: understand the objective, design the system, validate the output, deliver something people can trust.

This project became my transition into data engineering — my first production-scale platform built on real business data, and the one I would defend line by line.

---

### The question behind the request

The company runs a coaching program: clients purchase a plan, are assigned a coach, and the relationship is the product. Engagement signals lived across five disconnected systems — the CRM, meetings, email, platform logins, and communication history — and none of them spoke to each other. The question the business actually needed answered was simple: *is this client still with us, and if not, who noticed?* Most days, nobody had.

Before writing a single line of code, I interrogate the request. What decision changes because this pipeline exists? Who is waiting on the other side of it, and what will they do with the output? If it disappeared tomorrow, what would silently break? And the question that disciplines everything else: what does *done* look like in one sentence?

For this project, the answer was: **every morning at 8:00 AM Eastern, before coaches open their laptops, they know which clients need attention today — and why.**

That sentence shaped every decision that followed. A pipeline that only moves data is plumbing. A pipeline that moves *attention* is an asset — and attention is only worth moving if it arrives before the cost of inaction lands on the customer.

---

### The first hard problem: whose email is this?

Before any scoring could happen, the pipeline had to separate staff activity from client activity — otherwise coaches would open their customer views and find their own emails at the top of them.

The source systems mask every identity as opaque `user_…` hashes, and every system hashes differently, so no two systems can be joined directly. I built an identity spine: one row per *person* across all systems, carrying every identity hash, every email, and every lead attribution — 131,290 persons. On top of it, a staff split: 189 staff persons and 443 CRM directory identities identified and subtracted from the client side. The result is validated, not assumed — of 103,653 distinct customer email addresses, zero overlap with any staff identity or directory address. Coaches see only customers.

---

### What runs every morning

Thirty-six orchestrated tasks execute in twelve dependency waves on Databricks Serverless, finishing in 20–26 minutes for roughly **$1.29 a run** — about $39 a month, $471 a year at list rate. A deliberate full silver rebuild, the expensive outlier, costs about $5.28.

What lands is one current profile per client — 1,374 active coaching clients today — with an engagement score, a band, and the reasons behind it. A coach can look up a client by email and see the last login, the last email, the last meeting, missed calls, and the next meeting on the books. And beside the client's grade sits a second grade the business didn't know it needed: the company's own service record.

---

### The numbers, reconciled

Any pipeline paper can quote numbers. Mine had to survive the question: *do these numbers agree with each other?* They do — because they measure different things, and the paper says so.

* **19.8 million rows in bronze.** The sources re-dump their full tables on every export, and bronze deliberately keeps every dump. It is the immutable audit trail — the layer I compare everything else against.
* **52.8 million rows in silver before the rebuild.** Processing every re-dump as a new version multiplied the duplicates: 12.7M activity rows, 22.1M CRM-user rows, 15.3M Calendly rows.
* **8.2 million rows in the same seven silver parents after the rebuild — an 84% reduction.** Underneath, only 254K distinct activity payloads and, remarkably, just **641 distinct CRM-user payloads ever existed** in 22.1M rows.
* The distinct truth underneath all of it: **55,279 leads**, of which 1,374 are active coaching clients.

Runtime deserves the same honesty. The job finishes in 20–26 minutes of wall clock, but the sum of task runtimes is 3,388 seconds (~56 minutes) and actual SQL execution is 1,828 seconds. All three are true: tasks run in parallel lanes — up to nine at once — so aggregate task time is a *sum across lanes*, not a schedule; and roughly half of each task's reported time is serverless environment overhead, not SQL. The operational lesson I verified across a week of runs: wall-clock duration is noisy (±25%, shifting between task cohorts as compute gets allocated), but DBU consumption is stable at ~3.7 per run. **On serverless, monitor cost per run, not minutes.**

---

### Bronze, Silver, Gold — and the detachment principle

The platform follows the medallion pattern. Bronze is the immutable landing zone: nine raw Delta tables, append-only, watermark-tracked, no transformations — source output preserved exactly as received. Silver repairs, normalizes, deduplicates, and produces typed current-state tables plus version history. Gold is the star schema the business uses.

The structural decision I defend hardest is **detachment**: every table is built by its own notebook, and the dependency graph mirrors actual data lineage — the job definition is nearly a translation of the data flow diagram. When one table fails, exactly one thing needs fixing, and everything else keeps running. That same graph enforces **strict barriers**: a silver failure halts the day's score rather than publishing one built on stale sources. A wrong score costs more than a late one, because in a coaching business the score is the product.

During development I managed the job in the UI, because clicking was faster than editing code while the design was still moving. Once it stabilized, I moved the whole job into a Declarative Automation Bundle: defined as code, version-controlled alongside the notebooks, deployed through a validated pipeline. The UI is now locked. Fast where it's cheap to be wrong, formal once it's expensive.

*Figure 1. Student Engagement Pipeline — Medallion Architecture (Source → Bronze → Silver → Gold → Consumption).*

---

### The engine room

The real workhorse is a shared utilities notebook. Every silver parent calls one function, `process_table`, which owns JSON repair, schema discovery, watermarking, hashing, validation, and writing. One engine, nine sources — adding a new source is configuration, not engineering.

**The JSON repair ladder.** The sources ship malformed, single-quoted, wrapper-nested JSON with human free text inside it. The hardest problem was never parsing — it was parsing without *destroying what customers wrote*. The engine escalates only when a step fails: direct parse; then type-based wrapper detection (stringified inner JSON, single-key array wrappers, recursive — no hardcoded key names, so it works on any source); then quote normalization that protects real apostrophes — `it's`, `don't`, `O'Brien` — before it flips anything; then reversible tokenization of designated free-text fields: the apostrophe in `girl's` becomes a token, the parse happens, the original character is restored. If all of that fails, the row lands as fail-safe NULL and the run logs the parse percentage and per-field health.

Some repairs are deliberately *rejected*. An SMS body that literally reads `"None"` stays `"None"` — it's quoted text, not a null literal. And the engine refuses to overwrite a silver table if the parsed output resolves entirely to NULL: a bad schema must not silently replace good data. That guard is the difference between a pipeline that fails loudly and one that fails invisibly.

**Schema discovery by sampling.** Discovery reads a sample, not 50 million rows — but sampling has a coverage problem I hit and fixed. The original first-20-records sample was dominated by Calls (35% of volume), so rare types like Meetings (3.4%) and their fields never appeared. The fix is type-aware sampling: group records by type, take a few of each, fall back progressively. Even so, sampling found 76 of 135 email-specific fields. I quantified the gap rather than scanning millions of rows, and built targeted extraction for the 59 the sample missed — the coverage number is a measured fact, not a hope.

**Knowing when not to build.** Early on I exploded every array into child tables and got seventy-plus tables, most of them useless. The engine now has a dry-run lineage mode: print the full possible tree, materialize only what the health profile needs. Small enough to be right, big enough to work.

---

### Measure, then design: the CDC rebuild

I did not choose an ingestion strategy on faith. I measured what the sources actually sent: of 19.07 million records, roughly 866,000 — 4.5% — carried new information. 95.5% were byte-identical re-dumps. That measurement, not fashion, justified hash-based CDC.

The design uses **two hashes, because they answer two different questions**. The silver *payload hash* (MD5 over the row's business columns) answers *did the source record change?* — it is a change-detection key, not a business key; even the theoretical collision costs one missed version row, and source-to-target reconciliation is the backstop. The gold *state hash* answers *did the client's engagement state change?* — that one drives history, and I'll come back to it.

Incrementality is watermark-driven at both layers. Bronze loads only rows past a per-table `insert_date` watermark; a failed load re-pulls exactly the missed rows, no duplicates. Silver processes only rows past a `bronze_updated_at` watermark — delete the affected versions at microsecond precision, append the changed payloads; child tables inherit the watermark. Deletes need no tombstones: the CRM re-dumps full tables, so *absence from the latest dump is the delete signal*, version history preserves what was, and `lead_merges` is ingested to track identity merges. Deduplication rates varied from 35.6% on leads to 99.3% on CRM users and Calendly — which is exactly why I measured each source instead of assuming they behaved alike.

The result: the platform scales with *change volume*, not historical volume. A quiet day costs seconds.

---

### Green is not correct: the incidents

The most valuable thing this project taught me is that **a pipeline can be green every morning and still be wrong.** Alerts fire on crashes. They do not fire on plausible-looking wrongness. I audit output against source-of-truth as a habit, and that habit caught three failures that monitoring never would have.

**The silent daily truncate.** A parsing regression began nulling free-text fields on *every* row, not just repaired ones. Downstream, a filter on that field returned zero rows — and the old write path, given zero rows, overwrote the entire child table with nothing. Every night, silently, for days. The dashboards still looked reasonable. The detection was manual: comparing Delta version history against the last good version — 1.8M rows, then a cliff to zero. The fix was one flag (null only repaired rows), the guards became structural (skip the write on a null frame, a missing column, or zero parsed rows), and the recovery reloaded 1.94M rows with content verified against source. That incident is why the all-NULL write refusal exists.

**The truncated history nobody noticed.** Silver `leads` sat at 10,069 rows for weeks — a table silently truncated in an earlier era — while the real version history held 339K rows. Twenty-nine active clients and **$185,174 in contract value** were invisible to scoring. A before/after reconciliation during the rebuild caught it; the rebuild restored the full history and surfaced 24 hidden active clients.

**The rank/filter ordering bug.** A filter written for a one-row-per-lead world resurrected expired clients under versioning. The correct semantics: rank versions first, *then* filter. The active population went from 1,636 to 1,371 — the original number had overstated the client base by 16%, and it would have shipped to the business.

Three different mechanisms, one lesson: **monitoring detects failures; validation detects wrong results.** The pipeline now runs source-to-target reconciliation, count validation, CRM cross-validation, population validation, and dedup validation as standing checks — and the strict dependency barriers mean a broken source blocks the score instead of quietly aging it.

---

### What the business sees

Gold is a proper star schema: staff dimensions (the CRM directory dimension plus a person-grain staff account view), the lead dimension, an activity fact at event grain, a sales fact with setter and closer, seven single-metric tables, and the wide `health_metrics` staging join that feeds the SCD2 health profile, its 90-day rolling snapshot history, two communication tables, and the coaching score.

The headline deliverable is the **Customer Health Profile**. Each client is banded on a 0–100 engagement score:

| Band | Rule | Clients (current) |
|---|---|---:|
| GOOD | score ≥ 75 | 205 |
| AVERAGE | 50–74 | 289 |
| POOR | < 50 | 417 |
| NO DATA | no engagement evidence on any channel | 463 |

**NO DATA is a real band, not an error** — and it is arguably the most commercially interesting one. Inside those 463 clients the split is clean: 349 the company is actively emailing and calling who have never once replied, and 114 with no company contact ever recorded. The first group is the pipeline's sharpest retention signal — and it was invisible before the systems were joined.

The commercial context is documented with the same honesty. Contract value is on file for 719 of 1,374 clients; contract start dates cover 1,329 of 1,374 (the CRM's stored custom field first, the qualifying sale date as fallback — the 45 with neither are a source-side gap, not a pipeline one, and the paper treats them as such). Qualifying New Sale activities exist for only 43 clients. These limits live in the table documentation where any analyst will trip over them — honest gaps, stated.

---

### Three histories, three questions

The platform keeps three separate history mechanisms because each answers a different business question:

| Layer | Question | Mechanism |
|---|---|---|
| Bronze→Silver CDC | Did the source data change? | payload hash + watermarks |
| Health Profile (SCD2) | When did the client's engagement change? | state hash, versioned ledger |
| Health History | What did engagement look like on a given day? | rolling 90-day daily snapshots |

The SCD2 state hash is built from identity, contract value, sentiment, engagement flags, pattern, and scores — and **deliberately excludes clock metrics** like days-since-last-email. Include those and the ledger mints a new version every single day, turning business history into noise. The way I say it in the docs: *CDC tracks technical change. SCD2 tracks business-state change. Snapshots track daily observations. Or more simply — SCD1 keeps the present. SCD2 keeps the story. Snapshots keep the timeline.*

---

### What the data asked back

The original request was a health score. Once the data became trustworthy, it started asking questions back — and three models were born that nobody requested.

**Communication state: whose turn is it?** Activity dashboards count interactions; they don't distinguish contact from attempt. A client emails; the coach calls and hits voicemail; the dashboard shows two interactions — but the client's message still has no answer. This model separates *contact made* from *attempt only* and reports whose move it is. Responsibility becomes visible.

**Communication recency: how long has that responsibility existed?** Time context across email, phone, SMS, Slack, and meetings: reply debt, outreach exhaustion, last successful contact, last attempt.

**The coaching score: what grades the company?** The health score grades the client. This table inverts the gaze — it measures whether the company kept the promises implied by a premium coaching contract. The verdict is a promise ladder:

| Verdict | Meaning |
|---|---|
| PROMISE KEPT | no reply is owed — our move was made |
| WITHIN THE PROMISE (0–2 days) | the last acceptable day |
| PROMISE SLIPPING (3–7 days) | past the two-day promise |
| PROMISE BROKEN (8–30 days) | the client is waiting on a broken promise |
| PROMISE ABANDONED (31+ days) | a month or more, and nobody tried |

On the current run: 1,257 clients KEPT, 58 within promise, 29 slipping, 15 broken, 12 abandoned — on any given morning, **27 clients are waiting on a broken or abandoned promise**. A client can be GOOD while the company is PROMISE BROKEN; a POOR client can sit beside a perfectly kept promise. The two scores stay deliberately independent, so the business can tell a disengaged client from a service failure.

The framing lives in the data itself. Every column carries a comment describing how the *client* experiences that state — *"the client does not know we are busy; they only know we are silent."* The honest voice survives inside the data dictionary, where nobody can call it editorial.

---

### Judgment: what I chose not to build

Half of engineering maturity is declining work. I ran the performance diagnosis on the production run: the biggest finding was a 235-second statement processing ~105K Calendly records — roughly $61 a year if fixed. I declined. It implements recency semantics I had corrected days earlier; the risk-to-savings ratio isn't close. And when the CDC rebuild changed the data layer, I *re-ran the diagnosis instead of trusting it* — the original findings no longer existed. A diagnosis has a shelf life; re-measure after major change.

I prototyped an ML-routed repair system and found the models were mostly placeholders while the deterministic engine actually worked — for a daily 1,400-client job, ML was cost and fragility with no payoff. I designed a cumulative schema catalog that would remember every field it ever saw and alert on new ones, and chose not to build it: the sampling gap has zero business cost today, and the better version is extra code and another moving part for a problem that isn't hurting us. Knowing how to build something and knowing when it's worth building are different skills.

---

### Governance and trust

Late in the project I ran a workspace-wide reference audit, dropped 11 orphaned legacy tables, and ended with exactly **45 tables — a 1:1 map between the catalog and the version-controlled code that produces every one of them.** If a table exists, a notebook in the repo builds it. Nothing else.

On privacy: the CRM exports emails and names pre-masked as per-system hashes — tokenization happens source-side, so the pipeline never sees raw PII, and cross-system joins go through the identity spine by hash, never by a raw email or hash across systems. I'll state the gaps honestly: bronze retains every dump indefinitely, there is no retention policy yet, and a GDPR/CCPA right-to-erasure would need an explicit tombstone path. Those are roadmap items, and I'd rather name them than imply they're solved.

---

### Limits, and the next questions

This score measures engagement. It tells you who has gone quiet so someone can reach out. It does not tell you whether the client is making progress or receiving value — a client struggling until the next group session, a language barrier, unclear next steps. Those signals need inputs these systems don't carry yet, and the honest expansion path is known: short post-session questionnaires, client-canceled versus coach-canceled meetings (the cancellation data already exists in Calendly), tenure cohorts — the version history already exposes the contract-ladder shape, 63% of dated clients on 12-month base contracts with 3-month extensions layered on top. Per-program views need one new source: Moodle's course-enrollment tables. The engine was built for exactly this. New tables are additions, not surgery.

---

### Close

I approach data engineering the way I approached construction: understand the objective, respect the constraints, build the system, validate the result, and improve it until the people relying on it trust it. This pipeline evolved through the same cycle every mature system does — *measure, design, validate, audit* — and the auditing never stops, because green is not the same as correct.

What began as a customer-health scoring request became a decision-support platform: customer engagement, communication ownership, service quality, operational accountability, and the histories to prove all of it. What I shipped is not a pipeline that never breaks. It is a pipeline that *can't break silently* — version history, hash deduplication, reconciliation, and strict barriers each surface a class of silent failure. I don't claim to have found every class. The design makes finding them structural rather than lucky.

Because ultimately the code is only the mechanism. The value is someone making a better decision at the right time — before the customer feels the cost of inaction.
