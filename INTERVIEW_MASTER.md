# INTERVIEW MASTER DOCUMENT — FINAL
**Customer Engagement Pipeline · Databricks Medallion Architecture · How the Errors Built the Engine**

*Every current number in this document was re-verified against the live warehouse, the run log, and the billing system on Nov 19, 2026. Numbers marked "measured at the time" are incident records — point-in-time measurements pinned in code comments and dev logs that can't be re-measured from today's state.*

| Active clients | NO DATA | Runtime | Cost per run | Tasks | Regression pins | Production incidents |
| --- | --- | --- | --- | --- | --- | --- |
| 1,390 | 33.2% | 21–34 min | ~$0.80 | 37 | 32 | 13 |

**The one-line version:**

"I didn't design a robust pipeline — I earned one. Thirteen production incidents, each one understood, fixed, and pinned as a permanent regression test, until the engine fails loud instead of wrong."

---

## PART 1 — THE 60-SECOND PITCH

I build medallion pipelines for dirty nested JSON in Databricks and PySpark. Bronze lands the raw string append-only with a date watermark and an MD5 on the payload for dedup — nine raw sources, 22 million raw rows, and because the extractor re-dumps current state instead of sending changes, only 1.13 million distinct payloads. Ninety-five percent of bronze is the same bytes re-arriving; I measure that, I don't fight it. Silver tokenizes problem keys — leading digits, embedded quotes, symbols, reserved words — before parse, unwraps after, then runs a full schema walk called path inventory that enumerates every array and struct path with coverage and max size. A promotion function decides which paths become child tables using depth and row-multiplication gates, so the explode never runs unbounded. A source-aware mapping table links child columns to a parent PK role even when names don't match word for word. Everything not promoted goes to a custom-fields pivot. Gold builds a star schema on top — dims, facts, seven narrow metric tables — with its own business-level MD5 separate from the raw one. The result is one profile per client every morning at 8 AM Eastern, telling coaches exactly who to call. A third of clients score NO DATA because there isn't enough signal to judge — and that's deliberate, not a gap. The pipeline refuses to manufacture a score.

## PART 2 — THE HOOK

I built a customer health and communication intelligence platform in Databricks for a coaching company. Clients buy a plan, get assigned a coach, and the problem was nobody could see in one place whether a client was still engaged or had gone quiet. The data sat across five disconnected systems — CRM, meetings, email, platform usage, communication history — arriving as malformed nested JSON with single quotes.

What I built is a daily pipeline that turns all of that into one current profile per client. Every morning at eight AM Eastern, before the coaching day starts, coaches get a calculated engagement score banded GOOD, AVERAGE, POOR, or NO DATA, with the reasons behind it. The latest run covers 1,390 active coaching clients — and that number reconciles exactly against the CRM: 1,390 leads in Coaching Client status, 1,390 current profiles. Zero drift between the warehouse and the source of truth. Roughly a third — 33.2%, 462 clients — score NO DATA because there isn't enough activity to judge. That's deliberate. The pipeline refuses to manufacture a score.

The whole job runs on Databricks serverless: 37 tasks, 21 to 34 minutes, about $0.80 a night. I know the price because I verified it in the billing system, not from a dashboard estimate — 2.2 to 2.3 DBUs per clean nightly run at $0.35 per DBU on jobs-serverless in US East Ohio.

One of the first things I had to get right was separating staff activity from client activity, so coaches weren't looking at their own emails. I built a staff table from three signals only staff can produce — a 48-character organizer login hash, the company domain, and registration in the CRM staff list — 190 staff people, then subtracted their entire email footprint from the client email arrays. That separation also catches former clients who now work at the company: 75 staff identities overlap with lead records, and exactly 3 of those carry current Coaching Client status. Those three stay flagged but excluded from the active client book — and I verified the exclusion holds: the overlap between the active client book and the staff email blocklist is zero.

## PART 3 — ARCHITECTURE & ENGINEERING

The gap between the data and the goal was engineering a way to tie different activities back to the same person. The architecture is medallion. Bronze is nine append-only Delta tables landing raw strings with an insert-date watermark — no business logic, just preserve exactly what the source sent. Silver is where the real work lives: 25 tables, 3.06 million rows total. Every silver notebook calls one shared function, `process_table`, from my utils. That function checks the watermark, runs the JSON repair, discovers schema, dedupes on a payload MD5 hash, and writes one current table per source.

The repair engine went through more than thirty iterations. It tokenizes problem keys before parse, unwraps after. Free-text fields like notes and email subjects get apostrophes converted to reversible tokens so the JSON parses without destroying what the customer actually wrote.

The CDC measurement is what justified the design. As of today: 22,034,959 raw rows across the nine sources, 1,132,665 distinct payloads — 94.9% are re-dumps. When I first measured this at the November 9 census it was 19 million raw rows, 866,437 unique payloads, 95.5% re-dumps; the ratios hold as bronze grows. The rebuild collapsed pre-rebuild silver bloat — 52.8 million rows — down to 8.2 million at the time, and after event-time survivorship and canonical dedup landed, silver today sits at 3.06 million rows. Same data, one current version per entity, full history preserved in bronze.

Gold is a star schema: leads activity fact at 1,115,773 rows at event grain, a 69,974-row lead dim, a 447-row staff dim, a sales fact with setter and closer (255 rows), seven narrow metric tables — days since last email, last meeting, missed calls in 14 days, days until next meeting, days since last login, Calendly recency, sentiment — and health metrics joining all seven per lead. Communication state holds 54,950 rows. Customer health profile bands the result: a 9,472-version SCD2 ledger today for 1,390 current clients.

There are three separate history mechanisms, and keeping them separate is the point. The CDC hash answers "did the source record change." The profile itself is an SCD2 engagement ledger — a second, separate MD5 hash built from business-state attributes like contract value, sentiment, engagement flags, pattern, scores, and health band. Clock metrics like days-since-login are deliberately excluded from that hash, or you'd get a new version every single day; a new version opens only when engagement state actually changes. Then customer health history is a rolling ninety-day daily snapshot — 15,113 rows — of the active SCD2 versions. CDC tracks technical change, SCD2 tracks business-state change, snapshots track daily observations.

The lesson that changed how I work: I used to build upstream and hope it matched downstream. Now I start from the final metric's required columns and trace backward before touching silver. That's how I caught a lead name coming from a username hash in one table instead of first-last from another. Since then I put contract notes on the silver tables themselves — grain, key columns, what downstream they feed.

## PART 4 — DISCOVERIES & JUDGMENT

After the health score was working, three things showed up in the data that weren't part of the original ask. Communication State answers whose turn it is. It separates contact made from attempt only, because an unanswered outbound call doesn't actually satisfy a communication obligation. Communication Recency adds the time context on top of that. And Coaching Score flips the lens entirely — the health score grades the client, the coaching score asks what evaluates the company: reply debt, response delays, service inactivity, meeting delivery. A client can be GOOD health while the company is sitting in Promise Broken. Those three tables stay outside the health score on purpose, so neither metric can launder the other.

Real client names were the last masked casualty, and that extraction is one of my favorite problems on the project. Every structured name field in the source is PII-masked upstream — leads, contacts, Moodle names, even the Calendly invitee name. The staff directory has real names, but only for staff. So I pulled names out of free text, in a deliberate priority ladder: the "Invitee:" block inside Calendly confirmation emails first (a meeting confirmation is more authoritative than a first-name text), then first names from outbound SMS greetings — outbound only, because inbound greetings address the staff member — then email greeting lines, hash as the last resort. The first Calendly rung alone covered about 840 clients (measured at the time), and by the last count in my earlier notes the ladder had grown that to 1,165 of 1,384 — 84%. Today, with the ladder mature: 1,338 of 1,390 clients — 96.3% — have real names. Only 52 are still hashes.

**The reverse lookup — the decision I'm most glad I tested before building.** The obvious next idea was to run the same extraction on inbound messages. I tested it against the real 52 instead of guessing. Those 52 clients have almost no inbound traffic at all — three incoming emails, two inbound calls, and zero inbound SMS across all 52, total. Running signature and self-identification patterns over their inbound email fragments matched exactly one lead. The samples show why: inbound greetings address the staff member — "Hi Osaki, just checking in… Regards" — and some inbound bodies are themselves PII-masked with user-hash placeholders. So the reversed process would mostly harvest staff names and masked placeholders for a maximum gain of one name. I declined to build it. The 52 aren't clients with untapped free text; they're clients who never communicate. The ladder already captured everyone who ever talked. Knowing how to build something and knowing when it's worth building are two different skills — and this time I have the numbers to prove which one I used.

On the bugs that actually mattered: the email-body child originally kept only the first fragment of the quoted body. Of more than 20,000 activities that contained the Calendly scheduling phrase, 102 had the phrase in a later fragment (measured at the time). That produced wrong recency on 21 leads, including five paying clients — and the pipeline stayed green the whole time. I changed the grain to one row per fragment, backfilled about 22,500 missing rows, and moved both the email-exclusion and meeting-inclusion predicates to the parent's full cast of the quoted body. Phrase coverage went from 102 gaps to zero.

The repair engine itself had a rare bug — my own code. On two activity elements out of millions (measured at the time), the UDF mangled the element into an empty array. A chunk containing even one empty-array element makes from_json return null for the entire chunk, so one mangled element silently nulled a 93-row chunk. Two null rows in activities, 88 in users, five activities lost entirely. The tripwire that caught it was the Delta contracts cell I wrote — it refused to apply a primary key while null IDs existed. I reproduced the mangling deterministically, fixed the engine to filter non-dict elements before the explode with logged counts, and the five lost activities are queued for backfill. Even the code you build to catch corrupted data can itself corrupt data. So the validation layer doesn't trust anything, including the repair engine.

On judgment: I ran a performance diagnosis on a full clean run. Zero compute-level findings. The query notes totaled about thirty seconds of a thirty-minute job — Photon declining wide 40-to-50-field structs, a few unaccelerated discovery aggregations, telemetry-table write contention resolving in under 100 milliseconds. I declined to fix them. Optimization is waste times frequency versus effort times risk, and my frequency is one run a day. I prototyped an ML-routed repair system and found the models were placeholders while the deterministic engine actually worked — for a daily job of this size, ML was cost and fragility with no payoff. I designed a cumulative schema catalog that would store every field ever seen, and chose not to build it at the time because sampling wasn't dropping fields the business uses. That design later became the schema registry the day drift made the cost real — which is the right order: design ahead, build when the pain arrives.

Limits — I say this out loud in interviews: this score measures engagement. It tells you who went quiet so someone can reach out. It does not tell you whether the client is making progress or getting value. Success metrics need inputs that aren't in these systems yet.

What I shipped isn't a pipeline that never breaks. It's one that can't break silently.

## PART 5 — HOW THE ERRORS BUILT THE ENGINE (13 STORIES)

The theme: my utils never got better because I planned it — it got better because something broke in production, the failure was understood, and the fix was made permanent. Every guard in the engine is a receipt for a real incident, and 32 of them are pinned as regression checks that re-run on every `%run`.

**Story 1 — The quote wars (July).** Hand-parsing Close CRM JSON with regex, then `ast.literal_eval`, then from_json/schema_of_json. The lesson that shaped everything downstream: stop fighting syntax with patterns; understand the structure.

**Story 2 — The 3:29 AM rescue (Oct 4).** A UDF data-array unwrap bug had silently swallowed millions of rows. Fixing it rescued 4.16M rows in one table (measured at the time). Lesson: silent data loss is the worst failure class — you don't even know to look.

**Story 3 — 46 notebooks became one engine (Oct 2–7).** Per-child notebooks and hardcoded lineage trees collapsed into one generic engine with auto-discovery. Lesson: schema-specific code doesn't survive schema drift.

**Story 4 — Wrappers have shapes, not names (Oct 28).** Hardcoded "JSON_OBJECT"/"data" key checks replaced by type-based detection, validated on all bronze tables. Lesson: infer the contract from data, don't encode vendor quirks.

**Story 5 — Python in my JSON (Nov 6).** Payloads carried bare None/True/False. A character-level, quote-aware scanner converts them only in value positions — 11/11 unit cases, 100% real-bronze parse (measured at the time). Lesson: know what's a value and what's a delimiter.

**Story 6 — The over-prepared ladder (Nov 9).** Rungs 4 and 5, plus smart-quote and control-char handling — each added only after an A/B harness proved zero changed, zero broken. The harness caught my own bugs twice: a lookahead typo that made a fix a silent no-op, and a regex that produced unquoted keys. The census proved it: 946,696 payload versions parsing with zero NULLs (measured at the time). Lesson: the test harness guards the author, not just the code.

**Story 7 — Measure before rebuilding (Nov 9).** The silver census found parent bloat — users ×375, 72.9% orphaned children (measured at the time). Contract decision: latest-version-per-id, full-history rebuild from bronze as ground truth. Lesson: establish ground truth before touching silver.

**Story 8 — The MERGE that died on nested structs (Nov 11).** Batch discovery drifted on nested types and the MERGE crashed. Fix: recursive type reconciliation (`_cast_tree` / `_reconcile_to_target`), validated byte-identical on a clone of the exact failure. Lesson: reconcile types explicitly; never hope a cast works.

**Story 9 — The contracts that never were (Nov 13).** A session narrated 20 PKs and 15 CHECKs applied — catalog verification found zero. Lesson: verify the artifact, not the narration.

**Story 10 — The hardening pass (Nov 17).** Pure incident memory: 2 chunks → 89 all-NULL ghost rows and 5 lost activities (measured at the time) → parse-null quarantine with replay, NULL-PK quarantine, the ghost-child, column-drop, and case-collision guards, "never drop staging on failure," and the corpus itself. Lesson: quarantine loudly; a null primary key never gets written — quarantine, don't guess.

**Story 11 — The regression the corpus was built for (Nov 18).** My own 11/17 free-text guard blanket-exempted JSON_OBJECT from unwrapping — but for two source families, JSON_OBJECT was the wrapper. Two-line fix, two new pins, corpus green, then a full 17-table rebuild via dry-run → verify → live. Lesson: hardening can regress too — the corpus is the immune system, and it worked exactly as designed.

**Story 12 — The health check that only failed when it was needed (Nov 18).** The fleet health task crashed the moment it had real findings — a dict-shape mismatch on a branch no clean run ever exercises. Lesson: monitoring needs a tested failure path too; a guard that never ran with a problem present is a guard you don't have.

**Story 13 — The union that double-counted a field (Nov 18–19).** The scheduled 8 AM run crashed on the first real data batch after the rebuild: `AMBIGUOUS_REFERENCE_TO_FIELDS — '_type' appears 2 times`. Root cause: the schema registry's union compared the registry's raw field names against the discovery's sanitized names — and my sanitizer strips leading underscores, so `_type` became `type`. A field whose raw name changes under sanitization was always judged "missing" and re-added as a duplicate. The registry itself was the evidence: `_type` held 8 rows against a baseline of 5, meaning earlier runs had been silently double-writing. I fixed the comparison to check both raw and sanitized names, added a corpus pin that injects a fake registry through a new override parameter — so the test never touches the real one — and repaired the run. Next morning's unattended nightly went green first try: 30 minutes, 20,566 new rows, zero parse nulls, zero quarantines, the union stable at 234 fields. Lesson: even the metadata engine needs the same fail-loud contract as the data engine — and the guard that crashed was the reason this was a 9 AM fix instead of a silent wrong answer.

## PART 6 — WHAT BREAKS NEXT: MY DRIFT FORECAST

Interviewers ask "what would you worry about?" Here's my actual list, in priority order.

1. **Wrapper-role drift.** A key that's prose in one family and the payload wrapper in another is the JSON_OBJECT class of bug. My corpus pins the shapes I've seen; the next vendor variant will be caught loud by the guards, but role classification is still heuristic. I'd be first suspicious here.
2. **Type drift inside the registry.** A field that arrives as int and later as string. Handled today: the union falls back to StringType on unparseable DDL, and merge-time reconciliation casts recursively. Pinned after Nov 11, but it's a door.
3. **Volume drift.** An extractor outage gives an empty pull; the recovery gives one mega-batch — think millions of rows crossing the watermark at once. The run log already computes volume anomalies from logged rows-in versus history — but nothing alerts on them yet. That's my one real gap.
4. **Quarantine drip.** Null-PK quarantine grows about one row a day (91 → 92 → 93 this week) — a known source-side drip, loud and quarantined, but the replay backlog needs a cadence so it never becomes a landfill.
5. **A tenth source table.** Onboarding is manual: source config, silver task, job wiring, lineage. No auto-onboarding — deliberate, but it's toil.
6. **PII-mask policy change upstream.** Masking is what makes staff separation and name extraction work. If the vendor changes what's masked, coverage shifts quietly — my tell would be the name-coverage number (96.3%) moving without a code change.
7. **Schema width growth.** The registry is grow-only by design: lead activities discovered 234 fields this week against a registry knowledge of 259. Photon already declines the wider structs — about thirty seconds of query notes per run. Bounded, but it's the one cost line that only goes up.
8. **Guard failure-path rot.** Story 12's lesson, generalized: any new monitoring branch must be exercised with a problem present, or it isn't a guard. I test failure paths now.

And what can make the job crash tomorrow? Honestly — any of the guards tripping, and that's the contract: the all-null safety stop, the duplicate-field ambiguity (now pinned), merge-time type mismatch (now reconciled), a quarantine write failure (rows stay, warning fires loud), the corpus assert at import. If this job crashes, it's telling you something true. What it can't do anymore is crash silently wrong — that's the entire engineering thesis of the last five months.

## PART 7 — WHAT I'D BUILD NEXT (AND WHAT I DECLINED)

**Wire the alerts.** The one real gap. The run log already computes volume anomalies and parse rates; alerts only fire on crashes today. One scheduled check over the run log — empty pulls, parse-rate drops, quarantine growth — closes the loop between "the engine knows" and "I know."

**Rung telemetry.** Log which repair rung rescued each row. It would have caught the element-mangling bug in a day instead of a Delta-contracts tripwire. Proposed, specced, not built — bounded effort, high diagnostic value.

**A registry diff line.** "Three new fields appeared overnight" in the morning run log. Cheap, and it turns silent schema growth into a visible event.

**A churn view.** The CRM already carries the state — Contract Expired, 248 leads today — and bronze keeps every payload version, so Coaching-Client-to-Expired transitions are reconstructable from history. State is available now; events are one build away. I'd add it the day the business asks for retention, not before.

**Declined, with evidence:** the ML-routed repair (models were placeholders around logging while the deterministic engine actually worked); the reverse inbound name extraction (tested against the real 52 missing clients: three emails, two calls, zero SMS among them, maximum one name recoverable — not worth the code); the cumulative schema catalog on first design (became the registry only when drift made the cost real — right design, right timing, in that order).

## PART 8 — RAPID-FIRE Q&A

**Why medallion instead of one big script?** Detachment. One table per task means any failure is isolated and repairable. At 8 AM with a failed run, I rerun one task and its dependents, not the world. Last month's union crash: the other eight sources landed green while activities was down; a repair re-ran exactly the failed half.

**What's your watermark key?** Bronze watermarks on the source insert-date. Silver on a load-stamped updated-at. Only the delta crosses each layer.

**What was your hardest bug?** The email-body fragment bug — the pipeline stayed green while 102 activities lost the scheduling phrase in later fragments, wrong recency on 21 leads including 5 paying clients (all measured at the time). Or the repair UDF mangling two elements into empty arrays — my own code, caught by my own constraint guard.

**Where do real names come from if every name field is masked?** Free text, in a priority ladder: Calendly invitee blocks, outbound SMS greetings — outbound only, since inbound greets the staff member — then email greetings, hash as last resort. 1,338 of 1,390 clients, 96.3%, have real names today.

**Did you consider inbound messages too?** I tested it rather than guessed: the 52 still-hashed clients have three incoming emails, two inbound calls, and zero inbound SMS between them. Inbound greetings address staff; some inbound bodies are masked themselves. Maximum one name recoverable. I declined — the ladder already caught everyone who ever talked.

**Why is a third of your clients NO DATA?** Not a bug — an honest contract. There isn't enough activity to judge those 462 clients, and the pipeline refuses to manufacture a score.

**Why not hard-code the schemas?** I don't control the source and there are no changelogs. Discovery is self-healing; hard-coding fails silently when the SaaS adds a field. Silent nulls are the worst class of bug.

**How do you know it's correct?** Source-to-target reconciliation — the CRM says 1,390 coaching clients, the profile holds exactly 1,390. Dedup keeps latest by update timestamp. Every morning's profile is reproducible. And I don't trust green — the fragment bug, the truncated-table catch, and the ghost rows were all green when found.

**How much does it cost, and how do you know?** About $0.80 a night — 2.2 to 2.3 DBUs on jobs-serverless at $0.35 per DBU, verified in the billing system, not estimated. The incident day where I broke and repaired everything cost $3.80. I know both numbers because I looked.

**SCD1 or SCD2 — why?** The profile is an SCD2 engagement ledger — 9,472 versions today. A business-state MD5 opens a new version only when engagement state changes; clock metrics are excluded so you don't get a new version every day. The daily snapshot table is separate.

**Can you detect churn?** State, yes, today — the CRM carries Contract Expired, 248 leads among its top statuses. Events, one build away — bronze keeps every payload version, so transitions out of Coaching Client are reconstructable. What I won't claim is that engagement data tells you why someone churned — that needs inputs this system doesn't have.

**What if it fails at 8 AM?** Error-isolated bronze means one bad source doesn't block the other eight. The watermark re-pulls exactly the miss. Repair-run re-executes only failed tasks and dependents.

**Would you use machine learning here?** I prototyped it and chose not to ship it — placeholder models around logging, versus a deterministic engine that worked. For a once-a-day, 1,390-client problem, ML is cost and fragility with no payoff.

**What's a wrapper key?** Same name can be prose in one family, the whole payload in another. JSON_OBJECT was free text in some sources and the actual wrapper in others. I classify by role, not by name.

**Tell me about a bug you almost shipped.** My own 11/17 hardening blanket-exempted free-text keys from wrapper unwrapping — right for prose, wrong for two families where JSON_OBJECT is the wrapper. Caught in the dry-run rebuild before it shipped. Two pins added. And a month later the union bug proved the corpus again — this time in production, fixed same day, pinned as check 32.

## PART 9 — TELL ME ABOUT YOURSELF

Nearly three decades in construction — carpenter, foreman, managing multimillion-dollar projects from blueprint to delivery. My job was never just building structures. It was coordinating people, managing dependencies, preventing failures, and making sure the final product solved the customer's problem. Data engineering turned out to be the same work with different tools. Concrete, schedules, and crews became Databricks, SQL, Python, and Delta tables. This pipeline was my transition project and my first production-scale platform on real business data.

Two stories worth having ready. During validation I found records were being filtered before ranking, which let expired clients stay active downstream — rank first, filter second took the book from 1,636 to 1,381 (measured at the time). That's not an optimization, that's correctness. And a later audit caught a silently truncated table: 29 active clients and $185,174 in contract value completely absent from scoring, while the pipeline stayed green and no alerts fired (measured at the time). Monitoring detects failures. Validation detects incorrect results.

Hygiene: after a workspace-wide reference audit I dropped 11 orphaned legacy tables and got the catalog to exactly 44 tables, a one-to-one map between database and version-controlled code (measured at the time). The production core today is 55 tables — 10 bronze (nine sources plus a source-inventory config), 25 silver, 20 gold — with the ML-prototype schemas kept quarantined in their own schemas, separate from the pipeline. During development I managed the job in the UI so I could restructure gold three times while the shape was still forming; once it stabilized, I moved it into a bundle — defined as code, git-versioned, UI locked. And the deploy is itself part of the test now: the same-day diagnose-fix-pin-repair loop on the last two incidents is exactly the operational story an interviewer wants to hear.

## THE SPINE OF THE STORY

The utils arc is the strongest material I have: the 3 AM silent-loss rescue → 46 notebooks collapsed into one generic engine → auto-discovery → recursive type reconciliation → repair ladder → quarantine contracts → 32-pin regression corpus → fleet health → the union bug caught, pinned, and repaired green by the next morning. That's a day-after-day engine-hardening narrative where every guard is traceable to a dated incident — and every number in it, I've verified.

**Delivery note:** lead with the regression story — "my own hardening broke two families, and the safety net I'd built caught it before prod" beats any hypothetical. Close with the drift forecast — "here's what I expect to break next and what I'd do about it" — because the person who can enumerate the next failure is the person who built the last thirteen fixes.
