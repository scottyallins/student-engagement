# RELAY — cross-workspace AI coordination log

Canonical home: `scottyallins/student-engagement.git` (deterministic source lineage).
The port (`scottyallins/Autonomous`) and the epistemic/ML workspace coordinate through the owner.
This file is the message bus: each AI commits its report here; the owner pulls once.
Clipboard relay is O(n^2) transport and already caused one wrong-remote incident and one namespace collision.

## Conventions (binding on all workspaces)
- Plain AX frozen at 520; plain HYP frozen at 008. Everything newer is origin-prefixed (PORT-*, SRC-*, CO-*).
  Registering new text under an occupied ID silently re-points every citation (AX-528).
- Verification must match the claim's object: code for in-workspace claims, `git fetch` for cross-workspace claims.
- Receipts required — a report without a verification path is a claim, not a fact.
- Registry ownership: the ML workspace's `assumption_registry` is canonical for the AX/HYP namespace.

## Current rulings
- Repo topology: student-engagement.git = canonical deterministic lineage; Autonomous = clean-room canary.
- ML FREEZE (2026-11-27, owner directive): labels and observation only. No ML-driven decisions until lifted.
  Substrate: `crm_ingestion.ml_prototype.training_frame_v2_multi_date` (15,464 pairs; UP 2,108 / DOWN 1,966 /
  STABLE 11,390; 17 label dates) — nightly refresh = task `gold_ml_frame` (3_21).
- Blind-spot census materialized: `crm_ingestion.silver._column_census` (585 rows; 234 registered-blind,
  351 unregistered-blind).

## Open items
- Deploy gap (AX-518): bundle carries retries + on-failure email + gold_ml_frame; live job still on old settings.
- Late-arrival watermark: bounded-lookback CDC patch pending owner approval.
- Frame ownership across workspaces (source nightly proposed as owner of the label substrate).
