# UTILS_INDEX — generated 2026-11-27 03:27

GENERATED FILE — never hand-edit. Regenerate after every utils.py change:
%run utils, then `build_utils_index()`; commit the result with the change.

127 functions and classes across this engine, alphabetical.

| Name | Kind | Line | Cell | Description |
| --- | --- | --- | --- | --- |
| `__bool__` | def | 1402 | # Repair Ladder Tests | — |
| `_alias_verdict` | def | 1936 | # Schema Discovery & Bronze Parsing | 11/25/2026 — PURE resolver (corpus-pinned, no Spark). frac_map: |
| `_append_silver_atomic` | def | 3971 | # Silver Writing | 11/17/2026 — staged window-replace for child-table CDC writes, |
| `_apply_contract_remap` | def | 2376 | # Schema Discovery & Bronze Parsing | Rebuild the value struct so silver keeps the CONTRACT name carrying |
| `_apply_overprepared_quoting` | def | 737 | # JSON Repair UDF | Rung 5 OPTIONAL rules — the over-prepared path. Quotes digits-only |
| `_Boom` | class | 1401 | # Repair Ladder Tests | — |
| `_cast_tree` | def | 3868 | # Silver Writing | Build a Column expressing a source value (type sdt) as the TARGET type. |
| `_children` | def | 1532 | # Schema Discovery & Bronze Parsing | Yield (parent_path, [new child names]) for struct levels. |
| `_cleanup_run` | def | 4683 | # Orchestrator | 11/17/2026 — centralized cleanup guard: drop every temp table and |
| `_col_fingerprint` | def | 1922 | # Schema Discovery & Bronze Parsing | 11/25/2026 — one agg + one small take: the column's current biology |
| `_convert_python_literals` | def | 498 | # JSON Repair UDF | Null handling (spec): convert Python-style None/True/False to JSON |
| `_dedup_names` | def | 3253 | # Field Utilities | Ensure all names are unique by appending _1, _2, etc. for duplicates. |
| `_detect_and_unwrap` | def | 948 | # JSON Repair UDF | Recursively unwrap JSON wrappers by TYPE INSPECTION (not by key name). |
| `_discover_children` | def | 4713 | # Orchestrator | Discover children via schema (arrays) then prefix fallback. |
| `_discover_lineage_tree` | def | 4703 | # Orchestrator | Auto-discover hierarchical lineage tree from silver table schemas. |
| `_dt` | def | 6272 | # ENGINE HEALTH — fleet control room | — |
| `_dump_pk_df` | def | 3502 | # Silver Writing | Distinct non-null PK values inside ONE bronze dump (raw_data JSON). |
| `_enforce_gold_contract_table` | def | 2402 | # Schema Discovery & Bronze Parsing | 11/24/2026 — post-write gold-contract check (called by the write_silver |
| `_ensure_tombstone_columns` | def | 3490 | # Silver Writing | Add the engine-managed tombstone columns to a silver entity table. |
| `_escape_control_chars_in_strings` | def | 693 | # JSON Repair UDF | Rung 5 — escape raw control characters that JSON forbids inside |
| `_escape_other_breakers` | def | 822 | # JSON Repair UDF | Ladder rung 5 — string-level insurance, only reached after quote |
| `_et_sort_key` | def | 3744 | # Silver Writing | 11/24/2026 — CAST-TO-TIMESTAMP primary sort for event-time |
| `_extract_single_quoted_wrapper` | def | 469 | # JSON Repair UDF | Ladder rung 2 — bounded string-level unwrap, pure insurance. |
| `_find_event_time` | def | 3723 | # Silver Writing | 11/17/2026 — resolve the payload's business event-time column from |
| `_format_bucket` | def | 1896 | # Schema Discovery & Bronze Parsing | 11/25/2026 — cheap format classifier for fingerprint pinning. |
| `_full_rebuild_child_from_source` | def | 4463 | # Array Explosion & Child CDC | Child-table CDC escape hatch (Nov 12 2026): when a child's write |
| `_full_rebuild_custom_child` | def | 4629 | # Custom Field Extraction | Custom-child CDC escape hatch (Nov 12 2026): the raw custom struct |
| `_full_rebuild_parent` | def | 3808 | # Silver Writing | CDC escape hatch (Nov 12 2026): the full-history rebuild the Nov 9 |
| `_gap_finding` | def | 5982 | # Gap Taxonomy Engine I — Vocabulary Drift, Sticky Baselines, Dispatcher, Pins | Shared finding shape for the whole gap-taxonomy engine (Tranches 1-4). |
| `_gap_pin` | def | 6142 | # Gap Taxonomy Engine I — Vocabulary Drift, Sticky Baselines, Dispatcher, Pins | — |
| `_gold_drift_q` | def | 2270 | # Schema Discovery & Bronze Parsing | Best-effort audit row to _gold_drift_quarantine — the operator trail. |
| `_gold_quarantine_write` | def | 2008 | # Schema Discovery & Bronze Parsing | gold.gold_drift_quarantine — the drift paper trail (append-only). |
| `_grain_ratio` | def | 1651 | # Schema Discovery & Bronze Parsing | 11/23/2026 — CARDINALITY TELEMETRY: rows / distinct pk on the batch |
| `_guard_gold_contract` | def | 2313 | # Schema Discovery & Bronze Parsing | 11/24/2026 — parse-time gold-contract gatekeeper (called before |
| `_guard_grain_shift` | def | 1691 | # Schema Discovery & Bronze Parsing | 11/24/2026 — HALTING grain-shift airbag. Noise gates, all must hold: |
| `_guard_key_entropy` | def | 1505 | # Schema Discovery & Bronze Parsing | 11/24/2026 — HALTING dynamic-key airbag, called in parse_bronze_table |
| `_guard_magnitude_drift` | def | 1756 | # Schema Discovery & Bronze Parsing | 11/24/2026 — HALTING magnitude airbag, called in parse_bronze_table |
| `_guard_no_duplicate_definitions` | def | 56 | # Structural Guard — duplicate definitions fail loudly at import | — |
| `_is_nested_schema_mismatch` | def | 3684 | # Silver Writing | True when a CDC write died on a nested-schema mismatch — the MERGE |
| `_latest_per_pk` | def | 3761 | # Silver Writing | Collapse to the LATEST version per PK — the one-row-per-id silver |
| `_learn_alias` | def | 2253 | # Schema Discovery & Bronze Parsing | Persist {old: new} auto-resolutions (idempotent; read side dedups). |
| `_learned_aliases` | def | 2237 | # Schema Discovery & Bronze Parsing | {old_name: new_name} from _col_aliases (append log; later rows win, |
| `_lineage_walk` | def | 6637 | # Raw Lineage Scan | Print one field path with its datatype; recurse into structs/arrays. |
| `_looks_like_date` | def | 771 | # JSON Repair UDF | True when a string value matches one of the known date shapes |
| `_make_token` | def | 414 | # JSON Repair UDF | 11/17/2026 — collision-proof token derivation. The tokens |
| `_merge_silver` | def | 4031 | # Silver Writing | CDC upsert by PK: UPDATE existing ids in place, INSERT new ids. |
| `_needs_repair_scan` | def | 2574 | # Schema Discovery & Bronze Parsing | 11/17/2026 — per-row wrapper/quote inspection; True when ANY |
| `_next_value_char` | def | 525 | # JSON Repair UDF | — |
| `_norm_name` | def | 2231 | # Schema Discovery & Bronze Parsing | Normalize a column name for comparison: lowercase, alphanumerics only. |
| `_normalize_smart_quotes` | def | 639 | # JSON Repair UDF | Rung 1b — typographic smart quotes (the curly forms of double and |
| `_ok` | def | 5037 | # Orchestrator | — |
| `_parent_pk` | def | 3658 | # Silver Writing | Pinned per-family PK first (SILVER_MERGE_KEYS — the same pins the |
| `_parse_activity_final` | def | 1168 | # JSON Repair UDF | 11/19/2026 — BLANKET FAIL-SAFE wrapper (the 'one weird row kills the |
| `_parse_activity_final_impl` | def | 1041 | # JSON Repair UDF | Normalize JSON and return a valid JSON string for ALL tables. |
| `_parse_activity_flags` | def | 1190 | # JSON Repair UDF | Diagnostics companion to repair_json_udf — SAME ladder, but returns |
| `_parse_activity_telemetered` | def | 1242 | # JSON Repair UDF | — |
| `_prev_value_char` | def | 519 | # JSON Repair UDF | — |
| `_process_table_inner` | def | 5102 | # Orchestrator | Full Bronze → Silver pipeline: parse, flatten structs, write parent table. |
| `_protect_stringified_inner_quotes` | def | 571 | # JSON Repair UDF | Ladder rung 4 extension — tokenize the double quotes inside |
| `_quote_unquoted_keys` | def | 654 | # JSON Repair UDF | Rung 5 — quote unquoted keys ({name: value} -> {"name": value}). |
| `_reconcile_to_target` | def | 3920 | # Silver Writing | Cast a CDC batch's EXISTING columns to the target table's Delta types. |
| `_registry_read` | def | 1473 | # Schema Discovery & Bronze Parsing | 11/17/2026 — registry fields for a silver table as {name: ddl_type}. |
| `_registry_union` | def | 1581 | # Schema Discovery & Bronze Parsing | 11/17/2026 — union discovered schema with the registry's prior |
| `_registry_write` | def | 1482 | # Schema Discovery & Bronze Parsing | 11/17/2026 — record a discovered StructType into the registry |
| `_repair_single_quotes` | def | 860 | # JSON Repair UDF | Escalation inside the quote-repair path. |
| `_resolve_utils_source` | def | 23 | # Structural Guard — duplicate definitions fail loudly at import | Find this file on disk: __file__ first, then cwd candidates (bundle root), |
| `_restore_apostrophe_tokens` | def | 450 | # JSON Repair UDF | In parsed dict/list, restore token to ' in problem keys after unwrapping. |
| `_restore_dquote_tokens` | def | 611 | # JSON Repair UDF | Revert (*DQUOTE*) -> " across every string value, in OBJECT space |
| `_run_log_write` | def | 1606 | # Schema Discovery & Bronze Parsing | 11/17/2026 — RUN LOG: one row per parse run, appended to |
| `_rung_mix_to_json` | def | 1255 | # JSON Repair UDF | 11/19/2026 — batch rung mix as a compact JSON dict {"rung": count} |
| `_safe_ident` | def | 127 | # Configuration | 11/17/2026 — assert a name is safe to interpolate into SQL DDL. |
| `_sanitize_col_name` | def | 3246 | # Field Utilities | Sanitize: only alphanumeric and underscores allowed, everything else -> _. |
| `_scan_date_like_keys` | def | 787 | # JSON Repair UDF | Post-parse date locator: walk the parsed object and collect every |
| `_scan_duplicate_definitions` | def | 43 | # Structural Guard — duplicate definitions fail loudly at import | Pure (no Spark, no IO): every re-defined function/class as (name, first_line, dup_line). |
| `_scan_invalid_number_keys` | def | 805 | # JSON Repair UDF | Post-parse scan: keys whose values are leading-zero number-strings |
| `_strip_array_wrapper` | def | 2762 | # Schema Discovery & Bronze Parsing | Extract the inner type from ARRAY<...> via bracket-depth matching. |
| `_tokenize_apostrophe` | def | 443 | # JSON Repair UDF | — |
| `_tokenize_dquotes` | def | 603 | # JSON Repair UDF | — |
| `_type_label` | def | 6630 | # Raw Lineage Scan | — |
| `_unwrap_problem_keys` | def | 935 | # JSON Repair UDF | After successful parse, turn ['value'] back into 'value' for problem keys. |
| `_walk` | def | 5785 | # FUTURE USE — Drift, Deletion & Identity Detectors | — |
| `_wrap_problem_keys_in_string_and_tokenize` | def | 432 | # JSON Repair UDF | Wrap problem-key values and replace every ' in them with the token. |
| `_write_silver_inner` | def | 4175 | # Silver Writing | Write DataFrame to silver table. CDC mode if _CDC_WATERMARK is set. |
| `alias_contract_pins` | def | 1963 | # Schema Discovery & Bronze Parsing | 11/25/2026 — Tranche 6 corpus pins: the four resolver verdicts + the |
| `audit_event_time_format` | def | 5824 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — event-time survivorship orders STRING event times |
| `audit_event_time_regressions` | def | 5878 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — the ONE failure mode of event-time survivorship. The |
| `audit_nested_schema_drift` | def | 5771 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — the escape hatch nobody guards: the column-drop guard |
| `audit_schema_registry` | def | 5721 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — registry hygiene. The grow-only schema registry pins |
| `build_utils_index` | def | 6873 | # Utils Index Generator (regenerates UTILS_INDEX.md) | Regenerate UTILS_INDEX.md — the alphabetical function registry of this engine. |
| `categorize_fields` | def | 3233 | # Field Utilities | Return (scalars, arrays, structs) from DataFrame schema. |
| `check_field_budget` | def | 5692 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — column-explosion guard for wide sources. Reads the |
| `check_vocabulary_drift` | def | 5988 | # Gap Taxonomy Engine I — Vocabulary Drift, Sticky Baselines, Dispatcher, Pins | AX-02/AX-113 — vocabulary/enum drift for one (silver table, column). |
| `cleanup_orphaned_temp_tables` | def | 5444 | # Orchestrator | 11/17/2026 — engine-start sweep; AGE-GATED 11/24/2026: drops only |
| `detect_deletions` | def | 3518 | # Silver Writing | Tombstone sweep for ONE watched entity table. Reads the last |
| `detect_dynamic_key_maps` | def | 5632 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — dynamic-key maps ("user_123": {...}" — the KEY is data). |
| `detect_epoch_unit_drift` | def | 5668 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — epoch-unit drift (seconds vs millis vs micros) and |
| `detect_pk_format_change` | def | 5591 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — PK format drift (int id → UUID migration at the source, |
| `enforce_gold_contracts` | def | 2023 | # Schema Discovery & Bronze Parsing | 11/24/2026 — THE GOLD CONTRACT GATE (job task gold_contract_gate). |
| `engine_health_report` | def | 6203 | # ENGINE HEALTH — fleet control room | Fleet health in one command. Returns a findings dict (empty = clean). |
| `explode_to_child` | def | 4354 | # Array Explosion & Child CDC | Explode an array column into a child table. Recurse on nested arrays until scalar. |
| `extract_custom` | def | 4522 | # Custom Field Extraction | Extract custom struct AND custom_cf_ columns. |
| `find_child_mapping_keys` | def | 4775 | # Orchestrator | Return candidate mapping/natural-key columns: non-null and unique on a |
| `find_join_keys` | def | 4846 | # Orchestrator | Value-verify FK→PK joins across ALL silver tables (diagnostic; reads all |
| `find_pk` | def | 3309 | # Field Utilities | Find the best candidate for a primary key column. |
| `find_problem_keys` | def | 5006 | # Orchestrator | DISCOVER problem keys (free-text fields that break quote repair) with |
| `flatten_structs` | def | 3280 | # Field Utilities | Flatten StructType columns into individual columns. |
| `get_silver_watermark` | def | 1428 | # Schema Discovery & Bronze Parsing | Get last CDC watermark from Silver parent table. |
| `GoldContractError` | class | 2223 | # Schema Discovery & Bronze Parsing | Raised by the gold-contract gatekeeper: a contract column gold SQL |
| `GrainShiftError` | class | 1682 | # Schema Discovery & Bronze Parsing | Raised by _guard_grain_shift: the batch's rows/distinct-PK is >= 2x |
| `invoke_gap_check` | def | 6105 | # Gap Taxonomy Engine I — Vocabulary Drift, Sticky Baselines, Dispatcher, Pins | 11/26/2026 — the SINGLE dispatch point for the gap-taxonomy engine. |
| `KeyEntropyError` | class | 1494 | # Schema Discovery & Bronze Parsing | Raised by _guard_key_entropy: >= 10 brand-new field names under one |
| `MagnitudeDriftError` | class | 1748 | # Schema Discovery & Bronze Parsing | Raised by _guard_magnitude_drift: a numeric field's batch median is |
| `parse_bronze_table` | def | 2431 | # Schema Discovery & Bronze Parsing | Read Bronze, dedup snapshot re-dumps, detect wrapper, repair via UDF (runs ONCE), materialize to temp table, parse. |
| `pin_column_fingerprint` | def | 1955 | # Schema Discovery & Bronze Parsing | 11/25/2026 — run at adjudication time; paste the printed dict into |
| `print_keys_only` | def | 4832 | # Orchestrator | One-line keys summary for a table (PK \| FK \| mapping candidates). |
| `print_lineage_detail` | def | 4802 | # Orchestrator | Print one table: PK, carried-FK and mapping-key candidates, every column |
| `print_lineage_tree` | def | 4762 | # Orchestrator | Print the lineage tree with hierarchical numbering. |
| `process_table` | def | 5318 | # Orchestrator | 11/17/2026 — CLEANUP-GUARD WRAPPER around _process_table_inner: any |
| `raw_lineage_tree` | def | 6658 | # Raw Lineage Scan | Scan bronze raw payloads and print EVERY column path with its datatype. |
| `refresh_vocab_baselines` | def | 6045 | # Gap Taxonomy Engine I — Vocabulary Drift, Sticky Baselines, Dispatcher, Pins | AX-02 — WRITE-MODE, sticky by doctrine. Rebuilds silver._vocab_baselines for the |
| `repair_flag_summary` | def | 1204 | # JSON Repair UDF | Once-per-source repair review. Runs the flags UDF over a bronze |
| `replay_quarantined` | def | 5373 | # Orchestrator | 11/17/2026 — QUARANTINE REPLAY: reprocess payloads parked in |
| `report_deletion_drift` | def | 5540 | # FUTURE USE — Drift, Deletion & Identity Detectors | FUTURE USE — hard-delete / GDPR-erasure candidates. |
| `resolve_contract_observed` | def | 1994 | # Schema Discovery & Bronze Parsing | 11/24/2026 — schema-level observed-name resolution: for each required |
| `resolve_gold_columns` | def | 2289 | # Schema Discovery & Bronze Parsing | {missing_contract_name: replacement_source_name} for CONFIDENT renames |
| `run_tables` | def | 5348 | # Orchestrator | 11/17/2026 — NEW batch runner: process several bronze tables in one |
| `write_silver` | def | 4333 | # Silver Writing | 11/24/2026 — thin GOLD-CONTRACT wrapper around _write_silver_inner: |
