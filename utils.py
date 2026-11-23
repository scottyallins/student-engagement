# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Silver Engine Utils
# MAGIC
# MAGIC Shared utility functions for Bronze → Silver ingestion. Call via `%run /Users/scottsbv@gmail.com/student-engagement/utils`

# COMMAND ----------

# DBTITLE 1,# Configuration
# ============================================================================
# SILVER LAYER - CONFIGURATION 11/17/2026
# ============================================================================
# Bronze: crm_ingestion.bronze  (9 raw tables, full-loaded from Postgres)
# Silver: crm_ingestion.silver  (parsed, flattened, exploded)
# ============================================================================

from pyspark.sql.functions import (
    col, lit, from_json, schema_of_json, explode, explode_outer, udf,
    get_json_object, regexp_replace, to_json, expr, create_map,
    md5, concat_ws, coalesce, when, lower, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, ArrayType, StringType, IntegerType, LongType
)
from builtins import round, max, min
import time, re, json
import uuid  # 11/17/2026 — _RUN_ID per-run suffix (unique temp table/view names)

# JSON repair UDF defined in the 'JSON Repair UDF' cell below (single source of truth — do not duplicate).
# NOTE: _PROBLEM_KEYS (in the JSON Repair UDF cell) is DOMAIN-SPECIFIC to Close CRM, not universal.
# It was discovered empirically by profiling source data — see that cell for methodology.

CATALOG = "crm_ingestion"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# --- 11/17/2026 additions (see header changelog for the full list) ---
# Per-run id: temp tables/views get a unique suffix so two concurrent
# pipelines on the same source can no longer drop each other's _tmp_* table
# mid-read (fixed names collided).
_RUN_ID = uuid.uuid4().hex[:8]

# Cache registry — RETIRED 11/23/2026: nothing appends to this anymore
# (cache() removed from parse_bronze_table; see the dated note there — on
# serverless each cached DataFrame costs a leaked session schema until
# Databricks garbage-collects them). Kept as an empty list so the
# _cleanup_run() uncache loop stays a harmless no-op.
_CACHED_DFS = []

# SQL identifier guard — anything interpolated into DDL/MERGE text is
# asserted safe first (defense-in-depth against injection through
# discovered/renamed column names).
_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def _safe_ident(name):
    """11/17/2026 — assert a name is safe to interpolate into SQL DDL."""
    if not _IDENT_RE.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name

# Module-level state variables
_TEMP_TABLES = []
_pipeline_counter = 0
_lineage_tree = []  # List of (number, depth, table_name)
_CDC_WATERMARK = None  # Set by process_table in CDC mode; read by write_silver

# 11/17/2026 — SENTINEL GATE (opt-in per source, default EMPTY = dormant):
# raw string values that are NULL placeholders at the SOURCE ("", "null",
# "None", "N/A", "-") are counted as DATA by from_json and pollute silver
# with pseudo-values (the '87 empty users' class). Which sentinels are safe
# to null is a BUSINESS decision per source. Map: bronze table name -> set
# of lowercase sentinel values; parse_bronze_table applies them to STRING
# fields of the parsed struct AFTER parse. Nothing changes until you add
# an entry. Example: {"leads_raw": {"", "n/a", "null", "none"}}
#
# GOOD sentinels (safe at almost any source — the column's business domain
# provably excludes them):
#   ""        empty string (the classic emptiness marker)
#   "null"    the JSON-ish null text
#   "none"    python-repr None rendered as text
#   "n/a"     explicit not-applicable
#   "\\n"     Postgres COPY null token
#   "nan"     in STRING fields only (never in numeric columns — typed)
# BAD sentinels (usually REAL data — do NOT add blindly):
#   "unknown", "other", "tbd", "pending"  — can be legitimate categories
#   "-"        — a real note/dash in many domains; only safe if the column
#                provably never contains a meaningful dash
#   "false", "0", "-1" — typed values / booleans, not placeholders; the
#                gate is string-only anyway, so numeric sentinels CANNOT be
#                nulled here (future: per-column typed normalization)
# Trade-off to accept before adding any entry: once nulled, "was empty at
# source" and "field absent" become indistinguishable (both NULL). If that
# distinction matters downstream, do NOT null it.
SENTINEL_NULLS = {}

# Pipeline number mapping (fixed per pipeline, not execution-order dependent)
PIPELINE_NUMBERS = {
    "leads_raw": 1,
    "lead_activites_raw": 2,
    "close_crm_users_raw": 3,
    "custom_activites_raw": 4,
    "all_payments": 5,
    "calendly_scheduled_events": 6,
    "student_sentiment": 7,
    "mdl_users_raw": 8,
    "lead_merges": 9,
}
# 11/17/2026 NOTE: the 'lead_activites' spelling is INTENTIONAL and pinned —
# it matches existing bronze/silver table names and SILVER_MERGE_KEYS. Do not
# "fix" it; renaming is a breaking change.

# 11/17/2026 — FLEET COMPLETENESS CONTRACT: the bronze tables this engine is
# EXPECTED to process every run. This is the flag that was missing: silver
# only knows what it PARSED — if bronze silently loads 8 of 9 tables, the
# silver run looks green and nothing compared its activity against the
# expected fleet. engine_health_report() (Engine Health cell) checks every
# name here for: missing run-log rows, empty-increment streaks, and stale
# bronze (max insert_date age). Update this list when the estate grows.
EXPECTED_BRONZE_TABLES = sorted(PIPELINE_NUMBERS.keys())

print(f"✅ Config loaded: {CATALOG}.{BRONZE_SCHEMA} → {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,# JSON Repair UDF
# ============================================================================
# JSON REPAIR UDF 11/17/2026 — universal normalization with type-based wrapper detection
# RESOLVED Nov 9 2026 (was: pre-existing limitation, unrelated to the converter): a multi-key single-quoted outer holding a double-quoted stringified inner is now rescued — NOT by generalizing rung 2's anchored regex (multi-key breaks its boundedness), but by extending the rung-4 token scheme: the stringified inner's double quotes are tokenized (*DQUOTE*) in text space before the flip, then reverted in OBJECT space after parse, which turns the value back into a parseable '{...}' string that the existing post-parse wrapper detection (Rule 1) unwraps as usual. Current bronze has zero rows in this shape — insurance for future sources. See _protect_stringified_inner_quotes for the remaining documented residual (single-quoted inner inside a double-quoted value).
# 11/17/2026 HARDENING PASS — this cell was rebuilt from the 11/17/2026
# review; every change carries an inline '# 11/17/2026' comment:
#   * _MAX_JSON_DEPTH recursion cap — a deeply-nested wrapper row raised
#     RecursionError, which is NOT in the caught (JSONDecodeError, TypeError)
#     set anywhere and killed the whole UDF task; now fails safe to NULL.
#   * _make_token() — repair tokens derived PER ROW and PROVEN absent; a
#     literal (*APOSTROPHE*)/(*DQUOTE*)/APOSTROWORD in source data could
#     collide with the scheme and be silently 'restored' into a quote.
#   * Problem-key wrap lookahead widened '\w+' -> '[^']+' — space-containing
#     keys like "Current Situation" never matched the next-key lookahead,
#     so the value span ended early and the wrap misfired.
#   * Rule 1 unwrap skips _PROBLEM_KEYS — free text whose CONTENT happens
#     to be valid JSON is data, never a wrapper (was silently restructured).
#     (11/18/2026 refinement: the skip uses _UNWRAP_EXEMPT_KEYS — true
#     free-text keys only; JSON_OBJECT is a wrapper key and must unwrap.)
#   * Nested Rule-1 repair derives its own safe tokens (same fix, one
#     nesting level down).
# ============================================================================
# ESCALATION LADDER — first success wins, fail-safe NULL at the end.
# This ladder is the accumulated reasoning from the repair design sessions;
# keep it in sync with _parse_activity_final:
#   Rung 1. Direct parse (fast path). Clean double-quoted JSON needs no
#      surgery — apostrophes are legal inside values, so free text survives
#      untouched. Wrapper unwrap happens AFTER any successful parse (a
#      wrapper IS valid JSON; you cannot extract it from a string you
#      cannot parse without fragile regex surgery) — see _detect_and_unwrap.
#   Rung 1b. Smart-quote normalization (typographic curly quotes used as
#      DELIMITERS by non-conformant producers), applied ONLY inside the
#      escalation path — rows that parse clean at rung 1 are never touched,
#      so smart quotes that are legitimate VALUE content survive. After
#      normalization the full ladder runs on the normalized string, so
#      curly single-quote delimiters are rescued by the flip as well.
#   Rung 2. String-level unwrap (bounded insurance): a row that is exactly
#      a single-key single-quoted wrapper {'key': '<stringified JSON>'}
#      gets its inner string extracted and the WHOLE ladder run recursively
#      on it. This rescues the one shape the quote flip can never reach:
#      a single-quoted outer holding a double-quoted inner, because
#      flipping the outer quotes corrupts the inner ones before the inner
#      is ever seen. Anchored pattern, one key only, skip on no-match,
#      fall through to the flip on any failure.
#   Rung 3. Quote flip: APOSTROWORD protects word apostrophes (letter'
#      letter, e.g. O'Brien, it's) globally BEFORE any quote is flipped;
#      then all ' → " and APOSTROWORD → ' (reverted in TEXT space before
#      parse — safe because ' is legal inside double-quoted values).
#      11/17/2026: APOSTROWORD is derived per row via _make_token, no
#      longer the hardcoded literal.
#   Rung 4. Wrap+token: problem-key values are wrapped in a single-element
#      array and every ' inside is replaced with (*APOSTROPHE*) — catching
#      the apostrophes APOSTROWORD cannot (non-word-adjacent, e.g. girls'
#      night). Reverted in OBJECT space after parse, where every character
#      is legal — that post-parse revert point is what makes the token
#      scheme sound.
#      Extended Nov 9: single-quoted values that look like double-quoted
#      stringified JSON ('{"id": 1}') get their inner " tokenized as
#      (*DQUOTE*) before the same flip, rescuing the multi-key single-
#      quoted outer (see _protect_stringified_inner_quotes); reverted in
#      object space too, before wrapper detection. 11/17/2026: BOTH tokens
#      are now derived per row via _make_token and proven absent — the old
#      'never occur in source data' assumption is gone.
#   Rung 5. Escape other breakers (string-level insurance, reached only
#      after every repair above has failed on this row): quote unquoted
#      KEYS (string-aware, never inside string values), escape raw control
#      characters inside strings (pure rescue — parsed values unchanged,
#      a tab stays a tab), quote unquoted values that start with a digit
#      but contain letters (e.g. 12abc), double bare backslashes not
#      already part of a valid escape, and remove trailing commas before
#      } or ]. The former residual — digits-only invalid numbers (007) and
#      bare dates (2026-11-06) — is OPT-IN via the module switches
#      QUOTE_INVALID_NUMBERS / QUOTE_BARE_DATES (default False = the row
#      still NULLs; see _apply_overprepared_quoting): quoting changes a
#      would-be number into a string, which is right for identifiers and
#      dates-as-text and wrong for true numeric measures — decide per
#      source, then review via repair_json_flags_udf / repair_flag_summary.
#   Rung 6. NULL — fail-safe drop, visible in the engine's parse % line.
#
# Wrapper detection (post-parse, recursive — every stringified inner JSON
# re-runs the whole ladder on the inner string, so unwrap-then-parse
# happens at every nesting level). Rules, by type inspection not key names:
#   1. dict value that is a string starting with '{' or '[' → stringified JSON
#      (11/17/2026: true free-text keys (_UNWRAP_EXEMPT_KEYS) are exempt —
#      their string values are free text, i.e. DATA, never wrappers; 11/18
#      fix: JSON_OBJECT is NOT exempt, it is a wrapper key)
#   2. dict with exactly one key whose value is a list → array wrapper
#      (guarded against the token scheme's own single-string wrap — see
#      the inline comment in _detect_and_unwrap Rule 2)
#   3. otherwise → actual record
#
# Free-text fields (_PROBLEM_KEYS) are wrapped in a single-element array
# only when quote repair is needed, then unwrapped after parse so the
# original text is preserved. Clean rows are left untouched.
#
# _PROBLEM_KEYS is domain-specific (Close CRM free-text fields that break
# quote repair). For a new source, profile failed parses and update the set.
# The detection logic is universal; the key set is not.

_PROBLEM_KEYS = {
    # High-volume free-text (lead_activites_raw + leads_raw)
    "text",
    "body_text",
    "body_preview",
    "subject",
    "note",
    "note_html",
    "html",
    "outcome_autofill_reasoning",
    "user_note",
    "user_note_html",
    "name",
    "template_name",
    "title",
    "error_message",
    "filename",
    "description",

    # Wrapper keys that appeared as single-quoted JSON
    "JSON_OBJECT",

    # leads_raw free-text / custom fields
    "Current Situation",
    "Objections Faced?",
    "Reactivation Campaign",
    "SMS Opt-Out",
    "career goal timeline",
    "Lead Quality",
    "Current Salary",
    "Visa Status",
    "Ontraport_Call Notes",
    "Churn RCA",
    "Company Name",}

# 11/18/2026 — REGRESSION FIX (JSON_OBJECT). The 11/17 free-text guard
# exempted every _PROBLEM_KEYS key from the Rule-1 wrapper unwrap. That is
# right for prose keys (text, subject, name...) but WRONG for JSON_OBJECT:
# it is not free text — it is the WRAPPER KEY that carries the entire
# payload for close_crm_users_raw and custom_activites_raw, whose bronze
# rows are {"INSERT_DATE": ..., "JSON_OBJECT": "{'data': [...]}"} (outer
# parses clean at rung 1; the inner is single-quoted JSON repaired by the
# nested ladder). The exemption froze those tables at wrapper level:
# discovery saw only INSERT_DATE + a 200KB string, no user fields were
# ever extracted, full-history rebuilds degraded to STRING and aborted,
# and the CDC path would have written wrapper-level rows to silver.
# JSON_OBJECT stays in _PROBLEM_KEYS (the rung-4 string-level wrap still
# protects it when IT appears single-quoted); only the post-parse UNWRAP
# exemption is lifted. True free-text keys keep the exemption.
_UNWRAP_EXEMPT_KEYS = _PROBLEM_KEYS - {"JSON_OBJECT"}

# --- Rung-5 over-prepared switches (Nov 9 2026 upgrade). Defaults OFF so
# existing behavior is byte-identical. Flip per source (driver-side,
# before process_table; same pattern as _CDC_WATERMARK) when you decide the
# value is worth keeping as a string instead of NULLing the row:
#   QUOTE_INVALID_NUMBERS = True  ->  digits-only invalid numbers (007)
#       survive as strings (zip codes, account numbers, SKUs...)
#   QUOTE_BARE_DATES      = True  ->  bare dates (2026-11-06) survive as
#       castable strings
# Reset to False after the source is reviewed (or fix it at source).
# Visibility: repair_json_flags_udf / repair_flag_summary report which keys
# took on quoting — see the flags section at the bottom of this cell.
QUOTE_INVALID_NUMBERS = False
QUOTE_BARE_DATES = False

# --- 11/17/2026 ladder-safety additions (see header changelog) -------------

# Recursion cap for the repair/unwrap recursion. Before: a deeply-nested
# wrapper row raised RecursionError — which is NOT in the caught
# (JSONDecodeError, TypeError) set anywhere — and killed the whole UDF task.
# Now such a row fails safe to NULL (or stops unwrapping, keeping what it
# has).
_MAX_JSON_DEPTH = 50

def _make_token(s, base):
    """11/17/2026 — collision-proof token derivation. The tokens
    (*APOSTROPHE*)/(*DQUOTE*)/APOSTROWORD were ASSUMED absent from source
    text; a literal occurrence in a free-text field would have been
    silently 'restored' into a quote character, corrupting the row. Now the
    token is derived PER ROW and proven absent (the while loop escalates
    the suffix until it is). Deterministic — same string in, same token
    out — so the inserting side and the restoring side always agree without
    passing the token around. Design rules: letters, digits and
    parens/asterisks only (no quotes, backslashes or control chars — no
    rung can touch it), and one token word per character class so the
    revert codebook stays 1:1."""
    tok, i = base, 0
    while tok in s:
        i += 1
        tok = base.replace("*)", f"{i}*)") if base.endswith("*)") else f"{base}{i}"
    return tok

def _wrap_problem_keys_in_string_and_tokenize(s, token="(*APOSTROPHE*)"):
    """Wrap problem-key values and replace every ' in them with the token.
    Example: 'key': 'O'Reilly' → 'key': ['O(*APOSTROPHE*)Reilly']
    """
    for key in _PROBLEM_KEYS:
        # Pattern: matches the problem key up to the next closing ', next key, or }
        # 11/17/2026 — lookahead was '\w+', which can never match the next key
        # when it contains a space — and _PROBLEM_KEYS is full of them
        # ("Current Situation", "Churn RCA"), so the value span ended early
        # and embedded apostrophes broke the wrap. '[^']+' matches any key.
        pattern = rf"('{key}'):\s*'(.*?)'(?=\s*,\s*'[^']+'\s*:|\s*}})"
        def _tokenize_apostrophe(match):
            value = match.group(2).replace("'", token)
            return f"{match.group(1)}: ['{value}']"
        s = re.sub(pattern, _tokenize_apostrophe, s, flags=re.DOTALL)
    return s


def _restore_apostrophe_tokens(obj, token="(*APOSTROPHE*)"):
    """In parsed dict/list, restore token to ' in problem keys after unwrapping."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _PROBLEM_KEYS:
                if isinstance(v, list) and len(v) == 1:
                    if isinstance(v[0], str):
                        obj[k] = v[0].replace(token, "'")
                elif isinstance(v, str):
                    obj[k] = v.replace(token, "'")
                else:
                    _restore_apostrophe_tokens(v, token)
            else:
                _restore_apostrophe_tokens(v, token)
    elif isinstance(obj, list):
        for item in obj:
            _restore_apostrophe_tokens(item, token)


def _extract_single_quoted_wrapper(s):
    """Ladder rung 2 — bounded string-level unwrap, pure insurance.
    Matches ONLY a row that is exactly a single-key, single-quoted wrapper
    whose value looks like stringified JSON:
        {'payload': '{"id": "x"}'}
    Returns the inner string, or None when the row is not that exact shape.
    Bounded by construction:
      - whole-string anchor: multi-key rows never match; they fall through
        to the quote flip exactly as before this rung existed;
      - the inner must start with '{' or '[' — a plain quoted value is a
        string, not a wrapper;
      - the greedy inner capture runs to the final quote of the string,
        which is correct for a well-formed wrapper; any other ending fails
        the anchor and falls through.
    Why this rung exists: a single-quoted outer holding a double-quoted
    stringified inner is the ONE shape the quote flip can never reach —
    flipping the outer quotes corrupts the inner ones. Extracting the inner
    first rescues it. Current bronze has zero rows in this shape; this rung
    is insurance for future sources.
    """
    m = re.match(r"^\s*\{\s*'(\w+)'\s*:\s*'(.*)'\s*\}\s*$", s, flags=re.DOTALL)
    if not m:
        return None
    inner = m.group(2).strip()
    if inner and inner[0] in ("{", "["):
        return m.group(2)
    return None


def _convert_python_literals(s):
    """Null handling (spec): convert Python-style None/True/False to JSON
    null/true/false in a CONTROLLED manner — only in value positions and
    only OUTSIDE quoted strings.

    A character-level scanner tracks string spans (single or double quotes,
    backslash-escaped), so words inside narrative text are never touched:
        {'v': None, 'f': True}    -> {"v": null, "f": true}
        {'ids': [None, False]}    -> {"ids": [null, false]}
        {'text': 'None'}          -> {"text": "None"}   (string preserved)
        {'note': 'x: None, [y]'}  -> string content unchanged

    Value position = token preceded (after whitespace) by ':', ',', '[' or
    '(' and followed (after whitespace) by ',', '}', ']' or ')'. Runs inside
    the repair ladder BEFORE the quote flip, so a double-quoted row with an
    unquoted None is also rescued.
    """
    VALUE_OPEN = ":,[("
    VALUE_CLOSE = ",}])"
    WS = " \t\r\n"

    def _prev_value_char(j):
        k = j - 1
        while k >= 0 and s[k] in WS:
            k -= 1
        return s[k] if k >= 0 else None

    def _next_value_char(j):
        k = j
        while k < n and s[k] in WS:
            k += 1
        return s[k] if k < n else None

    out = []
    i = 0
    n = len(s)
    in_str = None
    while i < n:
        c = s[i]
        if in_str:
            if c == "\\" and i + 1 < n:
                out.append(s[i:i + 2])
                i += 2
                continue
            if c == in_str:
                in_str = None
            out.append(c)
            i += 1
            continue
        if c in ("'", '"'):
            in_str = c
            out.append(c)
            i += 1
            continue
        if c in "NTF":
            for tok, conv in (("None", "null"), ("True", "true"), ("False", "false")):
                if s.startswith(tok, i):
                    prev = _prev_value_char(i)
                    nxt = _next_value_char(i + len(tok))
                    if prev is not None and prev in VALUE_OPEN \
                            and nxt is not None and nxt in VALUE_CLOSE:
                        out.append(conv)
                        i += len(tok)
                        break
            else:
                out.append(c)
                i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _protect_stringified_inner_quotes(s, token="(*DQUOTE*)"):
    """Ladder rung 4 extension — tokenize the double quotes inside
    stringified-JSON values so the quote flip cannot corrupt them.

    Rescues the shape rung 2 deliberately does not reach: a MULTI-key
    single-quoted outer holding a double-quoted stringified inner:
        {'a': 'x', 'payload': '{"id": 1}'}
    The flip would make the value's delimiters the same character as the
    inner's quotes and corrupt the row. Tokenizing the inner " in TEXT
    space lets the row parse; _restore_dquote_tokens reverts the token in
    OBJECT space, the value is again a parseable '{...}' string, and the
    post-parse wrapper detection (Rule 1) unwraps it — the rescue composes
    with the existing machinery instead of generalizing rung 2's anchored
    regex (multi-key would break its boundedness).

    Bounded by construction:
      - cheap guard: only single-quoted outers containing at least one
        double quote are processed at all;
      - per-value check: only single-quoted values that START with '{' or
        '[' AND contain a " are tokenized (plain strings and numbers
        never collide with the flip and are left untouched);
      - the value-span pattern is the same key/value lookahead the
        problem-key wrap uses (next-key or '}' anchor), so an apostrophe
        inside the inner JSON does not end the span early.
    Residual, documented: a single-quoted inner inside a DOUBLE-quoted
    value ("{'inner': 'x'}") is still not rescuable — there is no " to
    protect; zero rows in current bronze.
    """
    _t = s.strip()
    if not ((_t.startswith("{'") or _t.startswith("[{'")) and '"' in s):
        return s
    pattern = r"'(\w+)'\s*:\s*'(.*?)'(?=\s*,\s*'\w+'\s*:|\s*})"
    def _tokenize_dquotes(match):
        value = match.group(2)
        if value[:1] in ("{", "[") and '"' in value:
            return "'" + match.group(1) + "': '" + value.replace('"', token) + "'"
        return match.group(0)
    return re.sub(pattern, _tokenize_dquotes, s, flags=re.DOTALL)


def _restore_dquote_tokens(obj, token="(*DQUOTE*)"):
    """Revert (*DQUOTE*) -> " across every string value, in OBJECT space
    (post-parse, where every character is legal). Runs BEFORE wrapper
    detection so a protected stringified inner is again a parseable
    '{...}' string and Rule 1 can unwrap it. Unlike the apostrophe token
    (problem keys only), DQUOTE tokens can appear under any key."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                if token in v:
                    obj[k] = v.replace(token, '"')
            else:
                _restore_dquote_tokens(v, token)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, str):
                if token in item:
                    obj[i] = item.replace(token, '"')
            else:
                _restore_dquote_tokens(item, token)


# ============================================================================
# RUNG-1B + RUNG-5 UPGRADE HELPERS (Nov 9 2026) — cover every breaker class
# discover_all_taxonomy can report. All are INSIDE the escalation path:
# rows that parse clean at rung 1 are never touched by any of them.
# ============================================================================

def _normalize_smart_quotes(s):
    """Rung 1b — typographic smart quotes (the curly forms of double and
    single quotes) used as DELIMITERS by non-conformant producers.
    Applied only to rows that already failed the raw direct parse, so
    smart quotes that are legitimate value content in a clean row are
    never touched. After normalization every lower rung runs on the
    normalized string — curly single-quote delimiters are rescued by the
    flip like any other single-quoted row. Documented edge: a
    rung-1-failing row whose VALUES contain smart quotes has them
    normalized too; such rows were NULL-bound anyway, and the delimiter
    case is the common one."""
    return (s.replace("\u201c", '"').replace("\u201d", '"')
             .replace("\u2018", "'").replace("\u2019", "'"))


def _quote_unquoted_keys(s):
    """Rung 5 — quote unquoted keys ({name: value} -> {\"name\": value}).
    String-aware: the key pattern is applied only OUTSIDE quoted spans, so
    a 'word: value' shape inside an existing string value is never injected
    with quotes (the naive regex misfires there). Tracks both quote
    characters — rung 5 sees post-flip (double-quoted) rows, but the helper
    is safe pre-flip too."""
    KEY_PATTERN = re.compile(r'([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)')
    out = []
    i = 0
    n = len(s)
    in_str = None
    while i < n:
        c = s[i]
        if in_str:
            if c == "\\" and i + 1 < n:          # keep escaped chars intact
                out.append(s[i:i + 2])
                i += 2
                continue
            if c == in_str:
                in_str = None
            out.append(c)
            i += 1
            continue
        if c in ("'", '"'):
            in_str = c
            out.append(c)
            i += 1
            continue
        m = KEY_PATTERN.match(s, i)
        if m:                                     # unquoted key — quote it
            out.append(m.group(1) + '"' + m.group(2) + '"' + m.group(3))
            i = m.end()
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _escape_control_chars_in_strings(s):
    """Rung 5 — escape raw control characters that JSON forbids inside
    strings (tab, newline, CR, FF, BS and other chars below 0x20), INSIDE
    quoted spans only. Pure rescue: the escape sequence is exactly how
    that character is legally represented, so the PARSED value is
    byte-identical (a tab stays a tab) — unlike the opt-in quoting rules
    this changes no type, so it is always on at rung 5. Outside strings
    these characters are legal whitespace and are left alone (a naive
    global replace would corrupt structural whitespace)."""
    _ESC = {"\t": "\\t", "\n": "\\n", "\r": "\\r", "\f": "\\f", "\b": "\\b"}
    out = []
    i = 0
    n = len(s)
    in_str = None
    while i < n:
        c = s[i]
        if in_str:
            if c == "\\" and i + 1 < n:          # keep existing escapes intact
                out.append(s[i:i + 2])
                i += 2
                continue
            if c == in_str:
                in_str = None
                out.append(c)
                i += 1
                continue
            if c in _ESC:
                out.append(_ESC[c])
                i += 1
                continue
            if ord(c) < 0x20:
                out.append("\\u%04x" % ord(c))
                i += 1
                continue
            out.append(c)
            i += 1
            continue
        if c in ("'", '"'):
            in_str = c
        out.append(c)
        i += 1
    return "".join(out)


def _apply_overprepared_quoting(s, track=None):
    """Rung 5 OPTIONAL rules — the over-prepared path. Quotes digits-only
    invalid numbers (007) and bare dates (2026-11-06) so the row PARSES
    instead of NULLing. Type-changing by design: those values become
    strings — the right call for free-text/identifier data (zip codes,
    account numbers, SKUs, dates-as-text), the wrong call for true numeric
    measures (decide per source). Defaults come from the module switches
    QUOTE_INVALID_NUMBERS / QUOTE_BARE_DATES (False = unchanged
    behavior). Records fired rules in `track` for repair-flags
    visibility."""
    if track is None:
        track = {}
    if QUOTE_INVALID_NUMBERS:
        _s = re.sub(r'([:,\[\(]\s*)(0\d+)(\s*[,}\])])', r'\1"\2"\3', s)
        if _s != s:
            track["quoted_invalid_numbers"] = True
            s = _s
    if QUOTE_BARE_DATES:
        _s = re.sub(r'([:,\[\(]\s*)(\d{4}-\d{2}-\d{2})(\s*[,}\])])', r'\1"\2"\3', s)
        if _s != s:
            track["quoted_bare_dates"] = True
            s = _s
    return s


# --- Post-parse date locator (upgrade request: scan for these) -------------

_DATE_PATTERNS = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "%Y-%m-%d"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"), "%Y-%m-%dT%H:%M:%S"),
    (re.compile(r"^\d{2}/\d{2}/\d{4}$"), "%m/%d/%Y"),
    (re.compile(r"^\d{2}-\d{2}-\d{4}$"), "%m-%d-%Y"),
]

def _looks_like_date(s):
    """True when a string value matches one of the known date shapes
    (YYYY-MM-DD, ISO datetime, MM/DD/YYYY, MM-DD-YYYY)."""
    from datetime import datetime as _dt
    if not isinstance(s, str):
        return False
    for pat, fmt in _DATE_PATTERNS:
        if pat.match(s):
            try:
                _dt.strptime(s[:19], fmt)   # ignore timezone tail
                return True
            except ValueError:
                continue
    return False


def _scan_date_like_keys(obj):
    """Post-parse date locator: walk the parsed object and collect every
    key whose value looks like a date-as-text — the columns that take on
    quoting under QUOTE_BARE_DATES, and generally the date-typed columns
    silver should cast. Returns a sorted, de-duplicated key list."""
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and _looks_like_date(v):
                keys.add(k)
            else:
                keys.update(_scan_date_like_keys(v))
    elif isinstance(obj, list):
        for item in obj:
            keys.update(_scan_date_like_keys(item))
    return sorted(keys)


def _scan_invalid_number_keys(obj):
    """Post-parse scan: keys whose values are leading-zero number-strings
    (007, 0123) — the columns that take on quoting under
    QUOTE_INVALID_NUMBERS (zip codes, account numbers...)."""
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and re.match(r"^0\d+$", v):
                keys.add(k)
            else:
                keys.update(_scan_invalid_number_keys(v))
    elif isinstance(obj, list):
        for item in obj:
            keys.update(_scan_invalid_number_keys(item))
    return sorted(keys)


def _escape_other_breakers(s):
    """Ladder rung 5 — string-level insurance, only reached after quote
    repair (rungs 3 AND 4) has already failed on this row. Handles the next
    most common reasons a flipped/tokenized string still won't parse:
      - unquoted values that start with a digit but aren't pure numbers
        (e.g. 12abc) — quoted so they parse as strings;
      - bare backslashes not already part of a valid JSON escape;
      - trailing commas before } or ].
    Deliberately NOT fixed HERE (former residual, now opt-in): digits-only
    invalid numbers such as 007 and bare dates such as 2026-11-06 — quoting
    those changes a would-be number into a string. They stay NULL by
    default; the over-prepared switches QUOTE_INVALID_NUMBERS /
    QUOTE_BARE_DATES (applied by _apply_overprepared_quoting right after
    this function, rung 5 of _repair_single_quotes) opt in per source.
    Context-blind edge, accepted: the digit rule can match a 'x: 12abc,'
    shape INSIDE a quoted value — but this rung only ever sees rows that
    are already NULL-bound, so the worst case is the row staying NULL;
    rows that parse at a higher rung are never touched by it.
    """
    # Unquoted values that start with a digit but aren't pure numbers
    s = re.sub(
        r'([:,\[\(]\s*)([0-9][0-9a-zA-Z_\-\.]*[a-zA-Z][0-9a-zA-Z_\-\.]*)(\s*[,}\])])',
        r'\1"\2"\3',
        s
    )
    # Escape bare backslashes that are not already part of a valid escape
    # 11/17/2026 — fixed escaping: the pattern must match ONE bare backslash
    # (not preceded by another, not starting a valid \" \b \f \n \r \t \u \/
    # escape) and replace it with a doubled pair, so the row parses and the
    # value keeps exactly one backslash. (The 11/17 rebuild had landed the
    # pattern double-escaped — it matched PAIRS of backslashes only, so a
    # single bare \y never got doubled and the row NULLed.)
    s = re.sub(r'(?<!\\)\\(?![\"\\\\/bfnrtu])', r'\\\\', s)
    # Remove trailing commas before } or ]
    s = re.sub(r',\s*([}\]])', r'\1', s)
    return s


def _repair_single_quotes(s, token="(*APOSTROPHE*)", track=None, dquote_token="(*DQUOTE*)"):
    """Escalation inside the quote-repair path.
    11/17/2026: APOSTROWORD is derived per row via _make_token (a literal
    'APOSTROWORD' in source text used to be silently flipped into an
    apostrophe by the revert), and the DQUOTE token is threaded in so the
    caller's restorer and this function always agree.
    1. Protect word apostrophes
    2. Convert Python literals
    3. Try clean quote flip
    4. Tokenize stringified-inner double quotes (*DQUOTE*), wrap + tokenize
       problem keys, then flip
    5. Escape other breakers — unquoted keys, raw control chars in strings,
       digit-leading values, bare backslashes, trailing commas, then the
       OPT-IN over-prepared quoting (insurance — only if everything above
       failed; records the rescuing rung and fired repairs in `track`)
    """
    track = {} if track is None else track
    # 1. Protect word apostrophes globally first.
    # 11/17/2026 — placeholder derived per row and PROVEN absent from this
    # row's text (was the hardcoded literal, which source text could contain
    # and the revert would then corrupt into an apostrophe).
    _aw = _make_token(s, "APOSTROWORD")
    protected = re.sub(r"([a-zA-Z])'([a-zA-Z])", rf"\1{_aw}\2", s)

    # 1b. Controlled Python-literal conversion (spec: null handling) —
    #     unquoted None/True/False -> null/true/false, outside quoted
    #     strings only. Quoted 'None' strings are source data and survive.
    protected = _convert_python_literals(protected)

    # 2. Try parse without wrap/token
    attempt = protected.replace("'", '"').replace(_aw, "'")
    try:
        json.loads(attempt)
        track["rung"] = 3
        return attempt          # clean success
    except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
        pass

    # 3. Tokenize stringified-inner double quotes, wrap & tokenize problem
    # keys, then flip quotes. _protect_stringified_inner_quotes is a no-op
    # for rows with no double-quoted stringified values, so this attempt is
    # byte-identical to the old one for every such row.
    # 11/17/2026 — DQUOTE token threaded (was a hardcoded default; the
    # caller now derives it per row so insertion and restore agree).
    dquote_protected = _protect_stringified_inner_quotes(protected, token=dquote_token)
    wrapped = _wrap_problem_keys_in_string_and_tokenize(dquote_protected, token=token)
    attempt = wrapped.replace("'", '"').replace(_aw, "'")
    try:
        json.loads(attempt)
        track["rung"] = 4
        return attempt
    except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
        pass

    # 4. Escape other breakers — insurance, only reached if everything above
    # failed. A JSONDecodeError here propagates to the caller and the row
    # becomes NULL (fail-safe rung 6).
    # Nov 9 2026 upgrade — three additions in the rung-5 pipeline:
    #   a. quote unquoted keys (string-aware — never inside string values)
    #   b. escape raw control characters inside strings (pure rescue —
    #      parsed values unchanged, a tab stays a tab)
    #   c. apply the OPT-IN over-prepared quoting (007 / bare dates —
    #      module switches; default off = previous NULL residual)
    track["rung"] = 5
    escaped = _quote_unquoted_keys(attempt)
    if escaped != attempt:
        track["unquoted_key_repair"] = True
    _c = _escape_control_chars_in_strings(escaped)
    if _c != escaped:
        track["control_char_repair"] = True
    escaped = _apply_overprepared_quoting(_escape_other_breakers(_c), track=track)
    json.loads(escaped)
    return escaped


def _unwrap_problem_keys(obj):
    """After successful parse, turn ['value'] back into 'value' for problem keys."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _PROBLEM_KEYS and isinstance(v, list) and len(v) == 1:
                obj[k] = v[0]
            else:
                _unwrap_problem_keys(v)
    elif isinstance(obj, list):
        for item in obj:
            _unwrap_problem_keys(item)


def _detect_and_unwrap(parsed, _depth=0):
    """Recursively unwrap JSON wrappers by TYPE INSPECTION (not by key name).
    Detection rules (applied recursively until actual records are reached):
      1. If a dict has a value that is a string starting with '{' or '[' → stringified
         inner JSON. Try to parse it (with single-quote repair fallback). If successful,
         recurse on the inner data.
      2. If a dict has exactly ONE key whose value is a LIST → array wrapper.
         Extract the list.
      3. Otherwise → actual record, return as-is.
    This is universal: it does not check for specific key names like 'JSON_OBJECT' or 'data'.
    It detects wrappers by inspecting value types, so it works on any source system
    regardless of what they name their wrapper keys.
    11/17/2026: (a) recursion capped at _MAX_JSON_DEPTH — pathological
    nesting used to raise RecursionError (caught nowhere) and kill the UDF
    task; unwrapping now stops and keeps what it has. (b) Rule 1 skips
    _UNWRAP_EXEMPT_KEYS — free text whose CONTENT happens to be valid JSON
    was silently unwrapped and restructured; free-text values are data,
    never wrappers. (11/18/2026 fix: the exemption is free-text keys ONLY —
    JSON_OBJECT is a wrapper key carrying whole payloads for
    close_crm_users_raw / custom_activites_raw and must unwrap.)
    """
    # 11/17/2026 — recursion cap: stop unwrapping, keep what we have
    if _depth >= _MAX_JSON_DEPTH:
        return parsed
    if isinstance(parsed, dict):
        # Rule 1: Check for stringified inner JSON wrapper (any value that looks like JSON)
        for key, val in parsed.items():
            # 11/18/2026 — regression fix: exempt only TRUE free-text keys
            # (_UNWRAP_EXEMPT_KEYS). JSON_OBJECT is a wrapper key, not prose —
            # exempting it froze close_crm_users/custom_activites at wrapper
            # level. 11/17/2026 — free-text guard: problem keys are prose by
            # definition; their string values are DATA, never wrappers
            # (user text that happens to be valid JSON was restructured)
            if key in _UNWRAP_EXEMPT_KEYS:
                continue
            if isinstance(val, str) and val.strip() and val.strip()[0] in ('{', '['):
                # Try parsing as-is first (might already be valid JSON)
                try:
                    inner = json.loads(val.strip())
                    return _detect_and_unwrap(inner, _depth=_depth + 1)  # 11/17/2026 — depth-capped
                except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
                    # Try repairing single quotes and re-parsing
                    try:
                        # 11/17/2026 — nested repair derives its OWN safe
                        # tokens (was hardcoded defaults — a literal token
                        # string in this value collided with the scheme)
                        _tok = _make_token(val, "(*APOSTROPHE*)")
                        _dqt = _make_token(val, "(*DQUOTE*)")
                        repaired = _repair_single_quotes(val, token=_tok, dquote_token=_dqt)
                        inner = json.loads(repaired)
                        # Inner repair may have wrapped/tokenized problem keys —
                        # restore HERE: the outer repaired_row flag in
                        # _parse_activity_final never sees this nested repair,
                        # so tokens would otherwise leak into the output.
                        _unwrap_problem_keys(inner)
                        _restore_apostrophe_tokens(inner, token=_tok)
                        # DQUOTE tokens from the extended rung 4 revert here
                        # too, before recursion — same leak-prevention logic.
                        _restore_dquote_tokens(inner, token=_dqt)
                        return _detect_and_unwrap(inner, _depth=_depth + 1)
                    except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
                        pass  # Not a JSON wrapper, continue checking other keys
        # Rule 2: Single-key array wrapper (dict with exactly one key whose value is a list)
        # GUARD against the token scheme's own wrapping: a repaired problem-key
        # value is a single-element list of a STRING ('text': ['girls' night']),
        # which is exactly this wrapper's shape. Without the guard, a single-key
        # record with one problem key is misread as a wrapper here, the extracted
        # list dodges _unwrap_problem_keys/_restore_apostrophe_tokens (they only
        # recurse through dicts), and the token LEAKS into silver. A list of
        # DICTS is still a genuine wrapper even under a problem-key name, and a
        # clean (unrepaired) single-string array under a problem key is kept
        # as the record's value rather than unwrapped to a bare string.
        if len(parsed) == 1:
            _k, val = next(iter(parsed.items()))
            _is_token_wrap = (
                _k in _PROBLEM_KEYS
                and isinstance(val, list)
                and len(val) == 1
                and isinstance(val[0], str)
            )
            if isinstance(val, list) and not _is_token_wrap:
                # Recurse on the extracted list — elements may have their own wrappers
                return _detect_and_unwrap(val, _depth=_depth + 1)  # 11/17/2026 — depth-capped
        # No wrapper detected — actual record
        return parsed
    elif isinstance(parsed, list):
        # Recurse into each element — an element may itself have a wrapper.
        # For example [{"payload": "{ 'records': [...] }"}] needs each element unwrapped.
        # Clean records (dicts with multiple keys, no stringified values) pass through
        # unchanged — _detect_and_unwrap returns them as-is via Rule 3.
        return [_detect_and_unwrap(item, _depth=_depth + 1) for item in parsed]  # 11/17/2026 — depth-capped
    return parsed

def _parse_activity_final_impl(raw, return_flags=False, _depth=0):
    """
    Normalize JSON and return a valid JSON string for ALL tables.
    11/17/2026: (a) recursion capped at _MAX_JSON_DEPTH — a deeply-nested
    wrapper row used to raise RecursionError (not in the caught exception
    set) and kill the UDF task; it now NULLs fail-safe. (b) repair tokens
    are derived per row via _make_token — a literal (*APOSTROPHE*) /
    (*DQUOTE*) / APOSTROWORD in source data can no longer collide with the
    scheme and get silently corrupted on restore.
    Escalation ladder — first success wins, fail-safe NULL at the end:
      1. Direct parse (clean rows are left untouched)
      1b. Smart-quote normalization (typographic quotes used as
          delimiters) — INSIDE the escalation path only, so smart quotes
          that are legal value content in a clean row are never touched
      2. String-level unwrap of a single-key single-quoted wrapper, then
         the FULL ladder recursively on the inner string (insurance rung:
         a single-quoted outer holding double-quoted inner JSON can never
         be reached by the quote flip)
      3. APOSTROWORD-protected quote flip (word apostrophes preserved), plus
         controlled None/True/False -> null/true/false conversion outside
         quoted strings (_convert_python_literals — spec: null handling)
      4. Tokenize stringified-inner double quotes (*DQUOTE*) + wrap
         problem keys + (*APOSTROPHE*) token (all reverted in object space;
         the multi-key single-quoted outer with a double-quoted
         stringified inner — formerly a documented limitation — is
         rescued here and unwrapped post-parse by Rule 1)
      5. Escape other breakers — unquoted keys, raw control chars in
         strings, digit-leading unquoted values, bare backslashes,
         trailing commas; then the OPT-IN over-prepared quoting of
         digits-only invalid numbers (007) and bare dates (2026-11-06) —
         module switches QUOTE_INVALID_NUMBERS / QUOTE_BARE_DATES,
         default off = the row NULLs as before
      6. NULL — fail-safe drop, visible in the engine's parse %
    Wrapper detection is by type inspection, not by hardcoded key names, and
    runs post-parse recursively (see _detect_and_unwrap).
    Problem keys: _PROBLEM_KEYS is domain-specific.

    return_flags=True (used by repair_json_flags_udf — the once-per-source
    repair review, NOT the nightly payload path) returns a
    (payload_json, flags) tuple instead. Flags report the rescuing rung,
    which repairs fired (smart quotes, unquoted keys, control chars, opt-in
    quoting), and which keys hold date-as-text / leading-zero values — the
    columns that take on quoting.
    """
    if not raw:
        return (None, {"rung": None}) if return_flags else None
    # 11/17/2026 — recursion cap (fail-safe NULL instead of RecursionError)
    if _depth >= _MAX_JSON_DEPTH:
        if return_flags:
            return None, {"rung": "depth_limit"}
        return None
    repaired_row = False
    # 11/17/2026 — tokens derived per row, PROVEN absent (was hardcoded —
    # a literal token in source text got silently 'restored' into a quote)
    token = _make_token(raw, "(*APOSTROPHE*)")
    _dq = _make_token(raw, "(*DQUOTE*)")
    track = {}
    s = raw.strip()
    try:
        outer = json.loads(s)
        track["rung"] = 1
    except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
        # Rung 1b — smart-quote normalization, INSIDE the escalation path
        # only (clean rows above are never touched, so smart quotes that are
        # legitimate VALUE content survive). A no-op for rows without smart
        # quotes — byte-identical to the previous ladder for those rows.
        normalized = _normalize_smart_quotes(s)
        if normalized != s:
            track["smart_quote_repair"] = True
        try:
            outer = json.loads(normalized)
            track["rung"] = "1b"
        except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
            # Rung 2 — unwrap a single-key single-quoted wrapper at the STRING
            # level and run the full ladder on the inner (on the NORMALIZED
            # string — the wrapper's delimiters may be curly quotes).
            # Strictly additive insurance: no match, an unparseable inner,
            # or any unexpected error falls through to the proven
            # quote-repair rungs below, so this rung can only rescue rows,
            # never break previously-working ones.
            try:
                _inner = _extract_single_quoted_wrapper(normalized)
                if _inner is not None:
                    _nested = _parse_activity_final(_inner, _depth=_depth + 1)  # 11/17/2026 — depth-capped
                    if _nested is not None:
                        track["rung"] = 2
                        if return_flags:
                            try:
                                _obj = json.loads(_nested)
                            except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
                                _obj = None
                            _flags = {**track}
                            if isinstance(_obj, (dict, list)):
                                _flags["quoted_date_keys"] = _scan_date_like_keys(_obj)
                                _flags["quoted_invalid_number_keys"] = _scan_invalid_number_keys(_obj)
                            return _nested, _flags
                        return _nested
            except Exception:
                pass  # insurance rung must never break the proven path — fall through
            try:
                # 11/17/2026 — per-row DQUOTE token threaded (derived above)
                repaired = _repair_single_quotes(normalized, token=token, track=track, dquote_token=_dq)
                outer = json.loads(repaired)
                # Revert (*DQUOTE*) tokens in OBJECT space BEFORE wrapper
                # detection: a protected stringified inner becomes a parseable
                # '{...}' string again and Rule 1 unwraps it — the multi-key
                # rescue composes with the existing post-parse machinery.
                if _dq in repaired:
                    _restore_dquote_tokens(outer, token=_dq)
                repaired_row = True
            except (json.JSONDecodeError, TypeError, RecursionError):   # 11/17/2026 — RecursionError caught everywhere: deeply-nested rows fail safe to NULL instead of killing the UDF task
                if return_flags:
                    return None, {**track, "rung": None}
                return None
    # Universal wrapper unwrap (unchanged)
    unwrapped = _detect_and_unwrap(outer)
    # Restore apostrophe tokens after unwrapping problem keys
    if repaired_row:
        _unwrap_problem_keys(unwrapped)
        _restore_apostrophe_tokens(unwrapped, token=token)
    if return_flags:
        _flags = {**track}
        _flags["quoted_date_keys"] = _scan_date_like_keys(unwrapped)
        _flags["quoted_invalid_number_keys"] = _scan_invalid_number_keys(unwrapped)
        return json.dumps(unwrapped), _flags
    return json.dumps(unwrapped)

def _parse_activity_final(raw, return_flags=False, _depth=0):
    """11/19/2026 — BLANKET FAIL-SAFE wrapper (the 'one weird row kills the
    nightly' class). The impl below fails safe to NULL for PARSE failures
    (rung 6), but its post-parse machinery (wrapper unwrap, token restore,
    scans) had NO catch-all: an unexpected exception type (catastrophic
    regex backtracking, a monster string) escaped the UDF and killed the
    whole task. Now any Exception fails safe with rung 'exception' — the
    rung mix makes the class VISIBLE instead of fatal. Systemic failures
    (every row 'exception') still trip the all-NULL / 0-parsed guards
    loudly. Recursion routes through this wrapper per level, so every
    level is guarded. KeyboardInterrupt/SystemExit deliberately not caught.
    """
    try:
        return _parse_activity_final_impl(raw, return_flags=return_flags, _depth=_depth)
    except Exception:
        return (None, {"rung": "exception"}) if return_flags else None


from pyspark.sql.types import StringType
repair_json_udf = udf(StringType())(_parse_activity_final)


def _parse_activity_flags(raw):
    """Diagnostics companion to repair_json_udf — SAME ladder, but returns
    the repair FLAGS instead of the payload (per-row visibility into which
    rung rescued the row, which repairs fired, and which keys hold
    date-as-text / leading-zero values). For the once-per-source repair
    review (see repair_flag_summary) — not called by the nightly engine,
    so the pipeline's payload contract is unchanged."""
    _payload, flags = _parse_activity_final(raw, return_flags=True)
    return json.dumps(flags)


repair_json_flags_udf = udf(StringType())(_parse_activity_flags)


def repair_flag_summary(df, payload_col):
    """Once-per-source repair review. Runs the flags UDF over a bronze
    payload column and returns (flagged_df, rung_summary_df):
      flagged_df      — input + repair_flags JSON column (per-row visibility)
      rung_summary_df — row counts per rescuing rung (parse health at a
                       glance; rescuing_rung null = rows that still NULL)
    Drill-down — which columns took on quoting, on flagged_df:
      flagged.select(
          explode(from_json(get_json_object("repair_flags", "$.quoted_date_keys"),
                           ArrayType(StringType()))).alias("date_key")
      ).groupBy("date_key").count().orderBy("count", ascending=False)
    Review once per source; then either fix the source or flip
    QUOTE_BARE_DATES / QUOTE_INVALID_NUMBERS for that source (and cast
    those keys to string in silver)."""
    flagged = df.withColumn("repair_flags", repair_json_flags_udf(col(payload_col)))
    rung_summary = (
        flagged.groupBy(
            get_json_object(col("repair_flags"), "$.rung").alias("rescuing_rung")
        )
        .count()
        .orderBy("rescuing_rung")
    )
    return flagged, rung_summary


# ============================================================================
# 11/19/2026 — RUNG TELEMETRY: same ladder, ONE parse, TWO outputs.
# repair_json_flags_udf (above) re-parses every payload, so it stayed a
# once-per-source review tool. This variant returns the PAYLOAD and the
# RESCUING RUNG from the same single pass, letting the nightly engine log
# which rung rescued each row at zero extra parses. The payload output is
# byte-identical to repair_json_udf — return_flags=True and False return
# the same json.dumps(unwrapped) / _nested / None in every branch of
# _parse_activity_final (pinned by the corpus telemetry smoke).
# rung values: 1 (clean direct parse), '1b' (smart quotes), 2 (string-level
# wrapper unwrap), 3/4/5 (quote-repair ladder), 'depth_limit' (recursion
# cap), None (fail-safe rung 6 — NULL payload, quarantined downstream).
# ============================================================================
def _parse_activity_telemetered(raw):
    _payload, _flags = _parse_activity_final(raw, return_flags=True)
    _rung = _flags.get("rung") if isinstance(_flags, dict) else None
    return (_payload, None if _rung is None else str(_rung))


from pyspark.sql.types import StringType, StructType, StructField  # telemetry struct return
repair_json_telemetry_udf = udf(StructType([
    StructField("payload", StringType()),
    StructField("rung", StringType()),
]))(_parse_activity_telemetered)


def _rung_mix_to_json(df, rung_col="_rung"):
    """11/19/2026 — batch rung mix as a compact JSON dict {"rung": count}
    for the run log. ONE groupBy over the post-repair increment (the small
    temp-table batch, not 22M-row bronze), zero extra parses. A None rung
    (fail-safe rung 6 / unparseable) logs as "null" so a parse-failure
    spike is visible IN the mix itself."""
    _rows = df.groupBy(rung_col).count().collect()
    if not _rows:
        return None
    _mix = {}
    for _r in _rows:
        _rv = _r[rung_col]
        _mix["null" if _rv is None else str(_rv)] = _r["count"]
    return json.dumps(_mix)

# COMMAND ----------

# DBTITLE 1,# Repair Ladder Tests
# ============================================================================
# 11/17/2026 NEW — REPAIR LADDER TEST CORPUS (auto-runs on %run)
# Table-driven regression tests for _parse_activity_final. Every documented
# incident shape is now a permanent fixture, so a future ladder change that
# regresses any rescue fails LOUDLY at import time instead of silently in
# production. The last few cases pin the 11/17/2026 hardening behaviors
# specifically (free-text-as-data guard, per-row token derivation,
# recursion cap, literal-token collisions).
# ============================================================================

_LADDER_CASES = [
    # (name, raw payload, expected parsed object — None = must NULL)
    ("rung1: clean double-quoted", '{"id": 1, "note": "ok"}', {"id": 1, "note": "ok"}),
    ("rung1: apostrophe legal in value", '{"note": "it\'s fine"}', {"note": "it's fine"}),
    ("rung1: clean problem-key text untouched", '{"text": "girls\' night"}', {"text": "girls' night"}),
    ("rung3: single-quoted flip", "{'a': 'x', 'b': 2}", {"a": "x", "b": 2}),
    ("rung3: word apostrophe (O'Brien)", "{'name': 'O\'Brien'}", {"name": "O'Brien"}),
    ("rung4: non-word apostrophe wrapped", "{'text': 'girls\' night'}", {"text": "girls' night"}),
    ("rung4: problem key with spaces", "{'Current Situation': 'can\'t decide', 'id': 7}", {"Current Situation": "can't decide", "id": 7}),
    ("rung4: 11/17 space-key lookahead fix", "{'text': 'girls\' night', 'Current Situation': 'maybe'}", {"text": "girls' night", "Current Situation": "maybe"}),
    ("Rule2: array wrapper", '{"data": [{"a": 1}, {"a": 2}]}', [{"a": 1}, {"a": 2}]),
    ("Rule1: stringified inner double-quoted", '{"payload": "{\\"id\\": \\"x\\"}"}', {"id": "x"}),
    ("rung2: single-quoted wrapper", "{'payload': '{\"id\": \"x\"}'}", {"id": "x"}),
    # 11/17 note: Rule 1 returns the stringified INNER as the record — the
    # wrapper's sibling keys are wrapper metadata, per the existing silver
    # contract (unchanged from pre-11/17 behavior; pinned here as a test).
    ("rung4ext: multi-key dquote rescue", "{'a': 'x', 'payload': '{\"id\": 1}'}", {"id": 1}),
    # 11/17 note: one unwrap level only — the nested struct {"data": ...} is
    # DATA (Rule 1/2 fire on stringified/array wrappers, not nested structs).
    ("Rule1: stringified inner; nested struct stays data", '{"payload": "{\\"data\\": {\\"id\\": 1}}"}', {"data": {"id": 1}}),
    ("rung1b: smart-quote delimiters", '{\u201ca\u201d: \u201cb\u201d}', {"a": "b"}),
    ("rung5: unquoted keys", '{name: "value"}', {"name": "value"}),
    ("rung5: raw tab in value", '{"a": "x\ty"}', {"a": "x\ty"}),
    ("rung3: python literals", "{'v': None, 'f': True, 's': 'None'}", {"v": None, "f": True, "s": "None"}),
    ("rung5: trailing comma", '{"a": 1,}', {"a": 1}),
    ("rung5: bare backslash", '{"a": "x\\y"}', {"a": "x\\y"}),
    ("rung5: digit-leading value", "{'a': 12abc}", {"a": "12abc"}),
    ("rung6: invalid number NULLs by default", "{'zip': 007}", None),
    ("rung6: None input", None, None),
    ("rung6: empty string", "", None),
    ("11/17: literal (*APOSTROPHE*) token survives", "{'text': 'girls\' night (*APOSTROPHE*) plan'}", {"text": "girls' night (*APOSTROPHE*) plan"}),
    ("11/17: literal APOSTROWORD survives", "{'text': 'APOSTROWORD and girls\' night'}", {"text": "APOSTROWORD and girls' night"}),
    ("11/17: free text that looks like JSON stays data", '{"text": "{\\"not\\": \\"a wrapper\\"}"}', {"text": "{\"not\": \"a wrapper\"}"}),
    # 11/18/2026 — regression pins (JSON_OBJECT unwrap fix):
    # close_crm_users_raw / custom_activites_raw are exactly this shape —
    # clean double-quoted outer, JSON_OBJECT holding single-quoted inner.
    # The 11/17 free-text guard froze it at wrapper level; it MUST unwrap
    # to the inner records (nested single-quote repair + Rule-2 array).
    ("11/18: JSON_OBJECT wrapper unwraps",
     '{"INSERT_DATE": "2026-01-01 08:00:00", "JSON_OBJECT": "{\'data\': [{\'id\': \'u1\'}]}"}',
     [{"id": "u1"}]),
    ("11/18: exempt-key JSON_OBJECT-shaped value stays data",
     '{"subject": "{\'a\': 1}"}',
     {"subject": "{'a': 1}"}),
]

# --- runner ----------------------------------------------------------------
_ladder_failures = []
for _case_name, _raw, _expected in _LADDER_CASES:
    try:
        _got = _parse_activity_final(_raw)
    except Exception as _e:  # 11/17/2026 — the ladder must NEVER raise
        _ladder_failures.append((_case_name, f"raised {type(_e).__name__}: {_e}"))
        continue
    if _expected is None:
        _ok = _got is None
    else:
        try:
            _ok = (_got is not None) and json.loads(_got) == _expected
        except Exception:
            _ok = False
    if not _ok:
        _ladder_failures.append((_case_name, f"expected {_expected!r}, got {_got!r}"))
    else:
        print(f"  PASS  {_case_name}")

# --- 11/17/2026 unit checks: per-row token derivation -----------------------
assert _make_token("clean", "(*APOSTROPHE*)") == "(*APOSTROPHE*)"
assert _make_token("has (*APOSTROPHE*) literal", "(*APOSTROPHE*)") == "(*APOSTROPHE1*)"
assert _make_token("APOSTROWORD in text", "APOSTROWORD") == "APOSTROWORD1"
print("  PASS  11/17: _make_token escalation")

# --- 11/17/2026 recursion-cap check: pathological nesting must not raise ----
# Depth stress via NESTED ARRAYS, not stringified wrappers: stringified
# nesting re-escapes quotes each level (string size doubles per level), so
# a 200-level stringified chain is physically impossible to build — nested
# arrays exercise the same _detect_and_unwrap recursion compactly. Old
# behavior: unbounded recursion; new: capped at _MAX_JSON_DEPTH (partial
# unwrap, no crash).
_deep = "[" * 300 + "]" * 300
try:
    _deep_out = _parse_activity_final(_deep)
    assert _deep_out is not None, "deep nesting must fail safe, not crash"
    print("  PASS  11/17: recursion cap (300-deep nested arrays, no RecursionError)")
except AssertionError:
    raise
except Exception as _e:
    _ladder_failures.append(("11/17: recursion cap", f"raised {type(_e).__name__}: {_e}"))

# --- 11/17/2026 flags-path smoke test ---------------------------------------
_p, _f = _parse_activity_final("{'a': 'x'}", return_flags=True)
assert isinstance(_f, dict) and "rung" in _f, "return_flags must report the rescuing rung"
print("  PASS  11/17: return_flags smoke")

# --- 11/19/2026 rung-telemetry smoke ------------------------------------------
# The telemetry variant must return the SAME payload the nightly UDF would
# (the nightly path consumes it — byte identity is the contract), plus a
# string rung. One parse, two outputs.
_t_payload, _t_rung = _parse_activity_telemetered('{"a": 1}')
assert json.loads(_t_payload) == {"a": 1} and _t_rung == "1", \
    "clean row must pass through with rung '1'"
_t_payload, _t_rung = _parse_activity_telemetered("{'a': 'x'}")
assert _t_payload is not None and isinstance(_t_rung, str), \
    "single-quoted row must rescue with a reported rung"
_t_payload, _t_rung = _parse_activity_telemetered(None)
assert _t_payload is None and _t_rung is None, "empty row must fail safe with no rung"
_mix_df = spark.range(4).withColumn(
    "_rung", when(col("id") < 3, lit("1")).otherwise(lit(None).cast("string")))
_mix = _rung_mix_to_json(_mix_df, "_rung")
assert _mix is not None and '"1": 3' in _mix and '"null": 1' in _mix, \
    "rung mix must count rescuing rungs and surface fail-safe nulls"
print("  PASS  11/19: rung telemetry (payload identity, rung, mix)")

# --- 11/19/2026 blanket fail-safe smoke ---------------------------------------
# One pathological row raising an UNEXPECTED exception type (regex blowup,
# monster string) used to escape the UDF and kill the whole nightly task.
# The 11/19 wrapper must swallow it into fail-safe NULL with rung
# 'exception' — visible in telemetry, quarantined downstream, non-fatal.
class _Boom:
    def __bool__(self):
        raise ValueError("pathological row — __bool__ raises")
_p, _f = _parse_activity_final(_Boom(), return_flags=True)
assert _p is None and _f == {"rung": "exception"}, \
    "unexpected exception must fail safe with rung 'exception', not kill the UDF task"
assert _parse_activity_final(_Boom()) is None, "non-flags path must fail safe to NULL"
print("  PASS  11/19: blanket fail-safe (exception → rung 'exception')")

# --- verdict ------------------------------------------------------------------
if _ladder_failures:
    _msg = "\n".join(f"  FAIL {_n}: {_why}" for _n, _why in _ladder_failures)
    raise AssertionError(
        f"Repair ladder regression: {len(_ladder_failures)} of "
        f"{len(_LADDER_CASES) + 5} checks failed:\n{_msg}"
    )
print(f"✅ Repair ladder corpus: all {len(_LADDER_CASES) + 5} checks passed "
      f"({len(_LADDER_CASES)} cases + token/cap/flags/telemetry/fail-safe checks)")

# COMMAND ----------

# DBTITLE 1,# Schema Discovery & Bronze Parsing
# ====================================================================
# SCHEMA DISCOVERY & BRONZE PARSING 11/17/2026
# ====================================================================


def get_silver_watermark(silver_table):
    """Get last CDC watermark from Silver parent table.
    Returns MAX(bronze_updated_at) or None if table doesn't exist.
    11/17/2026 — the old blanket except swallowed EVERY failure, including
    transient read errors on a table that EXISTS: the engine then believed
    there was no watermark and silently took the FULL-overwrite path
    (overwriteSchema=true can drop columns). Now existence is checked
    explicitly; a missing table still returns None (legitimate first run),
    but any error on an EXISTING table propagates — loud, not silent.
    """
    # 11/17/2026 — explicit existence check instead of blanket except
    try:
        if not spark.catalog.tableExists(f"{CATALOG}.{SILVER_SCHEMA}.{silver_table}"):
            return None
    except Exception:
        return None
    # 11/17/2026 — table exists: errors now PROPAGATE (a transient read
    # failure used to trigger the full-overwrite path by accident)
    result = spark.sql(f"""
        SELECT MAX(bronze_updated_at) as max_date
        FROM {CATALOG}.{SILVER_SCHEMA}.{silver_table}
    """).collect()[0]
    return result['max_date']


# ============================================================================
# 11/17/2026 — SCHEMA REGISTRY (progressive, grow-only schemas)
# Registry table: {CATALOG}.{SILVER_SCHEMA}._schema_registry, rows of
# (silver_name, field_name, ddl_type) — the UNION of every field any
# discovery pass has ever seen for that silver table (read-side dict
# dedups repeat appends). Why: CDC batches are small and randomly sampled,
# so a field present on <1% of rows can be missed by ANY single discovery
# pass — from_json then silently drops it for the whole batch, and the next
# full overwrite removes the column from silver (the Nov 10 'SMS Opt-Out'
# drift-incident class). The registry makes discovery MONOTONIC: once a
# field has been seen, it is always part of the schema — a later, narrower
# sample can never drop it. Fields are only ever ADDED (grow-only).
# 11/18/2026 — full discovery is now the DEFAULT (sampling deleted), so
# the registry's role shifts from primary defense to monotonic safety net:
# it still protects explicit sample opt-outs and guarantees a field seen
# once is never lost again.
# ============================================================================

_SCHEMA_REGISTRY = f"{CATALOG}.{SILVER_SCHEMA}._schema_registry"

def _registry_read(silver_name):
    """11/17/2026 — registry fields for a silver table as {name: ddl_type}.
    Returns {} when the registry or the table's entry doesn't exist yet."""
    try:
        _rows = spark.table(_SCHEMA_REGISTRY).filter(col("silver_name") == silver_name).collect()
    except Exception:
        return {}
    return {r["field_name"]: r["ddl_type"] for r in _rows}

def _registry_write(silver_name, struct_type):
    """11/17/2026 — record a discovered StructType into the registry
    (append; repeat rows are fine — the read side dedups via dict)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    _rows = [(silver_name, f.name, f.dataType.simpleString()) for f in struct_type.fields]
    if not _rows:
        return
    spark.createDataFrame(
        _rows, schema="silver_name STRING, field_name STRING, ddl_type STRING"
    ).write.format("delta").mode("append").saveAsTable(_SCHEMA_REGISTRY)

def _registry_union(silver_name, discovered_st, _prior_override=None):
    """11/17/2026 — union discovered schema with the registry's prior
    knowledge. Registry fields absent from THIS discovery are re-added
    (typed from the registry); discovered fields win on conflicts (fresher
    type, and they are written back to the registry by the caller). The
    result feeds from_json, so no known field can ever be silently dropped
    by a narrow CDC sample again.
    11/18/2026 — _prior_override lets the corpus pin this function without
    touching the real registry (inject a {field_name: ddl_type} dict)."""
    _prior = _registry_read(silver_name) if _prior_override is None else _prior_override
    if not _prior:
        return discovered_st
    _out = discovered_st
    _names = {_sanitize_col_name(f.name) for f in discovered_st.fields}
    for _fn, _ddl in sorted(_prior.items()):
        if _fn not in _names and _sanitize_col_name(_fn) not in _names:
            try:
              _t = StructType.fromDDL(f"`{_fn}` {_ddl}").fields[0].dataType
            except Exception:
                _t = StringType()
            _out = _out.add(_fn, _t, True)
            print(f"  📋 Registry re-added field (grow-only schema): {_fn} {_ddl}")
    return _out


def _run_log_write(bronze_table, silver_name, mode, raw_rows=None,
                   deduped_rows=None, cdc_rows=None, repair_needed=None,
                   discovery_mode=None, fields_discovered=None,
                   parsed_valid=None, parsed_null=None, quarantined_new=None,
                   note=None, rung_stats=None):
    """11/17/2026 — RUN LOG: one row per parse run, appended to
    {SILVER_SCHEMA}._run_log (rows in / deduped / CDC / parsed /
    quarantined / fields / discovery mode / note / rung mix). The fleet's nervous
    system: volume-anomaly ('empty pull vs outage'), parse-rate, and
    field-count convergence alerting all compute from this table — the
    engine itself only prints. Written on SUCCESS, SKIP and FAILURE
    paths alike. Non-fatal by design: a logging failure never blocks
    the pipeline.
    11/19/2026 — rung_stats: NULL on rows logged before rung telemetry
    (11/19) means PRE-TELEMETRY, not zero. Any mix-over-time query must
    exclude NULLs or the history silently biases toward zero."""
    try:
        from datetime import datetime, timezone
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
        spark.createDataFrame(
            [(_RUN_ID,
              datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
              bronze_table, silver_name, mode,
              raw_rows, deduped_rows, cdc_rows, repair_needed,
              discovery_mode, fields_discovered,
              parsed_valid, parsed_null, quarantined_new, note,
              rung_stats)],
            schema=("run_id STRING, logged_at STRING, bronze_table STRING, "
                    "silver_name STRING, mode STRING, raw_rows BIGINT, "
                    "deduped_rows BIGINT, cdc_rows BIGINT, repair_needed BOOLEAN, "
                    "discovery_mode STRING, fields_discovered INT, "
                    "parsed_valid BIGINT, parsed_null BIGINT, "
                    "quarantined_new BIGINT, note STRING, rung_stats STRING")
        ).write.format("delta").mode("append").option(
            # 11/19 — rung_stats column evolves the existing _run_log table
            "mergeSchema", "true").saveAsTable(
            f"{CATALOG}.{SILVER_SCHEMA}._run_log")
    except Exception as _le:
        print(f"  ⚠️ run log skipped ({str(_le)[:60]})")


def parse_bronze_table(bronze_table, watermark=None, limit=None, silver_name=None, schema_sample=None, only_payload_hashes=None):
    """Read Bronze, dedup snapshot re-dumps, detect wrapper, repair via UDF (runs ONCE), materialize to temp table, parse.

    only_payload_hashes: 11/17/2026 — replay filter (replay_quarantined):
    restrict the batch to exactly these payload hashes after the md5 dedup,
    so quarantined payloads can be re-run through the CURRENT engine in
    isolation.

    schema_sample: 11/18/2026 — SAMPLING DELETED AS A CONCEPT. Default None
    = full discovery: schema_of_json_agg runs over EVERY distinct payload
    (post-dedup), so no field can be missed by an unlucky sample. Measured
    11/18 on the 30-day window: 4,501,507 raw rows → 517,672 distinct
    payloads; full discovery for ALL 9 tables ~40s total — and the repair
    UDF already ran over every distinct row on every run, so the sample
    never reduced the expensive part. An explicit int (e.g. 200) remains
    available as an opt-out for one-off experiments; it also re-enables
    the small-batch augmentation branch below.
    """
    df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")

    if watermark:
        df = df.filter(col("bronze_updated_at") > lit(watermark))
        print(f"  CDC: reading Bronze rows after {watermark}")

    if limit:
        df = df.limit(limit)

    # ------------------------------------------------------------------
    # SNAPSHOT-DUMP DEDUPLICATION (Nov 3 2026 discovery)
    # The Postgres extractor does not send changes — it re-dumps the
    # current-state row set on every run (insert_date = dump timestamp),
    # so a watermark pull appends full copies of data already in bronze.
    # Measured: bronze.calendly_scheduled_events held 6,355,605 rows for
    # only 101,064 distinct EVENT_URIs (~63 copies each; just 939 events
    # ever changed payload). leads_raw: 540,298 rows / 70,811 distinct ids.
    # The payload carries its own identity fields, so md5(raw_data) IS
    # the universal dedup key — no per-table key needed:
    #   - byte-identical re-dumps collapse to ONE row (first-seen copy)
    #   - every distinct payload version survives (full history kept)
    # Runs BEFORE the repair UDF, so the UDF also only runs on distinct
    # rows. payload_hash is carried into silver so CDC batches can skip
    # versions silver already holds (anti-join below).
    # ------------------------------------------------------------------
    from pyspark.sql.functions import md5, row_number
    from pyspark.sql.window import Window
    _raw_total = df.count()
    df = (df.withColumn("payload_hash", md5(col("raw_data")))
            .withColumn("_dedup_rn", row_number().over(
                Window.partitionBy("payload_hash")
                      .orderBy(col("insert_date").asc(), col("bronze_updated_at").asc())))
            .filter(col("_dedup_rn") == 1)
            .drop("_dedup_rn"))
    _dedup_total = df.count()
    if _dedup_total < _raw_total:
        print(f"  🔁 Dedup: {_raw_total:,} → {_dedup_total:,} rows ({_raw_total - _dedup_total:,} byte-identical re-dumps skipped)")

    _after = None   # 11/17/2026 — CDC post-anti-join count (run log); None = not a CDC run

    # 11/17/2026 — replay filter: restrict the batch to quarantined
    # payloads (set by replay_quarantined) after the dedup, so a replay
    # re-runs ONLY the parked payloads through the current engine.
    if only_payload_hashes is not None:
        df = df.filter(col("payload_hash").isin(list(only_payload_hashes)))
        _dedup_total = df.count()
        print(f"  🔄 Replay filter: batch restricted to {len(only_payload_hashes):,} quarantined payload(s), {_dedup_total:,} row(s) present in bronze")

    # CDC guard: a re-dumped row is NEW to bronze (fresh bronze_updated_at)
    # but may be a version silver already holds. Skip increments whose
    # payload_hash already exists in silver, or every CDC batch would
    # re-insert the whole snapshot as duplicates. CDC mode ONLY — a FULL
    # load must read everything (it overwrites silver).
    if silver_name and watermark:
        # BUGFIX (Nov 3 2026, nightly failure): the old try/except built the
        # anti-join lazily — the join definition SUCCEEDED, df.count() raised
        # UNRESOLVED_COLUMN, the except caught it, but df kept the broken join
        # in its lazy plan, so the NEXT action died. Fix: check the silver
        # table's schema EAGERLY first (.columns never poisons a plan), and
        # commit the anti-joined DataFrame only after a successful count on a
        # TEMP variable — df is never left holding a failing plan.
        try:
            _silver_tbl = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}")
            _silver_cols = _silver_tbl.columns  # eager schema resolution, no plan built
        except Exception:
            _silver_cols = None
            print(f"  ℹ️ CDC dedup unavailable this run (silver table {silver_name} not readable yet)")
        if _silver_cols is not None:
            if "payload_hash" not in _silver_cols:
                print(f"  ℹ️ CDC dedup unavailable this run ({silver_name} lacks payload_hash — one full rebuild enables it)")
            else:
                # 11/17/2026 — distinct build side: full-history re-loads can
                # leave many payload_hash copies in silver; dedup shrinks the
                # anti-join build before the join planner sizes it.
                _anti = df.join(_silver_tbl.select("payload_hash").distinct(), on="payload_hash", how="left_anti")
                _after = _anti.count()  # force full analysis on the temp variable FIRST
                df = _anti              # commit only after the action succeeded
                if _after < _dedup_total:
                    print(f"  🔁 CDC dedup: {_dedup_total:,} → {_after:,} rows (versions already in silver skipped)")

    # 11/23/2026 — cache() REMOVED. On serverless, every .cache()/.persist()
    # materializes into a per-session schema inside the workspace's internal
    # temp-table-manager catalog. Those session schemas were not garbage-
    # collected (50 leaked Nov 18–21), the catalog's 50-schema quota refused
    # the 51st, and every silver task failed on the Nov 22 & Nov 23 8AM runs
    # (QUOTA_EXCEEDED — Cannot create 1 Schema(s), estimated 51, limit 50).
    # Actions now re-run the dedup window + CDC anti-join as they did before
    # 11/17/2026; measured cost is seconds per table on incremental batches.
    # Do NOT re-add .cache()/.persist()/CACHE TABLE anywhere on serverless
    # without checking that quota first.

    # Quick check for data
    sample_row = df.select("raw_data").filter(col("raw_data").isNotNull()).first()
    if not sample_row:
        # 11/17/2026 — run log: an empty increment is the signal the
        # volume-anomaly guard computes on (empty pull vs source outage).
        _run_log_write(bronze_table, silver_name or bronze_table,
                       "CDC" if watermark else "FULL",
                       raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                       note="empty increment — no non-null raw_data after dedup/CDC")
        return None

    # Detect if data needs UDF repair — universal wrapper detection by type inspection
    # Checks for: (a) unparseable JSON (single quotes), (b) stringified inner JSON wrapper,
    # (c) single-key array wrapper. Does NOT check for specific key names.
    # 11/17/2026 — the decision now comes from a SEEDED RANDOM SAMPLE of rows,
    # not the single first row: a mixed table (first row clean, later rows
    # single-quoted) used to skip the UDF entirely and silently NULL every
    # broken row in the batch.
    def _needs_repair_scan(rows):
        """11/17/2026 — per-row wrapper/quote inspection; True when ANY
        sampled row needs the repair ladder (previously only the FIRST row
        was inspected — mixed tables silently dropped broken rows)."""
        for _txt in rows:
            if not _txt:
                continue
            try:
                _p = json.loads(_txt.strip())
            except (json.JSONDecodeError, TypeError):
                return True   # unparseable — likely single-quote delimiters
            if isinstance(_p, dict):
                for _val in _p.values():
                    if isinstance(_val, str) and _val.strip() and _val.strip()[0] in ('{', '['):
                        return True   # stringified inner JSON wrapper
                if len(_p) == 1:
                    _val = next(iter(_p.values()))
                    if isinstance(_val, list):
                        return True   # single-key array wrapper
        return False
    _scan_rows = [r[0] for r in (df.filter(col("raw_data").isNotNull())
                                   .select("raw_data")
                                   .sample(False, min(1.0, 200.0 / max(_dedup_total, 1)), seed=42)  # 11/17 — seeded, reproducible
                                   .limit(200).collect())]
    _needs_repair = _needs_repair_scan(_scan_rows)

    if _needs_repair:
        # Parse + repair via UDF (handles JSON_OBJECT wrapper, single quotes, data array)
        # 11/19/2026 — RUNG TELEMETRY: the telemetry variant returns
        # (payload, rung) from the SAME ladder pass — no second parse. The
        # payload column is byte-identical to repair_json_udf's output
        # (corpus telemetry smoke), so the downstream contract is unchanged.
        df = (df.withColumn("_repair_out", repair_json_telemetry_udf(col("raw_data")))
                .withColumn("raw_data_repaired", col("_repair_out.payload"))
                .withColumn("_rung", col("_repair_out.rung"))
                .drop("_repair_out"))

        # NOTE: custom. keys are intentionally kept as nested struct fields so the
        # custom object can be extracted into its own child table (extract_custom)

        # Write repaired data to temp table — UDF runs ONCE during write, then all reads are UDF-free
        # 11/17/2026 — per-run suffix (_RUN_ID): two concurrent pipelines on
        # the same source can no longer drop each other's _tmp_ table
        # mid-read (fixed names collided across runs).
        full_temp = f"{CATALOG}.{SILVER_SCHEMA}._tmp_{bronze_table}_{_RUN_ID}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
        spark.sql(f"DROP TABLE IF EXISTS {full_temp}")
        # 11/17/2026 — raw_data rides along in the temp table: the parse-null
        # quarantine below stores the ORIGINAL text so replay_quarantined can
        # re-run the current ladder from scratch on parked payloads.
        # 11/19/2026 — _rung rides along for the rung-mix telemetry below
        df.select("insert_date", "bronze_updated_at", "payload_hash", "raw_data", "raw_data_repaired", "_rung") \
            .write.format("delta").mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(full_temp)
        _TEMP_TABLES.append(full_temp)

        # Read from temp table (fast — no UDF)
        df = spark.table(full_temp)
    else:
        # Clean JSON detected — skip UDF repair and temp table, use raw_data directly
        print(f"  ✓ Clean JSON detected — skipping UDF repair")
        df = df.withColumn("raw_data_repaired", col("raw_data"))
        # 11/19/2026 — rung telemetry: no ladder ran, so every row is
        # direct-parse rung 1 (per the same sampled decision above)
        df = df.withColumn("_rung", lit("1"))

    total = df.count()
    if total == 0:
        print(f"  ⏭️ SKIP: No data after repair")
        _run_log_write(bronze_table, silver_name or bronze_table,
                       "CDC" if watermark else "FULL",
                       raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                       repair_needed=_needs_repair,
                       note="no rows after repair")
        return None
    print(f"  Source: {total:,} rows")

    # 11/19/2026 — RUNG TELEMETRY: batch rung mix into the run log — the
    # drift tripwire for the repair ladder. A source that suddenly sends
    # more single-quoted payloads flips the mix OVERNIGHT, visible in
    # _run_log before anyone asks why parse % moved. One groupBy over the
    # small post-repair increment, zero extra parses. Best-effort like
    # everything around the log: telemetry failure never blocks the parse.
    _rung_stats = None
    try:
        if "_rung" in df.columns:
            _rung_stats = _rung_mix_to_json(df, "_rung")
            if _rung_stats:
                print(f"  📊 Rung mix: {_rung_stats}")
            df = df.drop("_rung")
    except Exception as _rte:
        print(f"  ℹ️ rung telemetry skipped ({str(_rte)[:60]})")

    # Detect if UDF returns arrays (each row is a JSON array of records) vs single objects
    _first_row = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired").first()
    if not _first_row:
        # 11/18/2026 — parse-failure guard: rows exist and survived dedup,
        # but the repair UDF NULLed EVERY raw_data (the ladder failed on
        # the whole batch). The old silent return None made the nightly
        # report 'CDC: No new rows' on a table whose entire batch was
        # unparseable. Raise loudly; process_table's cleanup guard still
        # releases temp tables/caches before the error surfaces.
        _run_log_write(bronze_table, silver_name or bronze_table,
                       "CDC" if watermark else "FULL",
                       raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                       repair_needed=True,
                       note="repair UDF NULLed every row (ladder failure)", rung_stats=_rung_stats)
        raise RuntimeError(
            f"{bronze_table}: {_dedup_total:,} distinct rows but the repair "
            f"UDF returned NULL for every one — repair-ladder failure, "
            f"not 'no new data'")

    # ------------------------------------------------------------------
    # SCHEMA SAMPLE — eliminate bad luck (custom_activites case: a 2-row CDC
    # batch fed discovery, so any field absent from those 2 rows would be
    # silently dropped by from_json). Rules: (a) the discovered schema must
    # describe the WHOLE table, not this increment — when the batch is
    # smaller than the sample target, AUGMENT with a random sample of the
    # FULL bronze table (repaired via the UDF on just those rows, bounded
    # at the sample size); (b) sample RANDOMLY, never first-N; (c) the
    # datatype ground truth is the MAJORITY type across sample rows, not a
    # single (possibly unrepresentative) first row.
    # ------------------------------------------------------------------
    # schema_sample=None -> discover over every distinct row (no sampling).
    _SCHEMA_SAMPLE = schema_sample
    _batch_df = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired")
    _sample_df = _batch_df
    if _SCHEMA_SAMPLE is not None and total > _SCHEMA_SAMPLE:
        # 11/17/2026 — seeded sample: discovery runs are reproducible (same
        # batch, same sample), so behavior is deterministic across runs.
        _sample_df = _batch_df.sample(False, _SCHEMA_SAMPLE / total, seed=42).limit(_SCHEMA_SAMPLE)
    if _SCHEMA_SAMPLE is not None and total < _SCHEMA_SAMPLE:
        # Small batch (typical CDC increment): pull extra random rows from full
        # bronze so discovery sees fields that live outside this increment.
        try:
            _full_bronze = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")
                .select("raw_data").filter(col("raw_data").isNotNull()))
            _full_total = _full_bronze.count()
            _need = _SCHEMA_SAMPLE - total
            if _full_total > total and _need > 0:
                print(f"  🔀 Batch has only {total} row(s) — augmenting schema sample with random rows from full bronze ({_full_total:,} rows)")
                _extra = (_full_bronze.sample(False, min(1.0, _need * 3.0 / _full_total), seed=42).limit(_need)  # 11/17 — seeded, reproducible
                    .select(repair_json_udf(col("raw_data")).alias("raw_data_repaired"))
                    .filter(col("raw_data_repaired").isNotNull()))
                _sample_df = _batch_df.unionByName(_extra)
        except Exception as _aug_e:
            print(f"  ⚠️ Schema sample augmentation unavailable ({str(_aug_e)[:60]})")

    _sample_count = _sample_df.count()
    # 11/18/2026 — the log states the discovery mode explicitly (FULL vs
    # sampled) so a run report always shows which contract produced the
    # schema.
    if _SCHEMA_SAMPLE is None:
        print(f"  📊 Schema discovery: FULL — schema_of_json_agg over all {_sample_count:,} distinct rows")
    else:
        print(f"  📊 Schema discovery via schema_of_json_agg on {_sample_count} sample rows")

    # Datatype ground truth: majority type across a handful of sample rows —
    # a single first row can be unlucky (e.g. a dict row in a table of
    # arrays, or vice versa). Still type inspection of the data itself;
    # the discovered DDL is validated against it in the level loop below.
    _type_votes = {"list": 0, "dict": 0}
    for _r in _sample_df.limit(20).collect():
        try:
            _tv = type(json.loads(_r["raw_data_repaired"])).__name__
            if _tv in _type_votes:
                _type_votes[_tv] += 1
        except Exception:
            continue
    _udf_returns_array = _type_votes["list"] >= _type_votes["dict"]

    # ------------------------------------------------------------------
    # DATATYPE DETECTION + RECURSIVE SCHEMA VALIDATION
    # Ground truth for the datatype is the DATA ITSELF (type inspection of
    # the first row, _udf_returns_array), never the discovered DDL:
    # schema_of_json_agg SILENTLY DEGRADES to STRING / ARRAY<STRING> when a
    # single input exceeds its size limit (~100KB) — e.g. Postgres dump rows
    # whose JSON_OBJECT column stringifies hundreds of records (~178KB per
    # row). A STRING result for data we KNOW is an array/object means
    # DISCOVERY FAILED, not that the data is strings. On datatype change
    # (DDL ≠ actual): descend one level — explode the array into element
    # texts (from_json(..., 'ARRAY<STRING>') preserves each element's raw
    # JSON text) and rediscover there, re-inspecting the element datatype.
    # Recursion is bounded (MAX_SCHEMA_LEVELS); if every level degrades,
    # FAIL LOUDLY and skip the write, so a wrong-schema 'value'-column table
    # can never silently replace a good silver table.
    # ------------------------------------------------------------------
    def _strip_array_wrapper(ddl):
        """Extract the inner type from ARRAY<...> via bracket-depth matching."""
        depth = 0
        for i, c in enumerate(ddl):
            if c == '<':
                depth += 1
            elif c == '>':
                depth -= 1
                if depth == 0:
                    return ddl[6:i]
        return ddl

    _MAX_SCHEMA_LEVELS = 3
    _level_df = _sample_df
    _expects_array = _udf_returns_array
    _element_ddl = None
    _fail_reason = None
    for _level in range(_MAX_SCHEMA_LEVELS):
        try:
            raw_schema_str = _level_df.select(expr("schema_of_json_agg(raw_data_repaired)").alias("s")).collect()[0]["s"]
        except Exception as e:
            print(f"  ⚠️ Level {_level}: schema_of_json_agg failed ({str(e)[:70]}), falling back to single-row schema_of_json")
            raw_schema_str = spark.range(1).select(
                schema_of_json(lit(_first_row["raw_data_repaired"])).alias("s")
            ).collect()[0]["s"]
        _upper = raw_schema_str.upper()

        # --- datatype check: does the discovered DDL match what the data IS? ---
        if _upper in ("STRING", "ARRAY<STRING>"):
            if _expects_array:
                # Degraded discovery on array-of-record texts (size limit hit).
                # Descend: explode to element texts and re-inspect the datatype.
                print(f"  ⚠️ Level {_level}: schema discovery degraded to {_upper} (inputs exceed schema_of_json size limit) — descending to element level")
                # 11/18/2026 — the descent now honors the discovery mode:
                # full discovery (schema_sample=None) keeps EVERY exploded
                # element (the old hardcoded 25%-capped-at-200 sample meant
                # even schema_sample=None still sampled at element level —
                # the one thing standing between full mode and 'every
                # column'). An explicit sample size keeps the old cap.
                _desc_df = (_level_df
                    .select(explode(from_json(col("raw_data_repaired"), "ARRAY<STRING>")).alias("raw_data_repaired"))
                    .where(col("raw_data_repaired").isNotNull()))
                if _SCHEMA_SAMPLE is not None:
                    _desc_df = _desc_df.sample(False, 0.25).limit(200)
                _level_df = _desc_df
                _el_first = _level_df.select("raw_data_repaired").first()
                if _el_first is None:
                    _fail_reason = "element level is empty after explode"
                    break
                _expects_array = isinstance(json.loads(_el_first["raw_data_repaired"].strip()), list)
                continue
            _fail_reason = f"schema discovery degraded to {_upper} at level {_level} and no further descent is possible"
            break

        # --- DDL matches a real structured type at this level ---
        if _upper.startswith("ARRAY<") and not _upper.startswith("ARRAY<ARRAY<"):
            _element_ddl = _strip_array_wrapper(raw_schema_str)
        else:
            _element_ddl = raw_schema_str
        break

    if _element_ddl is None or _fail_reason is not None:
        print(f"  ❌ SCHEMA DISCOVERY FAILED datatype validation: {_fail_reason or 'no structured schema found at any level'}")
        print(f"     ⏭️ SKIP: refusing to write silver — a wrong-schema build would produce a stringified 'value' column")
        _run_log_write(bronze_table, silver_name or bronze_table,
                       "CDC" if watermark else "FULL",
                       raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                       repair_needed=_needs_repair,
                       note=f"schema discovery failed: {_fail_reason or 'no structured schema'}")
        for temp in _TEMP_TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {temp}")
        _TEMP_TABLES.clear()
        return None

    # Parse DDL and deduplicate fields
    try:
        _test_st = StructType.fromDDL(_element_ddl)
        _seen = set()
        _unique_fields = []
        for _f in _test_st.fields:
            _sn = _sanitize_col_name(_f.name)
            if _sn not in _seen:
                _seen.add(_sn)
                _unique_fields.append(_f)
        deduped_st = StructType()
        for _f in _unique_fields:
            deduped_st = deduped_st.add(_f.name, _f.dataType, _f.nullable)
    except Exception:
        deduped_st = _element_ddl

    # 11/17/2026 — grow-only schema registry: union the registry's prior
    # knowledge so a narrow CDC sample can never drop a previously-seen
    # field, and record everything this pass discovered for future passes.
    # Registry unavailable (permissions, first run) degrades to plain
    # discovery — never blocks the pipeline.
    if silver_name:
        try:
            if isinstance(deduped_st, StructType):
                deduped_st = _registry_union(silver_name, deduped_st)
                _registry_write(silver_name, deduped_st)
            else:
                print("  ℹ️ Schema registry skipped (DDL-string fallback schema)")
        except Exception as _reg_e:
            print(f"  ⚠️ Schema registry unavailable ({str(_reg_e)[:70]}) — using discovered schema only")

    # 11/18/2026 — fields-discovered count: watch discovery converge
    # run-over-run (a still-climbing count means the source keeps adding
    # columns; a flat count means full discovery + the registry have
    # converged). Registry re-adds are itemized above; this is the final
    # schema size from_json will parse with.
    if isinstance(deduped_st, StructType):
        print(f"  📊 Fields: {len(deduped_st.fields)} (discovered + registry union)")

    # Parse (fast — reads from temp table, only from_json is computed)
    if isinstance(deduped_st, str):
        # DDL string fallback — from_json accepts DDL strings directly
        _from_json_schema = f"ARRAY<{deduped_st}>" if _udf_returns_array else deduped_st
    else:
        _from_json_schema = ArrayType(deduped_st) if _udf_returns_array else deduped_st
    df_parsed = df.withColumn("parsed", from_json(col("raw_data_repaired"), _from_json_schema))
    valid = df_parsed.filter(col("parsed").isNotNull()).count()
    print(f"  Parsed: {valid:,} ({valid/total*100:.1f}%)")

    # --- 11/17/2026 — PARSE-NULL QUARANTINE (detection WITH response) -----
    # Rows whose payload the ladder or from_json could not parse used to
    # flow into the output as all-NULL ghost rows (the '2 chunks → 89
    # all-NULL rows / 5 lost activities' incident class). Now every
    # unparsed payload is preserved in {SILVER_SCHEMA}.parse_quarantine
    # keyed by payload_hash — with its ORIGINAL raw_data, so
    # replay_quarantined can re-run the current ladder on it after an
    # upgrade — and excluded from the write. Registry-backed parses only
    # (silver_name given): a dry-run sample without the registry can
    # FALSELY null wide rows, and those must not be quarantined. If the
    # quarantine table cannot be written, rows stay in the output
    # (all-NULL, as before) with a loud warning: nothing is ever dropped
    # without a record.
    _quarantined_n = df_parsed.filter(col("parsed").isNull()).count()
    _new_q_n = 0
    # 11/17/2026 — degraded-quarantine flag: if the quarantine table cannot
    # be written, the protection is silently OFF (unparsed rows stay in the
    # output, all-NULL). Protection silently off must itself be VISIBLE —
    # the flag lands in the run-log note.
    _quarantine_degraded = False
    if _quarantined_n and not silver_name:
        print(f"  ℹ️ {_quarantined_n:,} unparsed row(s) in dry-run sample — not quarantined (no registry-backed schema)")
    elif _quarantined_n:
        try:
            _qt = f"{CATALOG}.{SILVER_SCHEMA}.parse_quarantine"
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {_qt} (
                    bronze_table       STRING,
                    silver_name        STRING,
                    payload_hash       STRING,
                    bronze_insert_date TIMESTAMP,
                    bronze_updated_at  TIMESTAMP,
                    raw_data           STRING,
                    quarantined_at     TIMESTAMP
                )
            """)
            _bad = (df_parsed.filter(col("parsed").isNull())
                    .select(lit(bronze_table).alias("bronze_table"),
                            lit(silver_name).alias("silver_name"),
                            col("payload_hash"),
                            col("insert_date").alias("bronze_insert_date"),
                            col("bronze_updated_at"),
                            col("raw_data"),
                            expr("current_timestamp()").alias("quarantined_at")))
            _new_q = _bad.join(spark.table(_qt).select("payload_hash"),
                               on="payload_hash", how="left_anti")
            _new_q_n = _new_q.count()
            if _new_q_n:
                _new_q.write.format("delta").mode("append").saveAsTable(_qt)
            print(f"  🚫 Quarantined {_quarantined_n:,} unparsed row(s) "
                  f"({_new_q_n:,} new, {_quarantined_n - _new_q_n:,} already known) → {_qt}")
            # ghost rows are excluded from the write ONLY once safely recorded
            df_parsed = df_parsed.filter(col("parsed").isNotNull())
        except Exception as _qe:
            _quarantine_degraded = True
            print(f"  ⚠️ Quarantine table unavailable ({str(_qe)[:80]}) — "
                  f"keeping {_quarantined_n:,} unparsed row(s) in the output "
                  f"rather than dropping them unrecorded")

    if valid == 0:
        # 11/18/2026 — parse-failure guard: the repair pass produced text,
        # but from_json parsed NONE of it — a wrong discovered schema or a
        # ladder regression. Old behavior: silent return None → the run
        # reported 'no new data' and moved on. Raise loudly instead; the
        # write never happens, so existing silver data is still safe.
        _run_log_write(bronze_table, silver_name or bronze_table,
                       "CDC" if watermark else "FULL",
                       raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                       repair_needed=_needs_repair, parsed_valid=0,
                       parsed_null=_quarantined_n, quarantined_new=_new_q_n,
                       note="0 parsed — whole batch failed (all rows quarantined)", rung_stats=_rung_stats)
        raise RuntimeError(
            f"{bronze_table}: {total:,} repaired rows but 0 parsed — "
            f"discovered-schema/parse failure, not 'no new data'")

    # --- 11/17/2026 — SENTINEL GATE (opt-in per source, default dormant) ---
    # Raw string values that are NULL placeholders at the SOURCE ("", "null",
    # "None", "N/A", "-") are counted as DATA by from_json and pollute silver
    # with pseudo-values (the '87 empty users' class). Which sentinels are
    # SAFE to null is a business decision per source ("unknown" can be a
    # legitimate category, "-" a real note) — see SENTINEL_NULLS in the
    # Configuration cell. String fields of the parsed struct whose lower()
    # value is in the source's set become NULL; everything else untouched.
    _sents = SENTINEL_NULLS.get(bronze_table)
    if _sents and isinstance(df_parsed.schema["parsed"].dataType, StructType):
        _slist = sorted(_sents)
        for _f in df_parsed.schema["parsed"].dataType.fields:
            if isinstance(_f.dataType, StringType):
                _fc = col("parsed").getField(_f.name)
                _is_sent = lower(coalesce(_fc, lit(""))).isin(_slist)
                df_parsed = df_parsed.withColumn(
                    "parsed",
                    col("parsed").withField(
                        _f.name,
                        when(_fc.isNull() | ~_is_sent, _fc)
                            .otherwise(lit(None).cast(StringType()))))
        print(f"  🧹 Sentinel gate ({bronze_table}): {len(_slist)} value(s) "
              f"{_slist} → NULL on string fields")

    # --- 11/17/2026 — CANONICAL DEDUP (key-order / dialect duplicates) -----
    # Two payloads that differ only in KEY ORDER (or quote dialect) produce
    # different md5(raw) hashes, so the byte-level dedup above misses them
    # and the survivorship tie later falls to payload_hash — effectively
    # arbitrary. md5(to_json(parsed)) is CANONICAL: Spark's struct→JSON is
    # key-stable and the ladder already normalized the dialect, so
    # key-reordered / dialect-re-delivered payloads collapse to ONE row.
    # The RAW payload_hash remains the row identity (CDC anti-join,
    # quarantine, replay continuity) — this only removes semantic
    # duplicates from the batch. NULL-parsed rows are never dropped
    # (unrecorded data stays, per the quarantine contract).
    from pyspark.sql.window import Window as _CanonWin
    from pyspark.sql.functions import row_number as _canon_rn
    _nulls_kept = df_parsed.filter(col("parsed").isNull())
    _valid_p = df_parsed.filter(col("parsed").isNotNull())
    _cw = (_CanonWin.partitionBy(md5(to_json(col("parsed"))))
           .orderBy(col("insert_date").desc_nulls_last(),
                    col("payload_hash").desc()))
    _valid_d = (_valid_p.withColumn("_crn", _canon_rn().over(_cw))
                .filter("_crn = 1").drop("_crn"))
    _canon_dupes = _valid_p.count() - _valid_d.count()
    if _canon_dupes:
        print(f"  🔁 Canonical dedup: {_canon_dupes:,} key-order/dialect "
              f"duplicate payload(s) collapsed")
        df_parsed = _valid_d.unionByName(_nulls_kept)

    # --- 11/17/2026 — TYPE-CONFLICT TELEMETRY (silent stringification) -----
    # Measured incident: a field whose raw value is an ARRAY collapsed to
    # the STRING '["y"]' — from_json COERCES instead of failing, so the row
    # parses "fine" while the field silently degrades to text (child
    # extraction then stops working). Two signals, aggregated once per
    # batch (sampled to 10K rows on large batches):
    #   A: key PRESENT in the repaired raw but NULL after from_json
    #      (discovered-schema mismatch on this field);
    #   B: STRING-typed field whose raw value is a smuggled struct/array
    #      literal (discovery collapsed a structured type to text — the
    #      '["y"]' class; a legit text starting with '[' is a possible
    #      false positive, hence alert-only).
    # Detection only — ALERT + run-log note, never fails the run. The
    # registry / quarantine / column-drop guards already fail loud on the
    # extremes; this catches the quiet middle. Struct-path parses only
    # (array-wrapper tables: element-level scan is future work).
    _tc_note = None
    _parsed_dt = df_parsed.schema["parsed"].dataType
    if isinstance(_parsed_dt, StructType):
        try:
            _fields = [f for f in _parsed_dt.fields
                       if "'" not in f.name and "\\" not in f.name]
            _scan = df_parsed.limit(10000) if valid > 10000 else df_parsed
            _agg_exprs = []
            for _f in _fields:
                _raw = get_json_object(col("raw_data_repaired"),
                                       f"$['{_f.name}']")
                _fc = col("parsed").getField(_f.name)
                _agg_exprs.append(spark_sum(
                    when(_raw.isNotNull() & _fc.isNull(), lit(1))
                        .otherwise(lit(0))).alias(f"a::{_f.name}"))
                if isinstance(_f.dataType, StringType):
                    _agg_exprs.append(spark_sum(
                        when(_fc.isNotNull() & _raw.rlike(r"^\s*[\[{]"), lit(1))
                            .otherwise(lit(0))).alias(f"b::{_f.name}"))
                # 11/17/2026 boss-mode NULL hygiene: per-field non-null count
                # — powers all-NULL-column detection below.
                _agg_exprs.append(spark_sum(
                    when(_fc.isNotNull(), lit(1))
                        .otherwise(lit(0))).alias(f"n::{_f.name}"))
            _row = _scan.agg(*_agg_exprs).first() if _agg_exprs else None
            _den = max(valid, 1)
            _tc = {}
            if _row:
                for _k, _v in _row.asDict().items():
                    _sig, _name = _k.split("::", 1)
                    if _sig not in ("a", "b"):
                        continue   # n:: = non-null counters, consumed below
                    if _v and (_v / _den >= 0.005):
                        _tc[_name] = _tc.get(_name, 0) + int(_v)
            if _tc:
                _top = ", ".join(f"{_k}:{_v}" for _k, _v in
                                 sorted(_tc.items(), key=lambda x: -x[1])[:6])
                print(f"  ⚠️ TYPE CONFLICT in {bronze_table} — key-in-raw-but-NULL "
                      f"/ stringified struct-array: {_top} (of {_den:,} parsed) — "
                      f"field(s) may be degrading to text; child extraction at risk")
                _tc_note = f"type-conflict suspects: {_top}"
            # --- 11/17/2026 boss-mode — SOURCE-SIDE NULL HYGIENE --------------
            # (a) all-NULL columns: a field that is NULL in EVERY row of the
            #     batch carries no data and its type is unverifiable — pure
            #     bloat, but the grow-only registry keeps it forever. Surface
            #     it so a human can decide (it may be a NEW field in an early
            #     sparse batch — check the run-log field-count trend).
            # (b) empty payloads: rows that PARSE but have not one non-null
            #     field — the source-side full-NULL-row class (e.g. '{}'-ish
            #     or all-null dumps). They write to silver as empty rows on
            #     unpinned/child tables; count them so the loss is visible.
            if _row:
                _dead_cols = [_f.name for _f in _fields
                              if not int(_row[f"n::{_f.name}"] or 0)]
                if _dead_cols:
                    print(f"  ⚪ {bronze_table}: {len(_dead_cols)} all-NULL column(s): "
                          f"{_dead_cols[:6]}{' …' if len(_dead_cols) > 6 else ''} — "
                          f"carry no data (unverifiable type)")
                    _tc_note = (f"{_tc_note + ' | ' if _tc_note else ''}"
                                f"all-NULL columns: {len(_dead_cols)}")
            try:
                from pyspark.sql.functions import (
                    array as _parr, size as _psz, filter as _pflt)
                _nonnull = _parr(*[col("parsed").getField(_f.name)
                                   for _f in _fields])
                _empty_n = _scan.filter(
                    _psz(_pflt(_nonnull, lambda _x: _x.isNotNull())) == 0).count()
                if _empty_n:
                    print(f"  ⚪ {bronze_table}: {_empty_n:,} empty payload(s) — "
                          f"parsed but every field NULL (source-side NULL rows)")
                    _tc_note = (f"{_tc_note + ' | ' if _tc_note else ''}"
                                f"empty payloads: {_empty_n}")
            except Exception:
                pass
            if _quarantine_degraded:
                _tc_note = (f"{_tc_note + ' | ' if _tc_note else ''}"
                            "quarantine DEGRADED — unparsed rows kept in output")
        except Exception as _tce:
            print(f"  ℹ️ type-conflict scan skipped ({str(_tce)[:60]})")

    # All-NULL field validation — catches silent from_json schema mismatches.
    # A full table where EVERY field is NULL on EVERY row means the schema is wrong
    # (wrong type, field name mismatch, UDF returned unexpected shape).
    # Individual rows being all NULL or individual columns being all NULL is legitimate —
    # only a full table of all-NULL indicates a problem and should prevent the write
    # so previous silver data is preserved instead of being overwritten with garbage.
    parsed_type = df_parsed.schema["parsed"].dataType
    _check_type = parsed_type.elementType if isinstance(parsed_type, ArrayType) else parsed_type
    if isinstance(_check_type, StructType) and len(_check_type.fields) > 0:
        # For ArrayType, explode first so we check element fields, not the array container
        if isinstance(parsed_type, ArrayType):
            _df_check = df_parsed.withColumn("_element", explode_outer(col("parsed"))).filter(col("_element").isNotNull())
            _field_ref = lambda f: col(f"_element.`{f.name}`")
        else:
            _df_check = df_parsed.filter(col("parsed").isNotNull())
            _field_ref = lambda f: col(f"parsed.`{f.name}`")
        _null_counts = _df_check.select(*[
            spark_sum(when(_field_ref(f).isNotNull(), 1).otherwise(0)).alias(f"_nc_{i}")
            for i, f in enumerate(_check_type.fields)
        ]).collect()[0]
        _all_null = all(_null_counts[f"_nc_{i}"] == 0 for i, f in enumerate(_check_type.fields))
        if _all_null:
            print(f"  ⛔ ABORT: All {len(_check_type.fields)} fields are NULL across {valid:,} rows — schema mismatch")
            print(f"  ⛔ Previous silver data preserved (write skipped)")
            _run_log_write(bronze_table, silver_name or bronze_table,
                           "CDC" if watermark else "FULL",
                           raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                           repair_needed=_needs_repair,
                           discovery_mode=("FULL" if _SCHEMA_SAMPLE is None else f"SAMPLE {_SCHEMA_SAMPLE}"),
                           fields_discovered=len(_check_type.fields),
                           parsed_valid=valid, parsed_null=_quarantined_n,
                           quarantined_new=_new_q_n,
                           note="all fields NULL — schema mismatch, write skipped", rung_stats=_rung_stats)
            return None
        _populated = sum(1 for i, f in enumerate(_check_type.fields) if _null_counts[f"_nc_{i}"] > 0)
        print(f"  ✓ Field health: {_populated}/{len(_check_type.fields)} fields have data")

    # Handle top-level type: struct → star-expand, array → explode first
    if isinstance(parsed_type, StructType):
        field_names = _dedup_names([_sanitize_col_name(f.name) for f in parsed_type.fields])
        select_exprs = [col("insert_date").alias("bronze_insert_date"), col("bronze_updated_at"), col("payload_hash")]
        for f, safe_name in zip(parsed_type.fields, field_names):
            select_exprs.append(col(f"parsed.`{f.name}`").alias(safe_name))
        df_flat = df_parsed.select(*select_exprs)
    elif isinstance(parsed_type, ArrayType):
        print(f"  🔍 Top-level JSON is an array — exploding elements")
        df_exploded = df_parsed.withColumn("_element", explode_outer(col("parsed")))
        element_type = df_exploded.schema["_element"].dataType
        if isinstance(element_type, StructType):
            elem_names = _dedup_names([_sanitize_col_name(f.name) for f in element_type.fields])
            select_exprs = [col("insert_date").alias("bronze_insert_date"), col("bronze_updated_at"), col("payload_hash")]
            for f, safe_name in zip(element_type.fields, elem_names):
                select_exprs.append(col(f"_element.`{f.name}`").alias(safe_name))
            df_flat = df_exploded.select(*select_exprs)
        else:
            df_flat = df_exploded.select(
                col("insert_date").alias("bronze_insert_date"),
                col("bronze_updated_at"),
                col("payload_hash"),
                col("_element").alias("value")
            )
    else:
        df_flat = df_parsed.select(
            col("insert_date").alias("bronze_insert_date"),
            col("bronze_updated_at"),
            col("payload_hash"),
            col("parsed").alias("value")
        )
    # 11/17/2026 — run-log row for the successful parse (fleet telemetry;
    # see _run_log_write). Volume-anomaly and convergence alerting read this.
    _run_log_write(bronze_table, silver_name or bronze_table,
                   "CDC" if watermark else "FULL",
                   raw_rows=_raw_total, deduped_rows=_dedup_total, cdc_rows=_after,
                   repair_needed=_needs_repair,
                   discovery_mode=("FULL" if _SCHEMA_SAMPLE is None else f"SAMPLE {_SCHEMA_SAMPLE}"),
                   fields_discovered=(len(deduped_st.fields) if isinstance(deduped_st, StructType) else None),
                   parsed_valid=valid, parsed_null=_quarantined_n, quarantined_new=_new_q_n,
                   rung_stats=_rung_stats, note=_tc_note)
    return df_flat

# COMMAND ----------

# DBTITLE 1,# Field Utilities
# ============================================================================
# FIELD UTILITIES 11/17/2026
# ============================================================================


def categorize_fields(df):
    """Return (scalars, arrays, structs) from DataFrame schema."""
    scalars, arrays, structs = [], [], []
    for f in df.schema.fields:
        if isinstance(f.dataType, ArrayType):
            arrays.append(f.name)
        elif isinstance(f.dataType, StructType):
            structs.append(f.name)
        else:
            scalars.append(f.name)
    return scalars, arrays, structs


def _sanitize_col_name(name):
    """Sanitize: only alphanumeric and underscores allowed, everything else -> _."""
    safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    safe = re.sub(r'_{2,}', '_', safe)  # collapse consecutive underscores
    return safe.strip('_') if safe.strip('_') else 'unnamed'


def _dedup_names(names):
    """Ensure all names are unique by appending _1, _2, etc. for duplicates.
    11/17/2026 — CASE-INSENSITIVE: 'id' and 'ID' (the same field re-sent
    under different casing by the source, both surviving sanitize) used to
    both keep the name 'id' — two identical column names in one DataFrame,
    with downstream writes resolving them arbitrarily. Collisions are now
    detected across case; the first casing wins, later casings get the
    _1/_2 suffix."""
    seen = {}
    seen_lower = {}
    result = []
    for name in names:
        _low = name.lower()
        if name in seen:
            seen[name] += 1
            result.append(f"{name}_{seen[name]}")
        elif _low in seen_lower:
            # 11/17/2026 — same name under a different casing
            seen_lower[_low] += 1
            result.append(f"{name}_{seen_lower[_low]}")
        else:
            seen[name] = 0
            seen_lower[_low] = 0
            result.append(name)
    return result


def flatten_structs(df, skip_custom=True):
    """Flatten StructType columns into individual columns.
    If skip_custom=True, the 'custom' struct is kept for separate extraction."""
    _, _, structs = categorize_fields(df)
    iterations = 0
    while structs and iterations < 10:
        for s_col in structs:
            if skip_custom and s_col == "custom":
                continue
            struct_type = df.schema[s_col].dataType
            new_cols = [col(f"{s_col}.`{f.name}`").alias(_sanitize_col_name(f"{s_col}_{f.name}"))
                        for f in struct_type.fields]
            other_cols = [c for c in df.columns if c != s_col]
            df = df.select(*other_cols, *new_cols)
        _, _, structs = categorize_fields(df)
        if skip_custom and set(structs) <= {"custom"}:
            break
        iterations += 1
    # 11/17/2026 — cap warning: hitting the 10-iteration cap with structs
    # still remaining means nesting deeper than the flattener will go.
    # Previously silent — the leftover struct columns just appeared in
    # silver with no hint why. Only fires when non-custom structs remain.
    if iterations >= 10 and structs:
        _left = [s for s in structs if not (skip_custom and s == "custom")]
        if _left:
            print(f"  ⚠️ flatten_structs: iteration cap hit with nested structs remaining ({_left}) — they are left nested")
    return df


def find_pk(df, table_name=None, validate=True, sample_size=10000):
    """Find the best candidate for a primary key column.

    Detection priority (first match wins):
      1. Exact 'id' column (case-insensitive) — Close CRM convention, works on all 9 source tables
      2. '{table_name}_id' or '{table_name}Id' pattern — common ORM convention
      3. Any column ending in '_id' — fallback for non-standard schemas

    If validate=True, checks that the candidate has no NULLs and is unique
    on a sample of rows. If the top candidate fails validation, tries the
    next candidate. Returns None if no valid PK is found.

    Args:
        df:          DataFrame to search
        table_name:  Silver table name (used for pattern matching in pass 2)
        validate:    If True, verify non-null + unique on a sample
        sample_size: Row sample for uniqueness validation (default 10K)
    """
    PK_TYPES = (StringType, IntegerType, LongType)

    # --- Collect candidates in priority order ---
    candidates = []
    for f in df.schema.fields:
        if not isinstance(f.dataType, PK_TYPES):
            continue
        lower = f.name.lower()
        if lower == "id":
            candidates.append((1, f.name))
        elif table_name and lower == f"{table_name}_id":
            candidates.append((2, f.name))
        elif lower.endswith("_id"):
            candidates.append((3, f.name))

    # Sort by priority (1 > 2 > 3), preserving schema order within each tier
    candidates.sort(key=lambda c: c[0])

    if not candidates:
        return None

    # --- Fast path: no validation, return first candidate ---
    if not validate:
        return candidates[0][1]

    # --- Validate each candidate: unique on a sample ---
    # 11/17/2026 — NULL-TOLERANT: the old check required non_null == total
    # (NO nulls anywhere in the sample), so a PK column with even one
    # late-arriving null row was rejected outright. NULLs are not
    # duplicates — uniqueness is now judged among NON-NULL values only
    # (non_null == distinct). Null-key rows are handled safely downstream:
    # silver writing treats them via the delete+append path, never as
    # MERGE keys.
    sample = df.limit(sample_size)

    for priority, col_name in candidates:
        non_null = sample.filter(col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        # 11/17/2026 — distinct over NON-NULL values only: an unfiltered
        # distinct counts NULL once, so non_null == distinct was impossible
        # while any NULL exists (uniqueness never passed — the exact rows
        # NULL-tolerance was meant to rescue) yet could accidentally pass
        # when duplicates hid behind a NULL row.
        distinct = sample.filter(col(col_name).isNotNull()).select(col_name).distinct().count()
        if non_null == distinct:
            return col_name

    # 11/17/2026 — NO LAST RESORT: the old fallback returned the first
    # candidate even after it FAILED validation — a wrong PK is worse than
    # no PK (a false 'id' pin silently collapses the version chain in the
    # silver MERGE — the exact failure the Nov 9 pinned keys were added to
    # avoid). Return None: _parent_pk then falls back to the pinned
    # SILVER_MERGE_KEYS, and the caller logs that no PK was found.
    return None


# --- 11/18/2026 CORPUS PIN: registry union must never duplicate a field ---
# Incident (13:00 nightly — first real CDC batch after the rebuild, 12,001
# rows): _registry_union compared the registry's RAW field name against the
# discovery's SANITIZED names, so any field whose raw name changes under
# _sanitize_col_name (_type -> type, custom.cf_* -> custom_cf_*) was always
# judged "missing" and re-added as a DUPLICATE — from_json parsed a struct
# with the field twice and the field-health guard crashed
# AMBIGUOUS_REFERENCE_TO_FIELDS ("_type" appears 2 times). Registry evidence:
# _type held 8 rows vs baseline 5 from earlier silent double-writes. Lives
# here, not in the corpus cell, because it needs _registry_union (cell 5)
# and _sanitize_col_name (this cell) to both exist at import time.
_st_pin = StructType().add("_type", StringType(), True).add("name", StringType(), True)
_u_pin = _registry_union("PIN_union_regression", _st_pin,
                         _prior_override={"_type": "string", "only_in_registry": "string"})
_pin_names = [f.name for f in _u_pin.fields]
assert _pin_names.count("_type") == 1, (
    f"_registry_union re-added _type ({_pin_names.count('_type')} occurrences) "
    "— the raw-vs-sanitized name comparison regressed")
assert "only_in_registry" in _pin_names, "grow-only registry re-add lost"
print("  PASS  11/18: _registry_union no-duplicate (raw vs sanitized names)")

# COMMAND ----------

# DBTITLE 1,# Silver Writing
# ============================================================================
# SILVER WRITING 11/17/2026
# ============================================================================

# Pinned merge keys for CDC upserts — the one-row-per-id silver contract
# (Nov 9 2026). Tables listed here upsert by PK on CDC batches (MERGE:
# existing ids UPDATE in place, new ids INSERT, so the row count tracks the
# true population instead of growing with every changed record). Tables NOT
# listed keep the legacy delete+append behavior. Keys are the pinned
# per-family PKs from 2_Silver/rebuild_silver_parents_latest_per_id;
# children get their entries in the child-rebuild step.
SILVER_MERGE_KEYS = {
    "leads_processed":                      "id",
    "lead_activites_processed":              "id",
    "close_crm_users_processed":             "id",
    "custom_activites_processed":            "id",
    "calendly_scheduled_events_processed":   "EVENT_URI",
    "student_sentiment_processed":           "CHANNEL_ID",
    "mdl_users_processed":                   "ID",
}

# 11/17/2026 — explicit allowlist: columns a full overwrite MAY drop.
# The overwrite path refuses to remove any target column not in this set
# (see the column-drop guard in write_silver). Intentional column removals
# go here; everything else fails loud.
_OVERWRITE_DROP_ALLOWLIST = set()


def _parent_pk(df, silver_name):
    """Pinned per-family PK first (SILVER_MERGE_KEYS — the same pins the
    Nov 9 rebuild notebooks used), find_pk as fallback. find_pk alone can
    pick the wrong key or none at all: lead_activites validation can fail
    on 'id' and hand back lead_id (collapsing the version chain per LEAD,
    not per activity), and calendly has no 'id'-shaped column at all
    (EVENT_URI is never detected). The pin is the provenance of the
    one-row-per-id contract — always prefer it when present in the parsed
    columns."""
    _pin = SILVER_MERGE_KEYS.get(silver_name)
    if _pin:
        if _pin in df.columns:
            return _pin
        # 11/17/2026 — pinned key MISSING from the parsed columns: a
        # discovery/parse failure on this batch, NOT a hint to guess a
        # different key. The old silent find_pk fallback could pick a
        # WRONG key (e.g. lead_id instead of id — collapsing the version
        # chain per LEAD instead of per activity). Raise loud so the run
        # stops instead of silently corrupting the merge contract.
        raise ValueError(
            f"{silver_name}: pinned merge key '{_pin}' missing from parsed "
            f"columns — refusing to guess a different PK. Fix discovery or "
            f"update SILVER_MERGE_KEYS.")
    return find_pk(df, table_name=silver_name)


def _is_nested_schema_mismatch(e):
    """True when a CDC write died on a nested-schema mismatch — the MERGE
    form (DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION, 'Cannot cast struct<...>
    to struct<...>', 'All nested columns must match') or the append form
    (DELTA_INSERT_SCHEMA_MISMATCH / 'Failed to merge incompatible data
    types' — children on the delete+append path). This is the drift
    _merge_silver's top-level ADD COLUMN evolution can never heal: a
    field added/removed/retyped INSIDE an existing column."""
    _s = str(e)
    return ("SCHEMA_MISMATCH" in _s or "Cannot cast struct" in _s
            or "nested columns must match" in _s
            or "Failed to merge incompatible data types" in _s)


# Tables currently mid-full-rebuild — explicit loop guard. Structurally a
# loop cannot happen today (the rebuild's write runs with the watermark
# cleared, i.e. the full-overwrite branch, and overwriteSchema replaces
# the schema so no cast mismatch can re-fire), but this makes the
# invariant hard instead of implied (Nov 12 2026 review).
_REBUILDING = set()


# 11/17/2026 — EVENT-TIME RESOLUTION: bronze's two timestamps are BOTH
# arrival clocks — bronze_insert_date is the source DUMP timestamp (every
# row of one dump shares it) and bronze_updated_at is the bronze LOAD time
# (stamped current_timestamp() in the loader; see the 'bronze load only'
# notebook). The business clock — when the record was last modified AT THE
# SOURCE (Close's date_updated, etc.) — only exists INSIDE the payload as a
# parsed column. Survivorship and MERGE provenance prefer it: a
# late-arriving OLDER version must never win as 'current'. Candidates in
# priority order; first scalar-typed match wins; None → arrival-time
# fallback (printed by the caller).
_EVENT_TIME_CANDIDATES = [
    "date_updated", "updated_at", "dateupdated", "updated",
    "last_modified", "modified_at", "date_modified",
    "last_updated", "lastupdated", "lastmodified",
]


def _find_event_time(df):
    """11/17/2026 — resolve the payload's business event-time column from
    the parsed columns (see _EVENT_TIME_CANDIDATES). Scalar fields only
    (arrays/structs/maps cannot order). String event times order correctly
    lexicographically because Postgres renders them uniformly per column.
    Returns the column name or None."""
    _low = {}
    for _f in df.schema.fields:
        if isinstance(_f.dataType, (ArrayType, StructType)):
            continue
        if _f.dataType.typeName() == "map":
            continue
        _low[_f.name.lower()] = _f.name
    for _c in _EVENT_TIME_CANDIDATES:
        if _c in _low:
            return _low[_c]
    return None


def _latest_per_pk(df, pk):
    """Collapse to the LATEST version per PK — the one-row-per-id silver
    contract (Nov 12 2026 review). parse_bronze_table keeps EVERY distinct
    payload version by design (full history lives in bronze); a FULL
    rebuild that writes all of them puts duplicate ids in silver and the
    next CDC MERGE dies on multiple matches. 11/17/2026 — EVENT-TIME
    SURVIVORSHIP: the ordering is now (payload event time) DESC — the
    business clock resolved by _find_event_time (date_updated /
    updated_at / …) — with arrival (bronze_insert_date) as tiebreak and
    payload_hash last. Both bronze timestamps are ARRIVAL clocks (dump
    time / load time), so the old arrival-first order let a
    late-arriving OLDER version win as 'current' (the stress-taxonomy's
    'biggest hidden flaw', confirmed against the loader). Falls back to
    arrival ordering (printed) when the table has no event-time column.
    NULL-PK rows are kept verbatim (they cannot collide on the ON clause)."""
    if not pk or pk not in df.columns \
            or "bronze_insert_date" not in df.columns:
        return df
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    _et = _find_event_time(df)
    if _et:
        print(f"  🕰️ event-time survivorship via `{_et}` (arrival as tiebreak)")
        _order = [col(f"`{_et}`").desc_nulls_last(),
                  col("bronze_insert_date").desc_nulls_last()]
    else:
        print("  ℹ️ no payload event-time column (date_updated/updated_at/…) — "
              "survivorship by arrival time (bronze_insert_date)")
        _order = [col("bronze_insert_date").desc_nulls_last(),
                  col("bronze_updated_at").desc_nulls_last()]
    if "payload_hash" in df.columns:
        _order.append(col("payload_hash").desc())
    w = Window.partitionBy(pk).orderBy(*_order)
    _before = df.count()
    df = (df.filter(col(pk).isNotNull())
            .withColumn("_rn", row_number().over(w))
            .filter("_rn = 1").drop("_rn")
            .unionByName(df.filter(col(pk).isNull())))
    _after = df.count()
    if _after < _before:
        print(f"  🔁 Latest-per-{pk}: {_before:,} → {_after:,} rows "
              f"({_before - _after:,} older versions collapsed)")
    return df



def _full_rebuild_parent(bronze_name, silver_name, flatten=False):
    """CDC escape hatch (Nov 12 2026): the full-history rebuild the Nov 9
    notebooks did, invoked automatically when the nightly MERGE hits a
    nested schema drift. parse_bronze_table over EVERY distinct bronze row
    (schema_sample=None so rare historical fields survive discovery),
    then the same parent prep the FULL LOAD path runs (flatten ->
    find_pk -> LATEST-per-PK collapse -> extract_custom), then the atomic
    overwrite with overwriteSchema=true. The watermark is cleared FIRST so
    write_silver takes the full-overwrite path — never overwrite a table
    with the CDC batch alone — and restored after, so a multi-table
    notebook keeps normal CDC for the next table. Returns the rebuilt
    parent DataFrame. _REBUILDING guards against re-entry loops."""
    if silver_name in _REBUILDING:
        raise RuntimeError(
            f"rebuild loop detected: {silver_name} is already being rebuilt")
    _REBUILDING.add(silver_name)
    global _CDC_WATERMARK
    _saved_wm = _CDC_WATERMARK
    _CDC_WATERMARK = None
    try:
        df_full = parse_bronze_table(bronze_name, schema_sample=None,
                                     silver_name=silver_name)
        if df_full is None:
            raise RuntimeError(
                f"full rebuild of {silver_name}: no parsable rows in {bronze_name}")
        if flatten:
            df_full = flatten_structs(df_full, skip_custom=True)
        pk = _parent_pk(df_full, silver_name)
        df_full = _latest_per_pk(df_full, pk)
        # 11/17/2026 — payload_hash carried (matches process_table's carry:
        # children stay CDC-dedup-able and traceable to their bronze payload)
        carry = list(dict.fromkeys(
            ["bronze_insert_date", "bronze_updated_at", "payload_hash"] + ([pk] if pk else [])))
        has_custom = ("custom" in df_full.columns
                      and isinstance(df_full.schema["custom"].dataType, StructType))
        has_custom_cf = any(c.startswith("custom_cf_") for c in df_full.columns)
        if has_custom or has_custom_cf:
            _pipeline = PIPELINE_NUMBERS.get(silver_name or bronze_name, 0)
            df_full = extract_custom(df_full, silver_name, carry, f"{_pipeline}_1",
                                     parent_pk=pk, dry_run=True)
        write_silver(df_full, silver_name)
        # 11/17/2026 boss-mode — REBUILD TELEMETRY: rebuilds previously wrote
        # no run-log rows, so a nightly rebuild STORM (nested drift re-firing
        # the escape hatch every run) was invisible. engine_health_report()
        # counts these.
        _run_log_write(bronze_name, silver_name, "REBUILD",
                       note="full rebuild (parent, nested drift escape hatch)")
        # 11/17/2026 — children-stale warning: child tables are built FROM
        # this parent, and a full overwrite can change rows the existing
        # child tables were built from (added/changed/deleted versions).
        # The rebuild itself is correct and atomic — this warning tells
        # the operator to re-run the child build step for this family.
        print(f"  ⚠️ Child tables of {silver_name} may be stale after this "
              f"full rebuild — re-run the child build step for this family")
        return df_full
    finally:
        _REBUILDING.discard(silver_name)
        _CDC_WATERMARK = _saved_wm


def _cast_tree(scol, sdt, tdt):
    """Build a Column expressing a source value (type sdt) as the TARGET type.

    Recursive nested-type reconciliation for CDC MERGE batches:
      struct -> struct  : rebuilt field-by-field; target fields the source
                          lacks become NULL; source fields the target lacks
                          are dropped (nested fields cannot be ALTERed in);
      string -> struct/array/map : re-parsed with from_json (covers the
                          size-limit-degraded discovery, where discovery saw
                          the JSON as text);
      array  -> array   : transformed element-wise by the same rules;
      map    -> map     : round-tripped via to_json + from_json when the
                          source is struct/array, else from_json;
      primitives        : plain CAST.
    """
    from pyspark.sql.functions import transform as _tf, from_json as _fj, \
        lit as _lit, struct as _struct, to_json as _tj
    from pyspark.sql.types import StructType, ArrayType, StringType, MapType

    if isinstance(tdt, StructType):
        if isinstance(sdt, StructType):
            smap = {f.name.lower(): f for f in sdt.fields}
            parts = []
            for tf in tdt.fields:
                if tf.name.lower() in smap:
                    sf = smap[tf.name.lower()]
                    parts.append(_cast_tree(scol.getField(sf.name),
                                            sf.dataType, tf.dataType).alias(tf.name))
                else:
                    parts.append(_lit(None).cast(tf.dataType).alias(tf.name))
            return _struct(*parts)
        return _fj(scol.cast("string"), tdt)
    if isinstance(tdt, ArrayType):
        if isinstance(sdt, ArrayType):
            if isinstance(tdt.elementType, StructType):
                if isinstance(sdt.elementType, StructType):
                    return _tf(scol, lambda x: _cast_tree(x, sdt.elementType, tdt.elementType))
                # degraded array<string> of JSON texts -> array<struct>
                return _tf(scol, lambda x: _fj(x.cast("string"), tdt.elementType))
            return _tf(scol, lambda x: x.cast(tdt.elementType.simpleString()))
        if isinstance(sdt, StringType):
            return _fj(scol.cast("string"), tdt)
        return _lit(None).cast(tdt)
    if isinstance(tdt, MapType):
        if isinstance(sdt, (StructType, ArrayType)):
            return _fj(_tj(scol), tdt)
        return _fj(scol.cast("string"), tdt)
    if isinstance(sdt, (StructType, ArrayType)):
        return _lit(None).cast(tdt)   # nested -> primitive: no safe cast
    return scol.cast(tdt.simpleString())


def _reconcile_to_target(src, tgt_df, table_name):
    """Cast a CDC batch's EXISTING columns to the target table's Delta types.

    Batch schemas come from per-batch sample discovery and drift on nested
    types (addresses degraded to array<string>, contacts missing rare element
    fields). struct/array<struct> types with different fields are NOT
    implicitly castable, so the explicit-column MERGE dies with
    DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION even when the ALTER step added
    every missing TOP-LEVEL column. Columns absent from the target pass
    through untouched (ALTER adds them with the batch type); matched columns
    pass through by name, aliased to the TARGET name.

    Validated Nov 11 2026 on a clone of silver.leads_processed with the exact
    run-99534124159213 failure signature: MERGE succeeds, degraded
    array<string> addresses round-trip to the target struct byte-identical
    (junk elements null-filled), missing contact element fields NULL-filled,
    new top-level columns ALTER-added.
    """
    # 11/17/2026 — case-collision guard: two source columns mapping to ONE
    # target column (e.g. 'ID' and 'id' both matching target 'id') produce a
    # duplicate alias in the SELECT below — an ambiguous-column failure at
    # best, arbitrary column resolution at worst. Refuse loudly so the
    # upstream casing bug is found at its source instead of here.
    _seen_t = {}
    for f in src.schema.fields:
        _tl = f.name.lower()
        if _tl in _seen_t and _seen_t[_tl] != f.name:
            raise ValueError(
                f"{table_name}: source columns '{_seen_t[_tl]}' and "
                f"'{f.name}' both map to target column '{_tl}' — "
                f"case collision, refusing to guess")
        _seen_t[_tl] = f.name
    from pyspark.sql.functions import col
    tgt_fields = {f.name.lower(): f for f in tgt_df.schema.fields}
    adj, sel = [], []
    for f in src.schema.fields:
        tf = tgt_fields.get(f.name.lower())
        if tf is None:
            sel.append(col(f"`{f.name}`"))
        elif f.dataType == tf.dataType:
            sel.append(col(f"`{f.name}`").alias(tf.name))
        else:
            sel.append(_cast_tree(col(f"`{f.name}`"), f.dataType, tf.dataType).alias(tf.name))
            adj.append(f"{f.name}: {f.dataType.simpleString()} -> {tf.dataType.simpleString()}")
    if adj:
        print(f"  🔧 {table_name}: reconciling {len(adj)} drifted column type(s): "
              + "; ".join(adj[:6]) + (" …" if len(adj) > 6 else ""))
        src = src.select(*sel)
    return src


def _append_silver_atomic(df, table_name):
    """11/17/2026 — staged window-replace for child-table CDC writes,
    replacing the legacy delete+append (which LOST the window's rows when
    it failed between the two transactions):
      1. materialize the batch to a per-run staging table
         (_stg_<table>_<_RUN_ID>);
      2. ALTER-add any new batch columns to the target (loud, explicit
         evolution — the old mergeSchema append silently widened the
         table);
      3. DELETE the window (exact microseconds so the DELETE matches
         the parse filter bronze_updated_at > watermark);
      4. load the staged batch into the target in ONE Delta transaction.
    A failure before step 3 leaves the target untouched. A failure in or
    after step 3 keeps the batch safe in staging — the staging table is
    retained and its name printed for recovery (and the next nightly
    re-reads the same window from bronze anyway). Staging is dropped
    only on success.
    Note: a MERGE on payload_hash was considered and rejected — array
    children share one parent payload_hash across many exploded rows, so
    a hash-keyed MERGE would hit multiple-source-row matches; staging is
    the atomicity mechanism instead."""
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    stg = f"{CATALOG}.{SILVER_SCHEMA}._stg_{table_name}_{_RUN_ID}"
    spark.sql(f"DROP TABLE IF EXISTS {stg}")
    # 11/17/2026 — identifier safety, same policy as _merge_silver: column
    # names that will reach DDL (ALTER / INSERT lists) are sanitized first.
    _ren = {c: _safe_ident(c) for c in df.columns if _safe_ident(c) != c}
    if _ren:
        print(f"  ⚠️ sanitized {len(_ren)} column identifier(s) for DDL in {table_name}")
        df = df.toDF(*[_safe_ident(c) for c in df.columns])
    _ok = False
    try:
        df.write.format("delta").mode("overwrite") \
            .option("mergeSchema", "true").saveAsTable(stg)
        # Explicit schema evolution: columns new to this batch become target
        # columns BEFORE the insert — nothing silently dropped or widened.
        _stg_types = {f.name: f.dataType for f in spark.table(stg).schema.fields}
        _tgt_cols = set(spark.table(full).columns)
        for _c, _dt in _stg_types.items():
            if _c not in _tgt_cols:
                spark.sql(f"ALTER TABLE {full} ADD COLUMN `{_c}` {_dt.simpleString()}")
        # Keep microseconds: the DELETE window must exactly match the parse
        # filter (bronze_updated_at > watermark). Truncating to seconds made
        # the DELETE wider than the read, silently dropping the previous
        # batch's newest rows.
        wm_str = _CDC_WATERMARK.strftime("%Y-%m-%d %H:%M:%S.%f")
        spark.sql(f"DELETE FROM {full} WHERE bronze_updated_at > '{wm_str}'")
        _collist = ", ".join(f"`{c}`" for c in spark.table(stg).columns)
        spark.sql(f"INSERT INTO {full} ({_collist}) SELECT {_collist} FROM {stg}")
        _ok = True
    finally:
        if _ok:
            spark.sql(f"DROP TABLE IF EXISTS {stg}")
        else:
            # 11/17/2026 — never drop the staging table on failure: it holds
            # the only materialized copy of this window's batch.
            print(f"  ⚠️ batch preserved in staging for recovery: {stg}")
    return spark.table(full).count()


def _merge_silver(df, table_name, pk):
    """CDC upsert by PK: UPDATE existing ids in place, INSERT new ids.

    Source batch rules:
      * deduped to latest-version-per-PK first (a multi-dump batch holding
        two versions of one id would make MERGE fail with multiple matches);
        11/17/2026 — event-time first: payload event time (date_updated /
        updated_at / … via _find_event_time) DESC, arrival
        (bronze_insert_date) as tiebreak, payload_hash last — a
        late-arriving OLDER version must not win as 'current';
      * NULL-PK rows are kept verbatim (never deduped away): they miss the ON
        match and INSERT; identical payloads are already skipped upstream by
        the payload_hash anti-join, so they only re-insert if a NULL-PK
        payload actually changes;
      * schema evolution: source columns the target lacks are added via
        ALTER TABLE ADD COLUMN before the merge; target columns missing
        from the source keep their values on UPDATE and go NULL on INSERT
        (explicit-column MERGE, not UPDATE SET * / INSERT *).
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, row_number

    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    # Dedupe batch to the latest version per PK (NULL-PK rows kept as-is).
    # 11/17/2026 — event-time survivorship: bronze_insert_date is the source
    # DUMP timestamp and bronze_updated_at the bronze LOAD time (stamped
    # current_timestamp() in the loader) — BOTH are arrival clocks, so the
    # old arrival-first window let a LATE-ARRIVING OLDER version win as
    # 'current'. The payload's business clock (resolved by _find_event_time)
    # is primary now; arrival only breaks ties. _et is reused for the MERGE
    # provenance check below.
    _et = _find_event_time(df)
    if _et:
        w = Window.partitionBy(pk).orderBy(
            col(f"`{_et}`").desc_nulls_last(),
            col("bronze_insert_date").desc_nulls_last(),
            col("payload_hash").desc())
    else:
        w = Window.partitionBy(pk).orderBy(
            col("bronze_insert_date").desc_nulls_last(),
            col("bronze_updated_at").desc_nulls_last(),
            col("payload_hash").desc())
    src = (df.filter(col(pk).isNotNull())
             .withColumn("_rn", row_number().over(w))
             .filter("_rn = 1").drop("_rn")
             .unionByName(df.filter(col(pk).isNull())))

    # Nested-schema reconciliation to the target's Delta types (Nov 11 2026):
    # batch schema comes from per-batch sample discovery and drifts on
    # NESTED types. Nov 11 8AM silver_leads failure (run 99534124159213) was
    # exactly this: addresses discovered as array<string> vs the target's
    # array<struct<...>>, contacts missing rare element fields — and Delta
    # MERGE rejects the explicit-column assignments with
    # DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION (struct types with different
    # fields are not implicitly castable), even though the ALTER step added
    # every missing TOP-LEVEL column. Cast the batch to the target's types
    # BEFORE the ALTER/MERGE below.
    src = _reconcile_to_target(src, spark.table(full), table_name)

    # 11/17/2026 — identifier safety: every column name that reaches DDL
    # (ALTER ADD COLUMN, the MERGE view, the column lists) is sanitized
    # first. A source field containing a backtick or brace breaks the
    # statement; _safe_ident guarantees a valid SQL identifier. Renamed
    # columns are logged so the drift is visible.
    _ren = {c: _safe_ident(c) for c in src.columns if _safe_ident(c) != c}
    if _ren:
        print(f"  ⚠️ sanitized {len(_ren)} column identifier(s) for DDL in {table_name}")
        src = src.toDF(*[_safe_ident(c) for c in src.columns])

    # Schema evolution: add source columns the target lacks (nullable).
    src_cols = [f.name for f in src.schema.fields]
    tgt_cols = set(spark.table(full).columns)
    for f in src.schema.fields:
        if f.name not in tgt_cols:
            spark.sql(f"ALTER TABLE {full} ADD COLUMN `{f.name}` {f.dataType.simpleString()}")

    # Explicit-column MERGE so a schema mismatch between batch and target
    # cannot make the write fail after the ALTER step above.
    # 11/17/2026 — collision guard: the temp view is per-run (_RUN_ID) so
    # two concurrent MERGEs on the same table can no longer drop each
    # other's view mid-merge.
    view = f"_merge_src_{table_name}_{_RUN_ID}"
    src.createOrReplaceTempView(view)
    # 11/17/2026 — provenance preservation: bronze_insert_date is the
    # FIRST-SEEN timestamp. Including it in the SET clause meant every CDC
    # UPDATE overwrote the original insert time with the latest batch's,
    # silently destroying first-seen provenance. Excluded from SET (a
    # matched row keeps its first-seen value) while remaining in the
    # INSERT list so new ids get their true first-seen time.
    _set_cols = [c for c in src_cols if c != "bronze_insert_date"]
    set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in _set_cols)
    ins_cols = ", ".join(f"`{c}`" for c in src_cols)
    ins_vals = ", ".join(f"s.`{c}`" for c in src_cols)
    # 11/17/2026 — event-time provenance: bronze_updated_at is the bronze
    # LOAD time (stamped current_timestamp() in the loader), so comparing
    # it asks 'which copy ARRIVED later' — a late-delivered OLDER business
    # version passed that check and overwrote newer state. When both sides
    # carry a payload event-time column (resolved once above), compare
    # THE BUSINESS CLOCK instead; arrival time stays the fallback for
    # tables with no event-time field.
    if _et and _et in src.columns and _et in spark.table(full).columns:
        _prov = f"s.`{_et}` > t.`{_et}` OR t.`{_et}` IS NULL"
        print(f"  🕰️ {table_name}: event-time provenance via `{_et}`")
    else:
        _prov = ("s.bronze_updated_at > t.bronze_updated_at "
                 "OR t.bronze_updated_at IS NULL")
    try:
        # 11/17/2026 — provenance-preserving: only update a matched row
        # when the batch's copy is actually NEWER on the provenance clock
        # above (or the target's value is NULL). Out-of-order CDC batches
        # (a late delivery of an older dump) previously overwrote newer
        # silver rows with older versions silently. New ids still INSERT;
        # the one-row-per-id contract is untouched.
        spark.sql(f"""
            MERGE INTO {full} AS t
            USING {view} AS s
            ON t.`{pk}` = s.`{pk}`
            WHEN MATCHED AND (
                {_prov}
            ) THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """)
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")


def write_silver(df, table_name, bronze_name=None, flatten=False):
    """Write DataFrame to silver table. CDC mode if _CDC_WATERMARK is set.

    CDC mode: MERGE-by-PK upsert for tables with a SILVER_MERGE_KEYS entry
    (one-row-per-id contract); legacy delete+append otherwise. Full-load mode
    (_CDC_WATERMARK=None): atomic full overwrite (overwriteSchema=true).

    Schema-drift escape hatch (Nov 12 2026): when the MERGE fails on a
    NESTED schema mismatch (drift inside an existing column — the one
    shape top-level ADD COLUMN evolution cannot heal) and bronze_name is
    supplied, fall back to _full_rebuild_parent: a full-history re-parse
    and overwrite of THIS table, so the nightly heals itself instead of
    failing the job. Other errors still propagate, and a mismatch
    without bronze_name (direct callers) propagates too — never overwrite
    a table with the CDC batch alone.

    bronze_name here means THIS table is the parse target of that bronze
    table — PARENTS only. Child tables are protected at the builder level
    (explode_to_child / extract_custom), which re-runs the child's own
    build recipe from its full source; passing bronze_name for a child
    write would rebuild it with parent-shaped rows.
    """
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    # NULL-PK quarantine (Nov 17 2026): pinned-PK silver parents carry NOT NULL
    # on the PK (3_18 contracts). NULL-PK rows in any batch — CDC MERGE, legacy
    # delete+append, or full rebuild — are quarantined to silver.null_pk_quarantine
    # and excluded from the write. Detect + preserve + surface: nothing is dropped
    # or hidden, every offending payload stays queryable, and the nightly stays
    # green instead of dying on DELTA_NOT_NULL_CONSTRAINT_VIOLATED.
    _mk = SILVER_MERGE_KEYS.get(table_name)
    if _mk and _mk in df.columns:
        _nulls = df.filter(col(_mk).isNull())
        _n = _nulls.count()
        if _n:
            from pyspark.sql.functions import (
                struct as _struct, current_timestamp as _ts)
            _qt = f"{CATALOG}.{SILVER_SCHEMA}.null_pk_quarantine"
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {_qt} (
                    table_name         STRING,
                    pk_column          STRING,
                    payload_hash       STRING,
                    bronze_insert_date TIMESTAMP,
                    bronze_updated_at  TIMESTAMP,
                    quarantined_at     TIMESTAMP,
                    row_json           STRING
                )
            """)
            _keys = ["table_name", "pk_column", "payload_hash",
                     "bronze_insert_date"]
            _batch = _nulls.select(
                lit(table_name).alias("table_name"),
                lit(_mk).alias("pk_column"),
                col("payload_hash"),
                col("bronze_insert_date"),
                col("bronze_updated_at"),
                _ts().alias("quarantined_at"),
                to_json(_struct(*[col(c) for c in _nulls.columns]))
                    .alias("row_json"),
            )
            _new = _batch.join(
                spark.table(_qt).select(*_keys), on=_keys, how="left_anti")
            _new_cnt = _new.count()
            if _new_cnt:
                _new.write.format("delta").mode("append").saveAsTable(_qt)
            print(f"  🚫 {table_name}: {_n} NULL-{_mk} row(s) quarantined "
                  f"({_new_cnt} new, {_n - _new_cnt} already known)")
            df = df.filter(col(_mk).isNotNull())

    if _CDC_WATERMARK is not None:
        # Upsert-by-PK when the table has a pinned merge key: changed records
        # UPDATE in place, new records INSERT. Errors PROPAGATE — never fall
        # through to the full overwrite below (it would replace the whole
        # table with just the CDC batch).
        _mk = SILVER_MERGE_KEYS.get(table_name)
        if _mk:
            # 11/17/2026 — pinned merge key MISSING from this batch: a
            # discovery/parse failure, NOT a hint to fall through to the
            # window-replace below (which has no PK contract and would
            # silently break the one-row-per-id guarantee). Raise loudly —
            # mirrors _parent_pk's guard, protecting direct write_silver
            # callers too.
            if _mk not in df.columns:
                raise ValueError(
                    f"{table_name}: pinned merge key '{_mk}' missing from "
                    f"batch — refusing silent fallback to window-replace. "
                    f"Fix discovery or update SILVER_MERGE_KEYS.")
            try:
                _merge_silver(df, table_name, _mk)
                cnt = spark.table(full).count()
                print(f"  ✅ CDC MERGE: {table_name}: {cnt:,} rows (upsert by {_mk})")
                return cnt
            except Exception as e:
                if not _is_nested_schema_mismatch(e) or bronze_name is None:
                    raise
                print(f"  ⚠️ CDC MERGE blocked by nested schema drift in {table_name}")
                print(f"     {str(e).splitlines()[0][:200]}")
                print(f"  🔄 Falling back to FULL REBUILD of {table_name} from "
                      f"{bronze_name} (full history + overwriteSchema)")
                _full_rebuild_parent(bronze_name, table_name, flatten=flatten)
                cnt = spark.table(full).count()
                print(f"  ✅ REBUILT: {table_name}: {cnt:,} rows (full overwrite)")
                return cnt
        # Legacy delete+append for child tables without a pinned merge key.
        # Only the existence check may fall through — a table that doesn't
        # exist yet needs its first full load via the overwrite below. A
        # failure of the DELETE or the append itself PROPAGATES: falling
        # through would replace the whole table with just the CDC batch
        # (silent data loss — this exact fallthrough wiped
        # leads_processed_contacts on Nov 10, 2026).
        try:
            spark.sql(f"DESCRIBE TABLE {full}")
        except Exception:
            pass   # table doesn't exist yet → first full load below
        else:
            # 11/17/2026 — legacy delete+append replaced by the staged
            # window-replace (_append_silver_atomic): the batch is safe in
            # staging BEFORE the window DELETE runs, and the load itself is
            # one INSERT transaction. The old form lost the window's rows
            # when the write failed between the DELETE and the append.
            _append_silver_atomic(df, table_name)
            cnt = spark.table(full).count()
            print(f"  ✅ CDC: {table_name}: {cnt:,} rows (staged window replace)")
            return cnt

    # 11/17/2026 — column-drop guard on the overwrite path: overwriteSchema
    # replaces the target schema wholesale, so a regressed discovery would
    # silently DROP target columns (the Nov 10 'SMS Opt-Out' near-miss was
    # only caught because a pinned Delta stats property happened to reject
    # the drop — luck, not design). If the target exists and this batch
    # would remove columns not in _OVERWRITE_DROP_ALLOWLIST, refuse the
    # write. Growing the schema is fine and is announced.
    if spark.catalog.tableExists(full):
        _tgt_cols = set(spark.table(full).columns)
        _drop = _tgt_cols - set(df.columns) - _OVERWRITE_DROP_ALLOWLIST
        if _drop:
            raise ValueError(
                f"{table_name}: full overwrite would DROP {len(_drop)} existing "
                f"column(s): {sorted(_drop)[:8]}{' …' if len(_drop) > 8 else ''} — "
                f"refusing (silent schema regression). If the removal is "
                f"intentional, add the column(s) to _OVERWRITE_DROP_ALLOWLIST "
                f"and re-run.")
        _added = set(df.columns) - _tgt_cols
        if _added:
            print(f"  ➕ {table_name}: overwrite adds {len(_added)} column(s): "
                  f"{sorted(_added)[:8]}{' …' if len(_added) > 8 else ''}")
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .saveAsTable(full)
    cnt = spark.table(full).count()
    print(f"  ✅ {table_name}: {cnt:,} rows")
    return cnt



# COMMAND ----------

# DBTITLE 1,# Array Explosion & Child CDC
# ============================================================================
# ARRAY EXPLOSION 11/17/2026 (dry_run mode — prints schema without writing tables)
# ============================================================================


def explode_to_child(df, parent_name, array_col, carry_cols, depth=0, num_path="", parent_pk=None, dry_run=False, recurse=True, bronze_name=None, _rebuilding=False):
    """Explode an array column into a child table. Recurse on nested arrays until scalar.

    When dry_run=True: prints the child table name and columns without writing.

    Nov 12 2026 child escape hatch: when dry_run=False and the child write
    fails on a nested schema mismatch, this rebuilds the child from the
    FULL current source (silver parent re-explode by default; full bronze
    re-parse when bronze_name is given) with the watermark cleared, so
    the child heals itself instead of failing the notebook — see
    _full_rebuild_child_from_source below. _rebuilding is the recursion
    guard: a rebuild that fails again propagates.
    """
    child_name = f"{parent_name}_{array_col}"

    df_child = df.withColumn("_element", explode_outer(col(array_col)))
    # 11/17/2026 — ghost-child guard: explode_outer keeps NULL elements
    # (absent/empty arrays) as all-NULL child rows — no data, just bloat,
    # and they mislead child-table counts. Skip-and-count: report them,
    # drop them (the parent row itself is never lost — it lives in the
    # parent table, and unparsable payloads are quarantined upstream).
    _ghosts = df_child.filter(col("_element").isNull()).count()
    if _ghosts:
        print(f"  ⛔ {child_name}: skipping {_ghosts:,} NULL-element ghost row(s) (absent/empty array)")
        df_child = df_child.filter(col("_element").isNotNull())
    element_type = df_child.schema["_element"].dataType

    if isinstance(element_type, StructType):
        element_field_names = [f.name for f in element_type.fields]
        elem_names = _dedup_names([_sanitize_col_name(f.name) for f in element_type.fields])
        # Detect element's own PK (for recursion into grandchild tables)
        element_pk = None
        for f, safe_name in zip(element_type.fields, elem_names):
            if safe_name.lower() == "id" and isinstance(f.dataType, (StringType, IntegerType, LongType)):
                element_pk = safe_name
                break
        carry = []
        for c in dict.fromkeys(carry_cols):
            if c in df.columns and c != array_col:
                if c in element_field_names or c == parent_pk:
                    carry.append(col(c).alias(f"{parent_name}_{c}"))
                else:
                    carry.append(col(c))
        element_fields = [col(f"_element.`{f.name}`").alias(safe) for f, safe in zip(element_type.fields, elem_names)]
        df_child = df_child.select(*carry, *element_fields)
    else:
        element_pk = None
        carry = []
        for c in dict.fromkeys(carry_cols):
            if c in df.columns and c != array_col:
                if c == parent_pk:
                    carry.append(col(c).alias(f"{parent_name}_{c}"))
                else:
                    carry.append(col(c))
        df_child = df_child.select(*carry, col("_element").alias(array_col))

    # Flatten any structs from the explosion (keep custom for separate extraction)
    df_child = flatten_structs(df_child, skip_custom=True)

    if dry_run:
        indent = "    " * (depth + 1)
        print(f"{indent}📦 {child_name} ({len(df_child.columns)} cols): {df_child.columns}")
    else:
        try:
            write_silver(df_child, child_name)
        except Exception as e:
            if not _is_nested_schema_mismatch(e) or _rebuilding:
                raise
            print(f"  ⚠️ child write blocked by nested schema drift in {child_name} — full rebuild from source")
            print(f"     {str(e).splitlines()[0][:200]}")
            _full_rebuild_child_from_source(parent_name, array_col, carry_cols, depth, num_path, parent_pk, recurse, bronze_name)

    _lineage_tree.append((num_path, depth + 1, child_name))

    # Build carry for downstream tables
    child_pk = find_pk(df_child)
    child_carry = list(dict.fromkeys(
        [c for c in carry_cols if c in df_child.columns] +
        [c for c in df_child.columns if c.startswith(f"{parent_name}_")] +
        ([child_pk] if child_pk else [])
    ))
    has_child_custom = "custom" in df_child.columns and isinstance(df_child.schema["custom"].dataType, StructType)
    has_child_cf = any(c.startswith("custom_cf_") for c in df_child.columns)
    if has_child_custom or has_child_cf:
        custom_num = f"{num_path}_0"
        df_child = extract_custom(df_child, child_name, child_carry, custom_num, parent_pk=element_pk, dry_run=dry_run)

    # Recurse on nested arrays
    _, child_arrays, _ = categorize_fields(df_child)
    pk = find_pk(df_child)
    new_carry = list(set(
        [c for c in carry_cols if c in df_child.columns] +
        [c for c in df_child.columns if c.startswith(f"{parent_name}_")] +
        ([pk] if pk else [])
    ))

    # recurse=False: write only this child table, skip nested-array grandchildren
    # (used by 2_1_03_01 so contacts phones/urls/integration_links are not rebuilt;
    #  the emails grandchild is maintained daily by 2_1_03_02)
    if not recurse:
        return

    grandchild_idx = 0
    for ca in child_arrays:
        grandchild_idx += 1
        grandchild_num = f"{num_path}_{grandchild_idx}"
        explode_to_child(df_child, child_name, ca, new_carry, depth + 1, grandchild_num, parent_pk=element_pk, dry_run=dry_run)


def _full_rebuild_child_from_source(parent_name, array_col, carry_cols, depth, num_path, parent_pk, recurse, bronze_name):
    """Child-table CDC escape hatch (Nov 12 2026): when a child's write
    fails on nested schema drift, rebuild that child — and, when recurse,
    its descendants — from the FULL current source, overwriting, the way
    the Nov 9 child rebuilds did. Two source styles, mirroring the two
    child-notebook patterns:
      - silver-parent-derived (default, e.g. 2_1_03_01 / 2_4_02): re-read
        the FULL silver parent table and re-explode it. Valid because a
        child can never be newer than its parent.
      - bronze-derived (bronze_name given): re-parse FULL bronze history
        (schema_sample=None so rare fields survive discovery) + the
        standard prep (flatten -> find_pk -> carry), then re-explode.
    The watermark is cleared FIRST so the rebuild's writes take the full
    overwrite path, and restored after. A failure inside the rebuild
    propagates (_rebuilding guard — never loop)."""
    global _CDC_WATERMARK
    _saved = _CDC_WATERMARK
    _CDC_WATERMARK = None
    try:
        if bronze_name:
            src = parse_bronze_table(bronze_name, schema_sample=None,
                                     silver_name=parent_name)
            if src is None:
                raise RuntimeError(
                    f"child rebuild: no parsable rows in {bronze_name}")
            # 11/17/2026 boss-mode — REBUILD TELEMETRY (attempt-level: even a
            # FAILED child rebuild is churn the health report must see).
            _run_log_write(bronze_name, parent_name, "REBUILD",
                           note=f"child rebuild ({array_col}, nested drift escape hatch)")
            src = flatten_structs(src, skip_custom=True)
            _pk = _parent_pk(src, parent_name)
            # one row per id before re-exploding — same window as _merge_silver;
            # without it every historical version of a parent row explodes
            # into duplicate child rows
            src = _latest_per_pk(src, _pk)
            # 11/17/2026 — payload_hash carried (matches process_table's
            # carry: rebuilt children stay CDC-dedup-able + traceable)
            _carry = list(dict.fromkeys(
                ["bronze_insert_date", "bronze_updated_at", "payload_hash"] + ([_pk] if _pk else [])))
        else:
            src = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{parent_name}")
            _carry = list(dict.fromkeys([c for c in carry_cols if c in src.columns]))
        print(f"  🔄 re-exploding {array_col} from full "
              f"{'bronze ' + bronze_name if bronze_name else 'silver parent ' + parent_name}")
        explode_to_child(src, parent_name, array_col, _carry, depth=depth,
                          num_path=num_path, parent_pk=parent_pk,
                          dry_run=False, recurse=recurse,
                          bronze_name=bronze_name, _rebuilding=True)
    finally:
        _CDC_WATERMARK = _saved

# COMMAND ----------

# DBTITLE 1,# Custom Field Extraction
# ============================================================================
# CUSTOM FIELD EXTRACTION 11/17/2026 — custom_fields MAP + _custom table (no child tables)
# ============================================================================


def extract_custom(df, parent_name, carry_cols, num_path, parent_pk=None, dry_run=False, bronze_name=None, _rebuilding=False):
    """Extract custom struct AND custom_cf_ columns.

    - cf_ fields (inside custom struct OR as top-level custom_cf_ columns) → custom_fields MAP
    - Non-cf_ scalar fields → _custom child table (one table, no further children)
    - Non-cf_ array fields → array columns in _custom table (preserved for future use, NOT exploded)
    The custom_fields MAP is always built on the parent regardless of dry_run.

    Nov 12 2026 child escape hatch: when dry_run=False, bronze_name is
    given (the source bronze table), and the _custom child write fails on
    a nested schema mismatch, this rebuilds the child by re-parsing FULL
    bronze history and re-running the extraction with the watermark
    cleared (full overwrite) — see _full_rebuild_custom_child below. The
    raw custom struct only exists in bronze, so there is no
    silver-parent fallback for custom children. _rebuilding is the
    recursion guard: a rebuild that fails again propagates.
    """
    # Detect cf_ from TWO sources:
    top_cf_cols = [c for c in df.columns if c.startswith("custom_cf_")]
    has_custom_struct = "custom" in df.columns and isinstance(df.schema["custom"].dataType, StructType)

    if not has_custom_struct and not top_cf_cols:
        return df

    carry = []
    for c in dict.fromkeys(carry_cols):
        if c in df.columns:
            if c == parent_pk:
                carry.append(col(c).alias(f"{parent_name}_{c}"))
            else:
                carry.append(col(c))

    struct_cf_fields = []
    struct_scalar_fields = []
    struct_array_fields = []

    if has_custom_struct:
        custom_type = df.schema["custom"].dataType
        for f in custom_type.fields:
            if f.name.startswith("cf_"):
                struct_cf_fields.append(f.name)
            elif isinstance(f.dataType, ArrayType):
                struct_array_fields.append((f.name, _sanitize_col_name(f.name)))
            else:
                struct_scalar_fields.append((f.name, _sanitize_col_name(f.name)))

    custom_name = f"{parent_name}_custom"

    # -- 1. Build _custom table (scalars as columns, arrays as array columns) --
    # No child tables are ever created from fields inside custom.
    # Arrays stay as array columns for future use if needed.
    if has_custom_struct and (struct_scalar_fields or struct_array_fields):
        select_exprs = list(carry) + [
            col(f"custom.`{orig}`").alias(safe) for orig, safe in struct_scalar_fields
        ] + [
            col(f"custom.`{orig}`").alias(safe) for orig, safe in struct_array_fields
        ]
        df_custom = df.select(*select_exprs)
        df_custom = flatten_structs(df_custom, skip_custom=False)

        if dry_run:
            print(f"    📦 {custom_name} ({len(df_custom.columns)} cols): {df_custom.columns}")
        else:
            print(f"\n  📦 CUSTOM TABLE — {custom_name}")
            try:
                write_silver(df_custom, custom_name)
            except Exception as e:
                if not _is_nested_schema_mismatch(e) or _rebuilding or bronze_name is None:
                    raise
                print(f"  ⚠️ custom child write blocked by nested schema drift in {custom_name} — full rebuild from bronze")
                print(f"     {str(e).splitlines()[0][:200]}")
                return _full_rebuild_custom_child(bronze_name, parent_name, num_path, parent_pk)
        _lineage_tree.append((num_path, 1, custom_name))

    # -- 2. Build custom_fields MAP column from ALL cf_ fields --
    map_entries = []

    for c in top_cf_cols:
        cf_key = c.replace("custom_cf_", "cf_", 1)
        if isinstance(df.schema[c].dataType, ArrayType):
            map_entries.extend([lit(cf_key), to_json(col(c))])
        else:
            map_entries.extend([lit(cf_key), col(c).cast("string")])

    if has_custom_struct:
        for f_name in struct_cf_fields:
            field_type = df.schema["custom"].dataType[f_name].dataType
            if isinstance(field_type, ArrayType):
                map_entries.extend([lit(f_name), to_json(col(f"custom.`{f_name}`"))])
            else:
                map_entries.extend([lit(f_name), col(f"custom.`{f_name}`").cast("string")])

    if map_entries:
        df = df.withColumn("custom_fields", create_map(*map_entries))
        print(f"  📝 custom_fields MAP — {len(map_entries) // 2} cf_ keys added to parent")

    # -- Drop everything custom-related from parent --
    cols_to_drop = list(top_cf_cols)
    if has_custom_struct:
        cols_to_drop.append("custom")
    for c in cols_to_drop:
        if c in df.columns:
            df = df.drop(c)

    return df


def _full_rebuild_custom_child(bronze_name, parent_name, num_path, parent_pk):
    """Custom-child CDC escape hatch (Nov 12 2026): the raw custom struct
    only exists in bronze — silver parents replace it with the
    custom_fields MAP — so a _custom child can only be rebuilt by
    re-parsing FULL bronze history (schema_sample=None so rare fields
    survive discovery) and re-running the extraction with the watermark
    cleared (full-overwrite write). Returns the mapped parent df built
    from full history. A failure inside the rebuild propagates
    (_rebuilding guard — never loop)."""
    global _CDC_WATERMARK
    _saved = _CDC_WATERMARK
    _CDC_WATERMARK = None
    try:
        df_full = parse_bronze_table(bronze_name, schema_sample=None,
                                     silver_name=parent_name)
        if df_full is None:
            raise RuntimeError(
                f"custom child rebuild: no parsable rows in {bronze_name}")
        # 11/17/2026 boss-mode — REBUILD TELEMETRY (attempt-level: even a
        # FAILED custom-child rebuild is churn the health report must see).
        _run_log_write(bronze_name, parent_name, "REBUILD",
                       note="custom child rebuild (nested drift escape hatch)")
        df_full = flatten_structs(df_full, skip_custom=True)
        pk = _parent_pk(df_full, parent_name)
        # one custom row per id — same window as _merge_silver; without it
        # every historical version of a parent row becomes its own custom row
        df_full = _latest_per_pk(df_full, pk)
        # 11/17/2026 — payload_hash carried (matches process_table's carry:
        # the rebuilt _custom child stays CDC-dedup-able + traceable)
        carry = list(dict.fromkeys(
            ["bronze_insert_date", "bronze_updated_at", "payload_hash"] + ([pk] if pk else [])))
        print(f"  🔄 re-extracting custom from full bronze {bronze_name}")
        return extract_custom(df_full, parent_name, carry, num_path,
                              parent_pk=parent_pk or pk, dry_run=False,
                              bronze_name=bronze_name, _rebuilding=True)
    finally:
        _CDC_WATERMARK = _saved

# COMMAND ----------

# DBTITLE 1,# Orchestrator
# ============================================================================
# ORCHESTRATOR 11/17/2026
# ============================================================================

# 11/17/2026 — DIAGNOSTICS GATE: the CDC no-new-data path used to run the
# FULL diagnostic suite (key detail + value-verified join keys over every
# silver table + the _key_join_map rebuild) on EVERY no-change cycle —
# minutes of full-layer scans for a nightly that usually has nothing new
# to report. Flip to True to run them on demand; the no-new-data path then
# prints just the existing-table summary and the lineage tree.
RUN_DIAGNOSTICS = False


def _cleanup_run():
    """11/17/2026 — centralized cleanup guard: drop every temp table and
    uncache every DataFrame registered in _CACHED_DFS. Called on BOTH the
    success path and (via the process_table wrapper's finally) the failure
    path — previously a mid-run exception leaked _tmp_ tables and left
    cached plans holding memory for the rest of the notebook."""
    for temp in _TEMP_TABLES:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {temp}")
        except Exception:
            pass
    _TEMP_TABLES.clear()
    for _df in _CACHED_DFS:
        try:
            _df.unpersist()
        except Exception:
            pass
    _CACHED_DFS.clear()


def _discover_lineage_tree(silver_name, pipeline, existing_table_names):
    """Auto-discover hierarchical lineage tree from silver table schemas.
    
    NOTE: Child table creation runs in dry_run mode — lineage is discovered and printed
    but child tables are not written. This function is used for CDC no-new-data path.
    """
    tree = [(f"{pipeline}", 0, silver_name)]
    table_set = set(existing_table_names)
    discovered = {silver_name}
    
    def _discover_children(parent_table, parent_num, depth):
        """Discover children via schema (arrays) then prefix fallback."""
        try:
            df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{parent_table}")
        except Exception:   # 11/17/2026 — bare except fix: also swallowed KeyboardInterrupt/SystemExit
            return
        
        _, arrays, _ = categorize_fields(df)
        
        # Also check for _custom table (extracted from custom struct, not an array)
        custom_name = f"{parent_table}_custom"
        if custom_name in table_set and custom_name not in discovered:
            arrays.append("custom")
        
        child_idx = 0
        # Pass 1: schema-based discovery (array columns → child tables)
        for arr_col in arrays:
            child_name = f"{parent_table}_{arr_col}"
            if child_name not in table_set or child_name in discovered:
                continue
            child_idx += 1
            child_num = f"{parent_num}_{child_idx}"
            discovered.add(child_name)
            tree.append((child_num, depth + 1, f"silver_{child_name}"))
            _discover_children(child_name, child_num, depth + 1)
        
        # Pass 2: prefix-based fallback for tables not found via schema
        # (handles custom array tables where arrays were in the original struct)
        for tname in sorted(table_set):
            if not tname.startswith(f"{parent_table}_") or tname in discovered:
                continue
            # Check if this is a direct child (not a grandchild via a more specific discovered table)
            is_direct = True
            for other in discovered:
                # Only reject if a MORE SPECIFIC (longer) discovered table is the real parent
                if other != parent_table and tname.startswith(f"{other}_") and len(other) > len(parent_table):
                    is_direct = False
                    break
            if is_direct:
                child_idx += 1
                child_num = f"{parent_num}_{child_idx}"
                discovered.add(tname)
                tree.append((child_num, depth + 1, f"silver_{tname}"))
                _discover_children(tname, child_num, depth + 1)
    
    _discover_children(silver_name, f"{pipeline}", 0)
    return tree


def print_lineage_tree(silver_name=None):
    """Print the lineage tree with hierarchical numbering."""
    print(f"\n  📋 LINEAGE TREE:")
    for num, depth, name in _lineage_tree:
        indent = "---" * depth
        print(f"  # * {indent}{num}_{name}")


# --- Lineage key detail (CDC no-new-data diagnostic path only) ----------------
# Mapping keys are CANDIDATES: unique + non-null on a <=10K sample, not proven
# table-wide. One Spark action per table (sample fetched once, counted locally).


def find_child_mapping_keys(df, carried_cols, sample_size=10000):
    """Return candidate mapping/natural-key columns: non-null and unique on a
    <=10K sample, NOT carried from the parent. Sampled candidates only."""
    candidates = []
    # 11/17/2026 — SEEDED RANDOM sample: limit() takes the FIRST N rows,
    # which is dump-order biased (bronze re-dumps put whole families of
    # near-identical rows together, so first-N overstates or understates
    # uniqueness depending on where the dump starts). Seeded random keeps
    # candidate verdicts reproducible run-to-run.
    from pyspark.sql.functions import rand
    sample = df.orderBy(rand(seed=42)).limit(sample_size).toPandas()
    if len(sample) == 0:
        return candidates
    carried = set(carried_cols or [])
    for f in df.schema.fields:
        if f.name in carried:
            continue
        if not isinstance(f.dataType, (StringType, IntegerType, LongType)):
            continue
        s = sample[f.name]
        if s.isna().any():
            continue
        if s.nunique(dropna=False) == len(sample):
            candidates.append(f.name)
    return candidates


def print_lineage_detail(df, num, depth, name, parent_keys=None):
    """Print one table: PK, carried-FK and mapping-key candidates, every column
    with its type. Returns (pk, carried, mapping) for a keys-only summary."""
    indent = "---" * depth
    parent_keys = parent_keys or []
    pk = find_pk(df, validate=False)
    carried = [c for c in df.columns
               if any(c == k or c.endswith(f"_{k}") for k in parent_keys)]
    # bronze control columns are pipeline bookkeeping, never mapping keys
    mapping = find_child_mapping_keys(
        df, carried + ["bronze_insert_date", "bronze_updated_at"]) if depth > 0 else []
    label = f"{num}_{name}" if num else name
    print(f"  # * {indent}{label}")
    print(f"  {indent}   PK: {pk or '—'}")
    if carried:
        print(f"  {indent}   FK/carried: {carried}")
    if mapping:
        print(f"  {indent}   mapping candidates: {mapping}")
    for f in df.schema.fields:
        mark = ""
        if f.name == pk:
            mark = "  ← PK"
        elif f.name in carried:
            mark = "  ← FK"
        elif f.name in mapping:
            mark = "  ← mapping"
        print(f"  {indent}   - {f.name}: {f.dataType.simpleString()}{mark}")
    return (pk, carried, mapping)


def print_keys_only(num, depth, name, pk, fks, mapping_keys):
    """One-line keys summary for a table (PK | FK | mapping candidates)."""
    indent = "---" * depth
    keys = []
    if pk:
        keys.append(f"PK:{pk}")
    if fks:
        keys.append(f"FK:{','.join(fks)}")
    if mapping_keys:
        keys.append(f"map:{','.join(mapping_keys)}")
    label = f"{num}_{name}" if num else name
    print(f"  # * {indent}{label}  {' | '.join(keys) or '(no keys)'}")


def find_join_keys(sample_cap=10000, min_values=30, min_containment=0.90, return_results=False):
    """Value-verify FK→PK joins across ALL silver tables (diagnostic; reads all
    business tables, materializes the _key_join_map side table).

    FK side: pk-like / *_id scalar columns, sampled <= sample_cap rows per table.
    On _custom tables (the pivot position) EVERY scalar column is tested — custom
    fields (CSM, Lead_Owner, …) can carry join keys with no *_id name shape.
    PK side: the COMPLETE distinct value set of each table's PK candidate
    (both-sides-sampling misses core joins — samples don't align on big tables).
    A PK candidate whose values are contained in another table's PK domain is a
    carried parent key (FK echo), not a join target — excluded here, recorded
    as a PK_ECHO row in the map (the PK(x) = PK(y) relation).
    Prints one family per target PK with its verified FK members, and writes
    every relation (PK census, PK_ECHO, PK_FK) to _key_join_map — one row per
    relation with its stable hash, containment, and domain sizes.
    Labels: PK_FK_n [md5-8] — n renumbers per run; the hash is the stable
    identity of the family's membership signature (same membership = same hash).
    """
    import hashlib
    import pandas as pd

    tables = sorted(t.name for t in spark.catalog.listTables(f"{CATALOG}.{SILVER_SCHEMA}"))
    pk_cols, pk_domains, fk_cols = {}, {}, {}
    for t in tables:
        try:
            df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{t}")
            schema = df.schema
        except Exception:
            continue
        pk = find_pk(df, validate=False)
        pk_cols[t] = pk
        cand = []
        for f in schema.fields:
            if not isinstance(f.dataType, (StringType, IntegerType, LongType)):
                continue
            n = f.name
            if n in ("bronze_insert_date", "bronze_updated_at", "payload_hash"):
                continue
            if n == pk or n.lower().endswith("_id") or (pk and n.endswith(f"_{pk}")):
                cand.append(n)
        # _custom tables are the pivot position: EVERY scalar column is a
        # custom-field value (CSM, Lead_Owner, Setter, …) with no *_id shape
        # hint — include them all so the containment test can verify joins
        # the name-based filter would never see.
        if t.endswith("_custom"):
            for f in schema.fields:
                if isinstance(f.dataType, (StringType, IntegerType, LongType)) \
                        and f.name not in ("bronze_insert_date", "bronze_updated_at", "payload_hash"):
                    cand.append(f.name)
        cand = list(dict.fromkeys(cand))
        if not cand:
            continue
        pdf = df.select(*cand).limit(sample_cap).toPandas()
        for c in cand:
            vals = {str(v) for v in pdf[c] if pd.notna(v) and str(v).strip() != ""}
            vals -= {"None", "nan", "NaN"}
            if vals:
                fk_cols[(t, c)] = vals
        if pk:
            full = {str(v) for v in df.select(pk).distinct().toPandas()[pk]
                    if pd.notna(v) and str(v).strip() != ""}
            full -= {"None", "nan", "NaN"}
            if full:
                pk_domains[t] = full

    # Echo exclusion: a PK candidate contained (>= min_containment) in another
    # table's PK domain is a carried parent key, not a true join target.
    # echo_of records the pairing (a inside b) so the map can express PK(x) = PK(y).
    echo, echo_of = set(), {}
    for a, avals in pk_domains.items():
        if a in echo:
            continue
        for b, bvals in pk_domains.items():
            if b == a or b in echo:
                continue
            if len(avals & bvals) / max(len(avals), 1) >= min_containment:
                if len(avals) < len(bvals):
                    echo.add(a); echo_of[a] = b   # a's domain sits inside b's
                elif len(avals) == len(bvals):
                    # equal domains: prefer the non-child (parent) table
                    if b.startswith(a + "_"):
                        echo.add(b); echo_of[b] = a
                    elif a.startswith(b + "_"):
                        echo.add(a); echo_of[a] = b
    targets = {t: v for t, v in pk_domains.items() if t not in echo}

    families = {}
    for (ft, fc), fvals in fk_cols.items():
        if len(fvals) < min_values:
            continue
        for pt, pvals in targets.items():
            if pt == ft:
                continue
            frac = len(fvals & pvals) / len(fvals)
            if frac >= min_containment:
                families.setdefault((pt, pk_cols[pt]), []).append((frac, ft, fc, len(fvals)))

    print(f"\n  🔗 JOIN KEYS (value-verified FK→PK; FK ≤{sample_cap:,} sampled rows, "
          f"PK domain complete; ≥{int(min_containment*100)}% containment, ≥{min_values} distinct; "
          f"_custom tables: all scalar columns tested):")
    n_fam = 0
    for key in sorted(families, key=lambda k: (k[0], k[1])):
        members = sorted(families[key], key=lambda m: (-m[0], m[1], m[2]))
        sig = "|".join(sorted(f"{ft}.{fc}" for _, ft, fc, _ in members)) + f"->{key[0]}.{key[1]}"
        h = hashlib.md5(sig.encode()).hexdigest()[:8]
        n_fam += 1
        print(f"  # * PK_FK_{n_fam} [{h}] → {key[0]}.{key[1]}   (PK domain {len(pk_domains[key[0]]):,})")
        for frac, ft, fc, n in members:
            print(f"  # *      {ft}.{fc}  ({frac*100:.1f}% of {n:,} sampled distinct)")
    if not families:
        print("  (none found at current thresholds)")

    # --- Materialize the key map (side table) ---------------------------------
    # One row per: PK census entry, PK→PK echo containment (PK(x) = PK(y)),
    # and every verified FK→PK member (PK(x) = FK(y)) with its stable hash.
    # Overwritten on every run — the map always reflects current silver state.
    try:
        from pyspark.sql.types import DoubleType, StructField
        from datetime import datetime, timezone
        _now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        map_rows = []
        for t in sorted(pk_domains):
            map_rows.append((_now, "PK", t, pk_cols[t], None, None, None,
                             len(pk_domains[t]), None, None))
            if t in echo_of:
                b = echo_of[t]
                map_rows.append((_now, "PK_ECHO", t, pk_cols[t], b, pk_cols[b],
                                 None, len(pk_domains[t]), len(pk_domains[b]), None))
        for key in families:
            members = sorted(families[key], key=lambda m: (-m[0], m[1], m[2]))
            _sig = "|".join(sorted(f"{ft}.{fc}" for _, ft, fc, _ in members)) + f"->{key[0]}.{key[1]}"
            _h = hashlib.md5(_sig.encode()).hexdigest()[:8]
            for frac, ft, fc, n in members:
                map_rows.append((_now, "PK_FK", ft, fc, key[0], key[1],
                                 round(float(frac), 4), n, len(pk_domains[key[0]]), _h))
        map_schema = StructType([
            StructField("captured_at", StringType()),
            StructField("kind", StringType()),
            StructField("left_table", StringType()),
            StructField("left_col", StringType()),
            StructField("right_table", StringType()),
            StructField("right_col", StringType()),
            StructField("containment", DoubleType()),
            StructField("left_n", LongType()),
            StructField("right_n", LongType()),
            StructField("rel_hash", StringType()),
        ])
        map_name = f"{CATALOG}.{SILVER_SCHEMA}._key_join_map"
        map_pdf = pd.DataFrame(map_rows, columns=[f.name for f in map_schema.fields])
        (spark.createDataFrame(map_pdf, schema=map_schema)
              .write.format("delta").mode("overwrite")
              .option("overwriteSchema", "true").saveAsTable(map_name))
        print(f"  🗺️ Key map → {map_name} ({len(map_rows):,} rows)")
    except Exception as e:
        print(f"  ⚠️ key map skipped: {str(e)[:80]}")

    if return_results:
        return families


def find_problem_keys(bronze_name, sample_rows=1000):
    """DISCOVER problem keys (free-text fields that break quote repair) with
    NO prior knowledge, using the real repair ladder as ground truth.

    Differential method:
      1. Randomly sample raw_data strings from the bronze source.
      2. Run the ladder with the CURRENT _PROBLEM_KEYS — rows still NULL are
         hard failures (reported with an excerpt).
      3. Run the ladder with _PROBLEM_KEYS emptied — rows that now fail but
         passed in step 2 were rescued ONLY by the problem-key wrap.
      4. For each such row, test every key name it contains with a single-key
         wrap: keys that rescue it are the discovered problem keys. Keys NOT
         in the current set are reported MISSING — paste those into
         _PROBLEM_KEYS (JSON Repair UDF cell) to close the gap.

    On-demand tool (NOT wired into the nightly): run per source, or whenever
    a new source starts failing. _PROBLEM_KEYS is mutated only during the
    run and restored in a finally block. Returns a summary dict.
    """
    global _PROBLEM_KEYS
    original = set(_PROBLEM_KEYS)

    br = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_name}")
    _src = br.filter(col("raw_data").isNotNull())
    total = _src.count()
    if total > sample_rows:
        _src = _src.select("raw_data").sample(False, sample_rows / total).limit(sample_rows)
    else:
        _src = _src.select("raw_data")
    raws = list({r["raw_data"] for r in _src.collect()})

    def _ok(s):
        try:
            return _parse_activity_final(s) is not None
        except Exception:
            return False

    rescued_by, missing = {}, {}
    try:
        # Step 2 — current set: hard failures are rows no rung can save.
        ok_current = {s: _ok(s) for s in raws}
        hard = [s for s in raws if not ok_current[s]]

        # Step 3 — emptied set: rows that fail now were saved by the wrap.
        _PROBLEM_KEYS = set()
        fail_no_wrap = [s for s in raws if not _ok(s)]
        wrap_rescued = [s for s in fail_no_wrap if ok_current[s]]

        # Step 4 — single-key differential: which key does the rescuing?
        from collections import Counter
        key_re = re.compile(r"[\"'](\w+)[\"']\s*:")
        rescued_by, missing = Counter(), Counter()
        unexplained = []
        for s in wrap_rescued:
            keys = sorted(set(key_re.findall(s)))
            row_rescuers = []
            for k in keys:
                _PROBLEM_KEYS = {k}
                if _ok(s):
                    row_rescuers.append(k)
            if not row_rescuers:
                # fallback: the row may need SEVERAL keys wrapped at once
                _PROBLEM_KEYS = set(keys)
                if _ok(s):
                    row_rescuers = keys
                else:
                    unexplained.append(s)
            for k in row_rescuers:
                rescued_by[k] += 1
                if k not in original:
                    missing[k] += 1
    finally:
        _PROBLEM_KEYS = original

    print(f"\n  🔧 PROBLEM KEYS — {bronze_name} (sampled {len(raws):,} distinct raw rows):")
    print(f"  # * hard failures (NULL even with current set): {len(hard):,}")
    for s in hard[:3]:
        print(f"      excerpt: {s[:110].replace(chr(10), ' ')}…")
    print(f"  # * rows rescued by problem-key wrap: {len(wrap_rescued):,}")
    if rescued_by:
        print("  # * discovered keys (rows rescued per key):")
        for k, n in sorted(rescued_by.items(), key=lambda x: -x[1]):
            flag = "  ⚠️ MISSING from _PROBLEM_KEYS — add it" if k not in original else ""
            print(f"      {k}: {n}{flag}")
    if unexplained:
        print(f"  # * {len(unexplained)} wrap-rescued row(s) no single/union key set explains — inspect manually")
    if not wrap_rescued and not hard:
        print("  # * sample fully clean — no problem keys exercised in this sample")
    return {"sampled": len(raws), "hard_failures": len(hard),
            "wrap_rescued": len(wrap_rescued),
            "discovered": dict(rescued_by), "missing": dict(missing)}


# 11/17/2026 — renamed to _process_table_inner; the public process_table
# below is the cleanup-guard wrapper (temp tables, cached DataFrames and
# the CDC watermark are released even when a run fails mid-way).
def _process_table_inner(bronze_name, silver_name=None, only_arrays=None, flatten=False):
    """Full Bronze → Silver pipeline: parse, flatten structs, write parent table.
    Child tables are discovered and printed (dry_run) but NOT written.

    Args:
        bronze_name:  Bronze source table name
        silver_name:  Silver target table name (defaults to bronze_name)
        only_arrays:  List of array column names to explode. None = all (default).
                      [] = don't explode any arrays. ["contacts"] = only explode contacts.
        flatten:       If True, flatten struct columns into individual columns (default False).
                      Keep False to preserve first-level JSON keys only.
    """
    global _pipeline_counter, _lineage_tree, _CDC_WATERMARK
    # 11/17/2026 — pipeline-number lookup fix: an UNPINNED table took the
    # raw counter, which can COLLIDE with a pinned PIPELINE_NUMBERS value
    # (two tables numbered identically → ambiguous lineage numbering
    # downstream). Unpinned tables now take the next counter value that no
    # pinned table uses.
    _pinned = set(PIPELINE_NUMBERS.values())
    _pipeline_counter += 1
    while _pipeline_counter in _pinned:
        _pipeline_counter += 1
    pipeline = PIPELINE_NUMBERS.get(silver_name or bronze_name, _pipeline_counter)

    if silver_name is None:
        silver_name = bronze_name

    _lineage_tree = [(f"{pipeline}", 0, silver_name)]

    start = time.time()
    print(f"\n{'='*80}")
    print(f"📥 {bronze_name} → {silver_name}")
    print(f"{'='*80}")

    # Print root structure
    print(f"  # * raw_{bronze_name}-> bronze_{bronze_name}-> {silver_name}")

    # Check Silver watermark for CDC
    _CDC_WATERMARK = get_silver_watermark(silver_name)
    if _CDC_WATERMARK:
        print(f"  CDC MODE: reading changes after {_CDC_WATERMARK}")
    else:
        print(f"  FULL LOAD: first run")

    df_flat = parse_bronze_table(bronze_name, watermark=_CDC_WATERMARK, silver_name=silver_name)
    if df_flat is None:
        # CDC found no new data — print full existing silver table details
        if _CDC_WATERMARK:
            print(f"  ✅ CDC: No new rows since {_CDC_WATERMARK}")
            try:
                all_tables = spark.catalog.listTables(f"{CATALOG}.{SILVER_SCHEMA}")
                related = []
                for t in all_tables:
                    tname = t.name
                    if tname == silver_name or tname.startswith(f"{silver_name}_"):
                        try:
                            tdf = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{tname}")
                            cnt = tdf.count()
                            related.append((tname, cnt, tdf.columns))
                        except Exception:   # 11/17/2026 — bare except fix
                            related.append((tname, -1, []))

                print(f"\n  📊 Existing silver tables ({len(related)}):")
                for tname, cnt, cols in related:
                    print(f"     📦 {tname} ({len(cols)} cols, {cnt:,} rows): {cols}")

            except Exception as e:
                print(f"  ⚠️ Could not read existing silver tables: {str(e)[:80]}")

            # Re-parse a small sample to discover full schema and lineage tree (dry run)
            #_CDC_WATERMARK = None
            print(f"\n  🔍 DRY RUN — re-parsing 200 sample rows for full lineage tree:")
            df_sample = parse_bronze_table(bronze_name, limit=200)
            if df_sample is not None:
                if flatten:
                    df_sample = flatten_structs(df_sample, skip_custom=True)
                pk = _parent_pk(df_sample, silver_name)
                # 11/17/2026 — payload_hash now CARRIED into child tables:
                # children become CDC-dedup-able and a child row traces back
                # to its exact bronze payload (was dropped at the explode
                # boundary before).
                carry = list(dict.fromkeys(["bronze_insert_date", "bronze_updated_at", "payload_hash"] + ([pk] if pk else [])))
                child_idx = 0
                has_custom = "custom" in df_sample.columns and isinstance(df_sample.schema["custom"].dataType, StructType)
                has_custom_cf = any(c.startswith("custom_cf_") for c in df_sample.columns)
                if has_custom or has_custom_cf:
                    child_idx += 1
                    custom_num = f"{pipeline}_{child_idx}"
                    df_sample = extract_custom(df_sample, silver_name, carry, custom_num, parent_pk=pk, dry_run=True)
                scalars, arrays, structs = categorize_fields(df_sample)
                if arrays:
                    for arr in arrays:
                        child_idx += 1
                        child_num = f"{pipeline}_{child_idx}"
                        explode_to_child(df_sample, silver_name, arr, carry, depth=0, num_path=child_num, parent_pk=pk, dry_run=True)

            # Key detail — PK / carried-FK / mapping-key candidates for the
            # parent and every existing child table. Diagnostic only, sampled,
            # wrapped in try/except so a failure here can never fail the task.
            # 11/17/2026 — GATED by RUN_DIAGNOSTICS (module flag at the top
            # of this cell): the full key-detail + value-verified join-key
            # scan over every silver table no longer runs on EVERY
            # no-change cycle — flip the flag to run it on demand.
            if RUN_DIAGNOSTICS:
                try:
                    print(f"\n  🗝️ KEY DETAIL (candidates, sampled ≤10K):")
                    pdf = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}")
                    pk_parent, _, _ = print_lineage_detail(
                        pdf, f"{pipeline}", 0, silver_name, parent_keys=[])
                    key_summary = [(f"{pipeline}", 0, silver_name, pk_parent, [], [])]
                    for tname, cnt, cols in related:
                        if tname == silver_name:
                            continue
                        try:
                            cdf = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{tname}")
                        except Exception:
                            continue
                        cpk, cfks, cmapping = print_lineage_detail(
                            cdf, "", 1, tname,
                            parent_keys=[pk_parent] if pk_parent else [])
                        key_summary.append(("", 1, tname, cpk, cfks, cmapping))
                    print(f"\n  🗝️ KEYS SUMMARY:")
                    for k in key_summary:
                        print_keys_only(*k)
                except Exception as e:
                    print(f"  ⚠️ key detail skipped: {str(e)[:80]}")

                # Value-verified join keys across the whole silver layer — FK→PK
                # containment with complete PK domains (read-only diagnostic).
                try:
                    find_join_keys()
                except Exception as e:
                    print(f"  ⚠️ join keys skipped: {str(e)[:80]}")
            else:
                print(f"  ℹ️ diagnostics skipped (RUN_DIAGNOSTICS=False) — key detail + join keys not run this cycle")

            print_lineage_tree()

            # Clean up temp tables
            for temp in _TEMP_TABLES:
                spark.sql(f"DROP TABLE IF EXISTS {temp}")
            _TEMP_TABLES.clear()

            elapsed = time.time() - start
            print(f"  ⏱️ Completed in {elapsed:.1f}s (CDC — no new data, full lineage via dry run)")
            print(f"{'='*80}")
        _CDC_WATERMARK = None
        return

    # Flatten structs (keep custom struct as-is on parent)
    if flatten:
        df_flat = flatten_structs(df_flat, skip_custom=True)

    # Find PK (carry was used for child tables — retained for re-enablement).
    # Pinned per-family key (SILVER_MERGE_KEYS) first — the Nov 9 pins;
    # find_pk alone is wrong for calendly (no id column) and risky for
    # lead_activites (sample validation can hand back lead_id).
    pk = _parent_pk(df_flat, silver_name)
    # FULL LOAD only: collapse to the LATEST version per PK — the
    # one-row-per-id silver contract (same window as _merge_silver / the
    # Nov 9 rebuild notebooks). parse_bronze_table keeps every distinct
    # payload version by design (full history lives in bronze); without
    # this collapse a first run writes one row per VERSION and the first
    # CDC MERGE then dies on duplicate ids. CDC batches skip it —
    # _merge_silver applies the same window itself.
    if not _CDC_WATERMARK and pk:
        df_flat = _latest_per_pk(df_flat, pk)
    # 11/17/2026 — payload_hash now CARRIED into child tables (was dropped
    # at the explode boundary): children become CDC-dedup-able and a child
    # row can be traced back to its exact bronze payload.
    carry = list(dict.fromkeys(["bronze_insert_date", "bronze_updated_at", "payload_hash"] + ([pk] if pk else [])))

    # Extract custom fields into MAP + print potential _custom child tables (dry_run)
    child_idx = 0
    has_custom = "custom" in df_flat.columns and isinstance(df_flat.schema["custom"].dataType, StructType)
    has_custom_cf = any(c.startswith("custom_cf_") for c in df_flat.columns)
    if has_custom or has_custom_cf:
        child_idx += 1
        custom_num = f"{pipeline}_{child_idx}"
        df_flat = extract_custom(df_flat, silver_name, carry, custom_num, parent_pk=pk, dry_run=True)

    # Print physical schema tree and summary for parent
    # print_physical_schema(df_flat, silver_name)
    # print_schema_summary(df_flat, f"{silver_name} (parent)")
    # bronze_name/flatten flow into write_silver's Nov-12 schema-drift
    # escape hatch: if the CDC MERGE hits a nested mismatch, it can rebuild
    # THIS table from full bronze history instead of failing the run.
    write_silver(df_flat, silver_name, bronze_name=bronze_name, flatten=flatten)

    # Explode arrays (dry_run — print schema without writing child tables)
    scalars, arrays, structs = categorize_fields(df_flat)
    if only_arrays is not None:
        arrays = [a for a in arrays if a in only_arrays]
    if arrays:
        print(f"\n  🔍 DRY RUN — potential child tables (not written):")
        for arr in arrays:
            child_idx += 1
            child_num = f"{pipeline}_{child_idx}"
            explode_to_child(df_flat, silver_name, arr, carry, depth=0, num_path=child_num, parent_pk=pk, dry_run=True)

    # Print lineage tree
    print_lineage_tree(silver_name)

    # Clean up temp tables
    for temp in _TEMP_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {temp}")
    _TEMP_TABLES.clear()

    # Reset CDC watermark for next table
    _CDC_WATERMARK = None

    elapsed = time.time() - start
    print(f"  ⏱️ Completed in {elapsed:.1f}s")
    print(f"{'='*80}")


def process_table(bronze_name, silver_name=None, only_arrays=None, flatten=False):
    """11/17/2026 — CLEANUP-GUARD WRAPPER around _process_table_inner: any
    failure still drops temp tables, unpersists the cached DataFrames and
    resets the CDC watermark BEFORE the exception propagates. Previously a
    mid-run failure leaked _tmp_ tables for the rest of the notebook and
    left the global _CDC_WATERMARK set, poisoning the NEXT table's run in
    the same notebook (it would take the CDC path against the wrong
    table's watermark)."""
    global _CDC_WATERMARK
    try:
        return _process_table_inner(bronze_name, silver_name=silver_name,
                                    only_arrays=only_arrays, flatten=flatten)
    finally:
        _cleanup_run()
        _CDC_WATERMARK = None


def run_tables(bronze_names, silver_names=None, only_arrays=None, flatten=False, continue_on_error=True):
    """11/17/2026 — NEW batch runner: process several bronze tables in one
    call with a per-table outcome summary. continue_on_error=True (default)
    keeps the batch going when one table fails — a nightly no longer dies
    at table 1 of 7 and leaves the rest unprocessed; failures are
    collected and reported in the summary. Set False for strict fail-fast
    behavior (identical to looping process_table by hand)."""
    silver_names = silver_names or [None] * len(bronze_names)
    results = {}
    for _b, _s in zip(bronze_names, silver_names):
        try:
            process_table(_b, silver_name=_s, only_arrays=only_arrays, flatten=flatten)
            results[_b] = "ok"
        except Exception as _e:
            results[_b] = f"FAILED: {str(_e).splitlines()[0][:120]}"
            print(f"  ❌ {_b}: {str(_e).splitlines()[0][:200]}")
            if not continue_on_error:
                raise
    _n_ok = sum(1 for r in results.values() if r == "ok")
    print(f"\n📋 BATCH SUMMARY: {_n_ok}/{len(results)} table(s) processed")
    for _b, _r in results.items():
        print(f"   {'✅' if _r == 'ok' else '❌'} {_b}: {_r}")
    return results


def replay_quarantined(bronze_table, silver_name=None, flatten=False):
    """11/17/2026 — QUARANTINE REPLAY: reprocess payloads parked in
    silver.parse_quarantine (rows the repair ladder / from_json could not
    parse when first seen) through the CURRENT engine. Run this after any
    ladder or discovery upgrade: payloads that now parse are written via
    the normal CDC path and removed from quarantine; still-failing
    payloads stay quarantined for the next upgrade. Recovery without a
    rebuild."""
    silver_name = silver_name or bronze_table
    qt = f"{CATALOG}.{SILVER_SCHEMA}.parse_quarantine"
    if not spark.catalog.tableExists(qt):
        print(f"  ℹ️ No quarantine table yet — nothing to replay")
        return {"replayed": 0, "recovered": 0}
    _hashes = [r["payload_hash"] for r in (spark.table(qt)
                .filter(col("bronze_table") == bronze_table)
                .select("payload_hash").distinct().collect())]
    if not _hashes:
        print(f"  ℹ️ No quarantined payloads for {bronze_table}")
        return {"replayed": 0, "recovered": 0}
    print(f"\n🔄 REPLAY: {len(_hashes):,} quarantined payload(s) from "
          f"{bronze_table} through the current engine")
    global _CDC_WATERMARK
    _saved_wm = _CDC_WATERMARK
    try:
        _CDC_WATERMARK = get_silver_watermark(silver_name)
        try:
            df = parse_bronze_table(bronze_table, watermark=_CDC_WATERMARK,
                                    silver_name=silver_name,
                                    only_payload_hashes=set(_hashes))
        except RuntimeError:
            # the 0-parsed guard fires when NONE of the parked payloads
            # recover yet — expected on replay; report instead of aborting
            print(f"  ℹ️ none of the {len(_hashes):,} quarantined payload(s) "
                  f"parse yet — ladder not fixed for this shape")
            df = None
        if df is not None:
            if flatten:
                df = flatten_structs(df, skip_custom=True)
            pk = _parent_pk(df, silver_name)
            if not _CDC_WATERMARK and pk:
                df = _latest_per_pk(df, pk)
            carry = list(dict.fromkeys(
                ["bronze_insert_date", "bronze_updated_at", "payload_hash"] + ([pk] if pk else [])))
            has_custom = ("custom" in df.columns
                          and isinstance(df.schema["custom"].dataType, StructType))
            has_custom_cf = any(c.startswith("custom_cf_") for c in df.columns)
            if has_custom or has_custom_cf:
                _pipeline = PIPELINE_NUMBERS.get(silver_name or bronze_table, 0)
                df = extract_custom(df, silver_name, carry, f"{_pipeline}_1",
                                    parent_pk=pk, dry_run=True)
            write_silver(df, silver_name)
        # resolved = quarantined payloads now present in silver (recovered
        # now, or landed via a later re-dump) — only these leave quarantine
        _silver_full = f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}"
        _present = []
        if spark.catalog.tableExists(_silver_full):
            _present = [r["payload_hash"] for r in (spark.table(_silver_full)
                        .filter(col("payload_hash").isin(_hashes))
                        .select("payload_hash").distinct().collect())]
        if _present:
            _in = ", ".join(f"'{_h}'" for _h in _present)
            spark.sql(f"DELETE FROM {qt} WHERE payload_hash IN ({_in})")
        _still = len(_hashes) - len(_present)
        print(f"  {'✅' if _present else '⚠️'} Replay {bronze_table}: "
              f"{len(_present):,} payload(s) recovered, {_still:,} still "
              f"failing (staying quarantined)")
        return {"replayed": len(_hashes), "recovered": len(_present)}
    finally:
        _CDC_WATERMARK = _saved_wm


def cleanup_orphaned_temp_tables():
    """11/17/2026 — engine-start sweep: crash-orphaned _tmp_* / _stg_*
    tables in the silver schema are dropped at load time (a crash between
    a write and the finally-cleanup used to leave them accumulating run
    over run). NOTE: if you ever run two CONCURRENT pipelines sharing this
    silver schema, remove the auto-call below — it would drop the other
    run's in-flight staging tables (today's max_concurrency=1 deployment
    is safe by construction)."""
    try:
        _orphans = [t.name for t in spark.catalog.listTables(f"{CATALOG}.{SILVER_SCHEMA}")
                    if t.name.startswith(("_tmp_", "_stg_"))]
        for _o in _orphans:
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SILVER_SCHEMA}.{_o}")
        if _orphans:
            print(f"  🧹 Dropped {len(_orphans)} orphaned temp/staging table(s): "
                  f"{_orphans[:6]}{' …' if len(_orphans) > 6 else ''}")
    except Exception as _ce:
        print(f"  ⚠️ orphan temp cleanup skipped ({str(_ce)[:60]})")


cleanup_orphaned_temp_tables()


print("✅ Silver engine loaded")
print("   Usage: process_table('bronze_table_name', 'silver_table_name')")
print("          run_tables(['bronze_t1', 'bronze_t2', ...]) — batch run with per-table summary")
print("          replay_quarantined('bronze_table_name') — re-run quarantined payloads through the current ladder")

# COMMAND ----------

# DBTITLE 1,# FUTURE USE — Drift, Deletion & Identity Detectors
# ============================================================================
# FUTURE USE — DRIFT, DELETION & IDENTITY DETECTORS (dormant) 11/17/2026
# ============================================================================
# 11/17/2026 — WHY THIS CELL EXISTS: the stress-taxonomy review listed engine
# threats with ZERO observed evidence in the current 9 bronze tables
# (dynamic-key maps, PK format changes, epoch-unit drift, source
# hard-deletes, 5K-field column explosion). Rather than build the cures for
# diseases nobody has, the DETECTORS are pre-built here. NOTHING in this
# cell auto-runs — no nightly cost, no behavior change, no writes. Run the
# one that matches a symptom; every function is REPORT-ONLY and never
# drops, deletes, or modifies anything.
#
#   Symptom → run:
#     silver rows that should be gone / GDPR erasure   → report_deletion_drift
#     PKs suddenly mixed formats / odd MERGE identity   → detect_pk_format_change
#     discovery exploded hundreds of one-off "fields"   → detect_dynamic_key_maps
#     date columns landing in 1970-1973                → detect_epoch_unit_drift
#     a wide new source joining the estate             → check_field_budget
#
# When a detector fires with real evidence, THAT is the moment to build the
# cure (tombstone policy, MAP/KV child table, PK migration, unit cast) —
# not before.
#
# 11/19/2026 — WIRING STATUS (who consumes what):
#   NIGHTLY via engine_health_report (cell 12 CALLS these functions — they
#   stay DEFINED here; this cell is the toolbox, that cell is the reader):
#     • audit_event_time_format — check 10, quiet=True: the survivorship-
#       ordering tripwire (one aggregation per SILVER_MERGE_KEYS parent).
#     • check_field_budget      — check 11: column-explosion budget from
#       the run log the report already reads.
#   NIGHTLY via engine-NATIVE telemetry (NOT this cell — the engine itself
#   writes these to _run_log; health checks 4-6, 9 read those fields):
#     type-conflict notes, fields_discovered jumps, rung_stats mix flips,
#     parse rates, empty-increment streaks, bronze freshness.
#   STILL ON-DEMAND ONLY (full bronze parse each, or a business decision):
#     report_deletion_drift, detect_pk_format_change,
#     detect_dynamic_key_maps, detect_epoch_unit_drift,
#     audit_schema_registry, audit_nested_schema_drift,
#     audit_event_time_regressions. Run the one matching a symptom.


# 11/17/2026 — DRIFT POLICY (documentation-of-record): what the engine
# ALREADY does per drift class, and which classes stay manual. Nothing
# consumes this at runtime yet; promote an entry to real config only when
# a source needs a different policy than the engine-wide default.
DRIFT_POLICY = {
    "additive (new field)":          "AUTO — grow-only registry + ALTER ADD COLUMN",
    "subtractive (field vanishes)":  "FAIL — column-drop guard refuses overwrite drops; registry keeps the column (all-NULL)",
    "type-change (mixed types)":     "ALERT + QUARANTINE — type-conflict telemetry + parse-null quarantine; never stringified silently",
    "unit / semantic drift":         "MANUAL — no engine can prove a field changed MEANING; only contract tests / consumer checks. Mechanically detectable subset: detect_epoch_unit_drift below",
    "deletion (source hard-delete)": "REPORT ONLY — report_deletion_drift below; never auto-delete",
    "key drift (PK format change)":  "REPORT ONLY — detect_pk_format_change below",
    "producer drift (extraction)":   "FAIL LOUD — parse guards + run-log note; canonical dedup absorbs dialect re-dumps",
    "restatement (re-dump history)": "AUTO — byte + canonical dedup; survivorship keeps the current version",
}


def report_deletion_drift(bronze_table, silver_name, pk=None, dumps_before_stale=3):
    """FUTURE USE — hard-delete / GDPR-erasure candidates.

    Snapshot re-dump sources never send a tombstone: a deleted row simply
    stops appearing in dumps, and silver keeps it forever. The source
    re-dumps the FULL current row set every night, so a PK whose LAST
    bronze appearance (max insert_date over all its payload hashes) is
    older than the newest dump(s) is a deletion candidate.

    REPORT-ONLY and staying that way — turning this into a removal /
    soft-delete-flag policy is a business decision. When you make it,
    this report is its input. Costs one full parse of the bronze table:
    run on demand, not nightly. dumps_before_stale tolerates N missed
    dumps before calling a PK stale (default 3).
    """
    df = parse_bronze_table(bronze_table, silver_name=silver_name)
    if df is None:
        print(f"  ℹ️ {bronze_table}: nothing parsable — nothing to compare")
        return None
    pk = pk or SILVER_MERGE_KEYS.get(silver_name) or _parent_pk(df, silver_name)
    from pyspark.sql.functions import max as _pmax

    # last-seen per PK, computed from BRONZE directly (max insert_date over
    # every payload hash the PK ever produced) — independent of the dedup
    # window's keep-earliest/latest semantics, so it stays correct either way.
    _b = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")
    _last_hash_seen = _b.groupBy("payload_hash") \
        .agg(_pmax("insert_date").alias("last_seen"))
    _pkmap = (df.select("payload_hash", col(pk).alias("_pk"))
                .filter(col(pk).isNotNull()))
    _per_pk = (_pkmap.join(_last_hash_seen, "payload_hash")
               .groupBy("_pk").agg(_pmax("last_seen").alias("last_seen")))

    _dumps = sorted(r["insert_date"] for r in
                    _b.select("insert_date").distinct().collect())
    if len(_dumps) <= dumps_before_stale:
        print(f"  ℹ️ only {len(_dumps)} dump timestamp(s) in bronze — "
              f"not enough history to call anything stale")
        return None
    _cutoff = _dumps[-dumps_before_stale]
    _ghosts = _per_pk.filter(col("last_seen") < lit(_cutoff))
    _n = _ghosts.count()
    _pop = _per_pk.count()
    print(f"\n🪦 DELETION-DRIFT REPORT {silver_name} "
          f"(stale cutoff {str(_cutoff)[:19]}): {_n:,} of {_pop:,} PKs "
          f"last seen in an older dump — deletion candidates (REPORT ONLY)")
    if _n:
        _ghosts.orderBy(col("last_seen").asc()).show(20, truncate=False)
    return _ghosts


def detect_pk_format_change(silver_name, pk=None):
    """FUTURE USE — PK format drift (int id → UUID migration at the source,
    id recycling). Groups the silver table's PK by format class (numeric /
    uuid / other) per month of bronze_insert_date and reports months where
    more than one class is materially present or the dominant class flips
    — the signature of a mid-history PK format change, which breaks MERGE
    identity silently ('123' ≠ 123 ≠ 'a1b2…'). REPORT-ONLY."""
    pk = pk or SILVER_MERGE_KEYS.get(silver_name)
    if pk is None:
        print(f"  ℹ️ no pinned PK for {silver_name} — pass pk=")
        return None
    t = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}")
    _cls = (when(col(pk).rlike(r"^\d+$"), "numeric")
            .when(col(pk).rlike(
                r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"), "uuid")
            .otherwise("other"))
    _rows = (t.select(expr("date_format(bronze_insert_date, 'yyyy-MM')").alias("month"),
                      _cls.alias("cls"))
               .groupBy("month", "cls").count().orderBy("month").collect())
    _by_month = {}
    for r in _rows:
        _by_month.setdefault(r["month"], {})[r["cls"]] = r["count"]
    print(f"\n🔑 PK-FORMAT SCAN {silver_name}.{pk}:")
    _flags = []
    _prev_dom = None
    for _m in sorted(_by_month):
        _cls_counts = _by_month[_m]
        _dom = max(_cls_counts, key=_cls_counts.get)
        _mixed = [c for c, n in _cls_counts.items() if n >= max(1, 0.01 * sum(_cls_counts.values()))]
        print(f"   {_m}: {_cls_counts}")
        if len(_mixed) > 1:
            _flags.append(f"{_m}: mixed classes {_mixed}")
        if _prev_dom and _dom != _prev_dom:
            _flags.append(f"{_m}: dominant class changed {_prev_dom} → {_dom}")
        _prev_dom = _dom
    for _f in _flags or ["  ✅ no format drift detected"]:
        print(f"  {_f}")
    return _flags


def detect_dynamic_key_maps(bronze_table, sample_rows=2000, rare_key_frac=0.01):
    """FUTURE USE — dynamic-key maps (\"user_123\": {...}\" — the KEY is data).

    Discovery explodes each unique key into a \"field\"; the tell is
    hundreds of one-off keys that appear in ~1 row. Scans a sample of raw
    payloads with a key regex and reports keys seen in fewer than
    rare_key_frac of rows. COARSE heuristic — the regex catches NESTED
    keys too, so treat a firing as \"go look at the payload shape\", not
    as proof. If a real table fires, the cure is a MAP/KV child table,
    not more columns. REPORT-ONLY; costs one .limit(sample).collect()."""
    import re as _re
    _rows = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")
             .select("raw_data").filter(col("raw_data").isNotNull())
             .limit(sample_rows).collect())
    _n = len(_rows)
    if not _n:
        print(f"  ℹ️ {bronze_table}: no raw_data rows to scan")
        return None
    _counts = {}
    for r in _rows:
        for k in set(_re.findall(r"['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\s*:",
                                 r["raw_data"] or "")):
            _counts[k] = _counts.get(k, 0) + 1
    _thresh = max(2, int(_n * rare_key_frac))
    _rare = {k: c for k, c in _counts.items() if c <= _thresh}
    print(f"\n🧭 DYNAMIC-KEY SCAN {bronze_table} "
          f"({_n:,} rows sampled, {len(_counts)} distinct keys, "
          f"rare = ≤{_thresh} rows):")
    if len(_counts) > 500 or len(_rare) > 100:
        print(f"  ⚠️ {len(_rare)} of {len(_counts)} keys are one-offs — "
              f"dynamic-key map suspected (e.g. {sorted(_rare)[:10]})")
    else:
        print("  ✅ no dynamic-key signature")
    return _rare


def detect_epoch_unit_drift(silver_name, year_floor=1990):
    """FUTURE USE — epoch-unit drift (seconds vs millis vs micros) and
    implausible dates. If a source switches units, epoch-derived dates land
    in 1970-1973 (seconds) or the far future (micros); year_floor < 1990
    catches the 1970s class. Scans timestamp/date columns of a silver
    table for values before year_floor. REPORT-ONLY."""
    t = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}")
    _hits = {}
    for f in t.schema.fields:
        if f.dataType.typeName() not in ("timestamp", "date"):
            continue
        _bad = t.filter(col(f"`{f.name}`").isNotNull() &
                       expr(f"year(`{f.name}`) < {year_floor}")).count()
        if _bad:
            _hits[f.name] = _bad
    print(f"\n🕰️ EPOCH-UNIT SCAN {silver_name} (year < {year_floor}):")
    if _hits:
        for _c, _n in sorted(_hits.items(), key=lambda x: -x[1]):
            print(f"  ⚠️ {_c}: {_n:,} implausibly-early value(s)")
    else:
        print("  ✅ no implausible dates")
    return _hits


def check_field_budget(field_budget=1000):
    """FUTURE USE — column-explosion guard for wide sources. Reads the
    engine's own run log (silver._run_log) and reports tables whose
    max(fields_discovered) exceeds field_budget — the point where 'every
    field is a column' should give way to top-N% columns + a MAP/sidecar
    tail. Run this before onboarding any new wide source. REPORT-ONLY."""
    qt = f"{CATALOG}.{SILVER_SCHEMA}._run_log"
    if not spark.catalog.tableExists(qt):
        print("  ℹ️ no run log yet — run the engine first")
        return None
    _rows = (spark.table(qt)
             .filter(col("fields_discovered").isNotNull())
             .groupBy("silver_name")
             .agg(expr("max(fields_discovered)").alias("fields"))
             .orderBy(col("fields").desc()).collect())
    print(f"\n📏 FIELD BUDGET (top fields_discovered per table, budget {field_budget}):")
    _over = []
    for r in _rows:
        _mark = "⚠️ OVER" if r["fields"] > field_budget else "  "
        print(f"  {_mark} {r['silver_name']}: {r['fields']}")
        if r["fields"] > field_budget:
            _over.append(r["silver_name"])
    return _over


# --- 11/17/2026 boss-mode — GUARD AUDITS (can the guards let wrong data
# through? These audit the RESIDUAL holes deliberately left open) ---------


def audit_schema_registry(silver_name=None):
    """FUTURE USE — registry hygiene. The grow-only schema registry pins
    every discovery FOREVER — including a BAD one (a misrepair producing
    garbage keys in one corrupted batch would re-add itself to every
    subsequent batch, forever, as a phantom column). Lists registry entries
    per silver table, compares to the live silver columns, and flags:
      • REGISTRY-ORPHANS  — registry fields the silver table doesn't have
        (stale, renamed, or poisoned entries);
      • UNREGISTERED      — silver columns the registry doesn't know
        (added outside discovery — check who/what added them).
    REPORT-ONLY. If orphans are garbage keys, purge manually after review
    (DELETE FROM _schema_registry WHERE silver_name=… AND field_name=…),
    then let the next batch's column-drop guard / rebuild settle the silver
    side. Requires the _schema_registry table to exist."""
    _regq = f"{CATALOG}.{SILVER_SCHEMA}._schema_registry"
    if not spark.catalog.tableExists(_regq):
        print("ℹ️ no _schema_registry yet — registry-backed tables appear "
              "after their first registry-backed parse")
        return None
    _reg = spark.table(_regq)
    _names = ([silver_name] if silver_name else
              sorted({r["silver_name"] for r in
                      _reg.select("silver_name").distinct().collect()}))
    _flags = {}
    for _s in _names:
        _fields = sorted({r["field_name"] for r in
                          _reg.filter(col("silver_name") == _s)
                          .select("field_name").distinct().collect()})
        try:
            _cols = set(spark.table(
                f"{CATALOG}.{SILVER_SCHEMA}.{_s}").columns)
        except Exception:
            _cols = None
        _line = [f"{_s}: {len(_fields)} registry field(s)"]
        if _cols is not None:
            _orphans = [f for f in _fields if f not in _cols]
            _unreg = [c for c in sorted(_cols) if c not in _fields
                      and not c.startswith("bronze_")]
            if _orphans:
                _line.append(f"⚠️ registry-orphan(s) {_orphans[:8]}")
                _flags[_s] = _orphans
            if _unreg:
                _line.append(f"⚠️ unregistered column(s) {_unreg[:8]}")
                _flags.setdefault(_s, []).extend(_unreg)
        print("  " + " | ".join(_line))
    if not _flags:
        print("  ✅ registry consistent with silver columns (checked top-level)")
    return _flags


def audit_nested_schema_drift(bronze_name, silver_name):
    """FUTURE USE — the escape hatch nobody guards: the column-drop guard
    protects TOP-LEVEL columns only. A full rebuild (overwriteSchema) can
    silently DROP or RETYPE fields INSIDE a struct/array column — the silver
    consumer just sees child paths vanish or change type with no flag at
    all. This audit re-parses the bronze table fresh (FULL discovery) and
    compares EVERY nested path (walked recursively: struct children, and
    struct children of array elements) against the live silver table's
    paths: reports DROPPED paths, ADDED paths, and RETYPED paths.
    Costs one full parse — run on demand (e.g., after any rebuild, or when
    a child extraction path returns NULLs suddenly). REPORT-ONLY.
    NOTE: compare against a NON-flattened silver table (flattened parents
    promote nested paths to columns — for those, dropped columns are
    already caught by the column-drop guard at write time)."""
    def _walk(dt, prefix=""):
        _out = {}
        for _f in dt.fields:
            _p = f"{prefix}.{_f.name}" if prefix else _f.name
            _out[_p] = _f.dataType.simpleString()
            if isinstance(_f.dataType, StructType):
                _out.update(_walk(_f.dataType, _p))
            elif (isinstance(_f.dataType, ArrayType)
                  and isinstance(_f.dataType.elementType, StructType)):
                _out.update(_walk(_f.dataType.elementType, _p + "[]"))
        return _out
    _fresh = parse_bronze_table(bronze_name, schema_sample=None,
                                silver_name=silver_name)
    if _fresh is None:
        print("  ℹ️ nothing parsable — cannot audit")
        return None
    _new = _walk(_fresh.schema["value"].dataType) if "value" in _fresh.columns \
        else {}
    _silver_dt = spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}").schema
    _old = {}
    for _f in _silver_dt.fields:
        if isinstance(_f.dataType, StructType):
            _old.update(_walk(_f.dataType, _f.name))
        elif (isinstance(_f.dataType, ArrayType)
              and isinstance(_f.dataType.elementType, StructType)):
            _old.update(_walk(_f.dataType.elementType, _f.name + "[]"))
    _dropped = {p: t for p, t in _old.items() if p not in _new}
    _added = {p: t for p, t in _new.items() if p not in _old}
    _retyped = {p: (_old[p], _new[p]) for p in _old if p in _new
                and _old[p] != _new[p]}
    print(f"\n🔬 NESTED SCHEMA AUDIT {silver_name} vs fresh parse of {bronze_name}:")
    print(f"  dropped nested path(s): {sorted(_dropped) or '✅ none'}")
    print(f"  added nested path(s):   {sorted(_added) or '✅ none'}")
    print(f"  retyped:                "
          f"{ {p: f'{a}→{b}' for p, (a, b) in _retyped.items()} or '✅ none' }")
    return {"dropped": _dropped, "added": _added, "retyped": _retyped}


def audit_event_time_format(silver_name, quiet=False):
    """FUTURE USE — event-time survivorship orders STRING event times
    lexicographically; that is only correct while the source renders ONE
    uniform format per column. If a source changes its updated_at rendering
    mid-history (format, precision, or TZ-offset change), ordering silently
    breaks and the WRONG row survives as 'current' — with no error
    anywhere. Scans the resolved event-time column (via _find_event_time)
    for format-class and length uniformity; multiple classes/lengths =
    alert. Costs one aggregation — cheap to run anytime. REPORT-ONLY.
    11/19/2026 — quiet=True: one line per table, no per-class detail
    (engine_health_report check 10 consumes this NIGHTLY — see the cell
    header WIRING STATUS)."""
    _t = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{silver_name}")
    _et = _find_event_time(_t)
    if not _et:
        print(f"  ℹ️ {silver_name}: no event-time column — nothing to audit")
        return None
    _v = col(f"`{_et}`")
    _cls = (when(_v.isNull(), lit(None))
            .when(_v.rlike(r"^\d{4}-\d{2}-\d{2}T[\d:.]+(Z|[+-]\d{2}:?\d{2})?$"), "ISO_T")
            .when(_v.rlike(r"^\d{4}-\d{2}-\d{2} [\d:.]+$"), "ISO_space")
            .when(_v.rlike(r"^\d{4}-\d{2}-\d{2}$"), "date_only")
            .when(_v.rlike(r"^\d+$"), "epoch_digits")
            .otherwise("other"))
    _rows = (_t.select(_cls.alias("cls"), expr(f"length(`{_et}`)").alias("len"))
             .groupBy("cls", "len").count().orderBy(col("count").desc()).collect())
    if not quiet:
        print(f"\n🕰️ EVENT-TIME FORMAT AUDIT {silver_name}.{_et}:")
        for r in _rows[:8]:
            print(f"   {r['cls'] or 'NULL'} len={r['len']}: {r['count']:,}")
    _classes = {r["cls"] for r in _rows if r["cls"]}
    _lens = {r["len"] for r in _rows if r["cls"]}
    _flags = []
    if len(_classes) > 1:
        _flags.append(f"MIXED format classes: {sorted(_classes)}")
    elif _classes and len(_lens) > 1:
        _flags.append(f"single class but MIXED lengths: {sorted(_lens)} "
                      "(precision change — ordering risk)")
    if _flags:
        for _f in _flags:
            print(f"  🔴 EVENT-TIME {silver_name}.{_et}: {_f} — lexicographic "
                  "survivorship ordering may be WRONG for mixed rows; "
                  "cast/normalize before trusting 'current'")
    else:
        print(f"  🟢 EVENT-TIME {silver_name}.{_et}: uniform — "
              "lexicographic ordering is sound")
    return _flags


def audit_event_time_regressions(bronze_table, silver_name=None, pk=None):
    """FUTURE USE — the ONE failure mode of event-time survivorship. The
    chain is sound even for deeply-nested updates: a nested change bumps
    the parent row's top-level date_updated (the row is the unit of update
    at the source), and if the source ever forgets to bump it, the distinct
    payload_hash + arrival tiebreak still resolve the newest version. The
    only hole: the source writes event time BACKWARDS (clock lie, history
    rewrite, merge re-dating) — then an OLDER version outranks the newer
    one and survivorship keeps stale 'current' state, silently.
    This scans every bronze version per PK, ordered by ARRIVAL
    (bronze_insert_date), and counts transitions where the event time
    DECREASED from the previously-arrived version: the exact rows where
    survivorship may have made the wrong pick. Costs one full parse — run
    on demand. REPORT-ONLY (a regression can be legitimate — a merge that
    re-dates a record — so the output is a review list, not a kill switch)."""
    from pyspark.sql.functions import lag as _lag
    from pyspark.sql.window import Window as _Sw
    df = parse_bronze_table(bronze_table, schema_sample=None,
                            silver_name=silver_name)
    if df is None or "value" not in df.columns:
        print("  ℹ️ nothing parsable — cannot audit")
        return None
    _vdt = df.schema["value"].dataType
    if not isinstance(_vdt, StructType):
        print("  ℹ️ non-struct payload — no event time to audit")
        return None
    _low = {f.name.lower(): f.name for f in _vdt.fields
            if not isinstance(f.dataType, (ArrayType, StructType))}
    _et = next(( _low[c] for c in _EVENT_TIME_CANDIDATES if c in _low ), None)
    if not _et:
        print("  ℹ️ no event-time column in the payload — arrival ordering is "
              "already in use; nothing to audit")
        return None
    pk = pk or SILVER_MERGE_KEYS.get(silver_name) or _parent_pk(df, silver_name)
    if not pk:
        print("  ℹ️ no PK resolvable — pass pk=")
        return None
    _w = _Sw.partitionBy(pk).orderBy(
        col("bronze_insert_date").asc_nulls_last(), col("payload_hash").asc())
    _et_col = col(f"value.{_et}")
    _reg = (df.filter(col(pk).isNotNull() & _et_col.isNotNull())
            .withColumn("_prev_et", _lag(_et_col).over(_w))
            .filter(col("_prev_et").isNotNull()
                    & (_et_col < col("_prev_et")))   # event time went BACKWARDS
            .select(pk, _et_col.alias("et_now"), col("_prev_et").alias("et_prev"),
                    "bronze_insert_date", "payload_hash"))
    _n = _reg.count()
    _tot = df.filter(col(pk).isNotNull()).count()
    print(f"\n🕰️ EVENT-TIME REGRESSION SCAN {silver_name or bronze_table} "
          f"via `{_et}`: {_n:,} of {_tot:,} version transitions moved BACKWARDS")
    if _n:
        _reg.orderBy(col("bronze_insert_date").desc()).show(20, truncate=False)
        print("  ⚠️ for these PKs, survivorship picked by event time may be "
              "keeping an OLDER version as 'current' — review each: a merge "
              "re-date is legitimate; a clock lie is not")
    else:
        print("  ✅ no backwards event-time transitions — survivorship picks are sound")
    return _reg


print("🧰 Drift/deletion/identity detectors loaded — audit_event_time_format + check_field_budget are consumed NIGHTLY by engine_health_report (checks 10-11); the rest stay manual, report-only; see DRIFT_POLICY")

# COMMAND ----------

# DBTITLE 1,# ENGINE HEALTH — fleet control room
# ============================================================================
# ENGINE HEALTH — fleet control room 11/17/2026
# ============================================================================
# 11/17/2026 — THE FLAG THAT WAS MISSING. Answers, with evidence:
#   "bronze silently loads 8 of 9 tables — does silver look green?"
#     Before: YES, silently. Silver only knows what it PARSED; a missing
#     bronze load showed as an 'empty increment' or NO run-log row, and
#     nothing compared silver's activity against the EXPECTED fleet.
#     Now: engine_health_report() checks every EXPECTED_BRONZE_TABLES entry.
#   "silver crashes → job stops, yesterday's data stays" — TRUE, and safe:
#     the CDC watermark lives IN SILVER, so a crashed run re-processes the
#     same batch on the next run (no skip, no dup — MERGE is idempotent).
#     The danger was always the OTHER direction: bronze partially loads,
#     silver runs 'green' on incomplete bronze. That is what this catches.
#   "does bronze load no matter what?" — bronze is out of this engine's
#     hands; the contract here is: silver data is CORRECT FOR THE BRONZE IT
#     HAS (per-table parse guards), and this report asserts bronze is
#     COMPLETE and FRESH enough to trust (fleet contract).
# Report-only — reads telemetry (silver._run_log, parse_quarantine,
# null_pk_quarantine) plus bronze max(insert_date); changes no data.
# Wire it as the job's LAST task: a non-empty findings dict is the alert.


def engine_health_report(check_bronze_freshness=True,
                         empty_increment_streak=3,
                         stale_bronze_hours=48,
                         parse_rate_alert=0.5,
                         quarantine_growth_alert=50,
                         rebuild_storm_window_days=7,
                         rebuild_storm_max=3,
                         field_jump_alert=10,
                         rung_mix_shift_alert=25,
                          field_budget=1000):
    """Fleet health in one command. Returns a findings dict (empty = clean).
    Per expected bronze table (EXPECTED_BRONZE_TABLES):
      1. MISSING TELEMETRY  — no run-log row ever: bronze load may have
         failed silently and silver never ran on it.
      2. EMPTY STREAK       — N consecutive 'empty increment' runs: source
         stopped sending (creds expired, extraction view broken, upstream
         outage). Silver is 'green' but eating nothing.
      3. BRONZE FRESHNESS  — max(insert_date) older than stale_bronze_hours:
         dumps stopped landing.
      4. PARSE RATE         — last real batch parsed < parse_rate_alert of
         the parsed batch (cdc_rows on CDC runs; deduped_rows on FULL runs,
         where no anti-join ran).
      5. FIELD-COUNT JUMP   — fields_discovered moved > field_jump_alert:
         source schema changed materially (check type-conflict notes).
      6. TYPE-CONFLICT NOTE — recent runs carrying type-conflict notes.
    Fleet-wide:
      7. REBUILD STORM  — > rebuild_storm_max REBUILD rows per silver table
         within rebuild_storm_window_days (nightly drift re-firing the
         nested escape hatch — root-cause needed, not a lifestyle).
      8. QUARANTINE GROWTH — new parse_quarantine rows/day above threshold,
         null_pk_quarantine size, and any quarantine-DEGRADED runs
         (protection silently off — the worst silent failure there is).
      9. RUNG-MIX FLIP — rung_stats mix vs 7-day baseline: new rung value,
         null-rung spike, share shift (repair-ladder drift tripwire; NULL
         rung_stats = pre-11/19 telemetry, excluded from baselines).
     10. EVENT-TIME FORMAT — cell-11 audit_event_time_format per silver
         parent (quiet): survivorship orders STRING event times; mixed
         classes/lengths = the wrong row can survive as 'current'.
     11. FIELD BUDGET — cell-11 check_field_budget: tables over
         field_budget discovered fields — the every-field-a-column wall.
    """
    from datetime import datetime, timedelta, timezone
    _findings = {}
    _rlq = f"{CATALOG}.{SILVER_SCHEMA}._run_log"
    if not spark.catalog.tableExists(_rlq):
        print("ℹ️ no run log yet — run the engine at least once first")
        return {"fatal": "no telemetry"}
    _rows = [r.asDict() for r in spark.table(_rlq).collect()]
    _now = datetime.now(timezone.utc)

    def _dt(_s):
        try:
            return datetime.strptime(str(_s)[:19], "%Y-%m-%d %H:%M:%S")\
                .replace(tzinfo=timezone.utc)
        except Exception:
            return None

    print(f"\n🩺 ENGINE HEALTH REPORT — {len(EXPECTED_BRONZE_TABLES)} expected bronze table(s)")
    _by_b = {}
    for r in _rows:
        _by_b.setdefault(r.get("bronze_table"), []).append(r)

    # ---- per-table checks (1,2,4,5,6) ----------------------------------
    for _b in EXPECTED_BRONZE_TABLES:
        _rs = _by_b.get(_b, [])
        _rs.sort(key=lambda r: str(r.get("logged_at")))
        _probs = []
        if not _rs:
            _probs.append("no run-log row EVER — bronze load may have failed "
                          "silently / table missing / engine never ran on it")
        else:
            _note = lambda r: (r.get("note") or "")
            _streak = 0
            for r in reversed(_rs):
                if _note(r).startswith("empty increment") or \
                        _note(r) == "no rows after repair":
                    _streak += 1
                else:
                    break
            if _streak >= empty_increment_streak:
                _probs.append(f"{_streak} consecutive empty increments — "
                              "source stopped sending? (creds/extraction/upstream)")
            _real = [r for r in _rs if r.get("parsed_valid")]
            if _real:
                _l = _real[-1]
                # 11/19/2026 fix: CDC rows must rate against cdc_rows (the
                # post-anti-join batch actually parsed). deduped_rows counts
                # unchanged re-dumped payloads the anti-join legitimately
                # skips — measured: mdl_users nightly CDC is 117 new payloads
                # inside a 26,317-payload dump (117/117 parsed = 100%), but
                # against deduped_rows it printed as '0.4% degrading'. FULL
                # runs and pre-11/17 rows have cdc_rows NULL → deduped_rows.
                _d = _l.get("cdc_rows")
                if _d is None:
                    _d = _l.get("deduped_rows") or 0
                if _d and (_l["parsed_valid"] / _d) < parse_rate_alert:
                    _probs.append(f"parse rate {_l['parsed_valid'] / _d:.1%} "
                                  f"on last batch — ladder/discovery degrading")
            _fr = [(r.get("logged_at"), r.get("fields_discovered"))
                   for r in _rs if r.get("fields_discovered")]
            if len(_fr) >= 2 and _fr[-1][1] is not None and _fr[-2][1] is not None \
                    and abs(_fr[-1][1] - _fr[-2][1]) > field_jump_alert:
                _probs.append(f"field count {_fr[-2][1]} → {_fr[-1][1]} — "
                              "source schema changed materially")
            _tc = [r for r in _rs if _note(r).startswith("type-conflict")]
            if _tc:
                _probs.append(f"type-conflict note(s) on {len(_tc)} run(s); "
                              f"latest: {_note(_tc[-1])[:90]}")
            if any("quarantine DEGRADED" in _note(r) for r in _rs[-5:]):
                _probs.append("recent run with quarantine DEGRADED — unparsed "
                              "rows were kept in output (protection silently off)")
        # ---- bronze freshness (3) ---------------------------------------
        if check_bronze_freshness:
            try:
                _mx = (spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{_b}")
                       .agg(expr("max(insert_date)")).first()[0])
                if _mx is None:
                    _probs.append("bronze table exists but has NO rows")
                else:
                    _age = ((_now - _mx.replace(tzinfo=_mx.tzinfo or timezone.utc))
                            .total_seconds() / 3600)
                    if _age > stale_bronze_hours:
                        _probs.append(f"bronze STALE: newest dump "
                                      f"{str(_mx)[:19]} ({_age:.0f}h old)")
            except Exception:
                _probs.append("bronze table MISSING/unreadable")
        if _probs:
            _findings[_b] = _probs
            print(f"  🔴 {_b}:")
            for _p in _probs:
                print(f"       - {_p}")
        else:
            print(f"  🟢 {_b}: healthy")

    # ---- rebuild storms (7) ----------------------------------------------
    _window_start = _now - timedelta(days=rebuild_storm_window_days)
    _by_s = {}
    for r in _rows:
        if r.get("mode") == "REBUILD":
            _t = _dt(r.get("logged_at"))
            if _t and _t >= _window_start:
                _by_s.setdefault(r.get("silver_name"), []).append(r)
    for _s, _rs in sorted(_by_s.items()):
        if len(_rs) > rebuild_storm_max:
            _findings[f"rebuild-storm:{_s}"] = \
                [f"{len(_rs)} rebuilds in {rebuild_storm_window_days}d — "
                 "nightly nested drift re-firing the escape hatch; root-cause it"]
            print(f"  🔴 rebuild storm: {_s} — {len(_rs)} rebuilds in "
                  f"{rebuild_storm_window_days}d")

    # ---- quarantine growth (8) -------------------------------------------
    for _q, _label in ((f"{CATALOG}.{SILVER_SCHEMA}.parse_quarantine", "parse_quarantine"),
                       (f"{CATALOG}.{SILVER_SCHEMA}.null_pk_quarantine", "null_pk_quarantine")):
        try:
            if not spark.catalog.tableExists(_q):
                continue
            _tq = spark.table(_q)
            _total = _tq.count()
            if _label == "parse_quarantine":
                _recent = (_tq.filter(expr(
                    f"quarantined_at >= date_sub(current_date, 3)"))
                    .groupBy(expr("date(quarantined_at)").alias("d")).count()
                    .orderBy("d").collect())
                _hot = [f"{str(r['d'])[:10]}: {r['count']}" for r in _recent
                        if r["count"] > quarantine_growth_alert]
                if _hot:
                    _findings[f"quarantine-growth:{_label}"] = _hot
                    print(f"  🔴 {_label} growth > {quarantine_growth_alert}/day: "
                          f"{', '.join(_hot)} (total {_total:,})")
                else:
                    print(f"  🟢 {_label}: {_total:,} rows total, "
                          "recent days within budget")
            else:
                print(f"  🟢 {_label}: {_total:,} rows")
                if _total > 10000:
                    _findings[f"quarantine-growth:{_label}"] = \
                        [f"{_total:,} rows — review volume"]
        except Exception as _qe:
            print(f"  ℹ️ {_label} check skipped ({str(_qe)[:60]})")

    # ---- rung-mix flips (9) — 11/19/2026 ----------------------------------
    # CONSUMPTION for rung telemetry (detection without consumption was the
    # last known gap): _run_log.rung_stats is written by every parse since
    # 11/19/2026 — NULL before that = PRE-TELEMETRY, excluded from baselines
    # (never treated as zero). Baseline: trailing 7 days of rung_stats rows
    # per source. Flags, strongest first:
    #   (a) NEW RUNG VALUE — a value today the 7-day baseline never saw:
    #       vendor change on the repair-ladder axis ('exception' = the
    #       11/19 blanket fail-safe catching something new)
    #   (b) NULL-RUNG SPIKE — fail-safe/unparsed rows on a source whose
    #       baseline was clean
    #   (c) SHARE SHIFT — any rung's share of the mix moved more than
    #       rung_mix_shift_alert points
    # One telemetry run per source so far → 'establishing baseline', no alarm.
    _mix_rows = [r for r in _rows if r.get("rung_stats")]
    if _mix_rows:
        _by_s_mix = {}
        for r in _mix_rows:
            _by_s_mix.setdefault(r.get("silver_name"), []).append(r)
        _cut = _now - timedelta(days=7)
        for _s, _rs in sorted(_by_s_mix.items()):
            _rs.sort(key=lambda r: str(r.get("logged_at")))
            _latest = _rs[-1]
            try:
                _l_mix = json.loads(_latest["rung_stats"])
            except Exception:
                continue
            if not isinstance(_l_mix, dict) or not _l_mix:
                continue
            _base_rows = [r for r in _rs[:-1]
                          if (_dt(r.get("logged_at")) or _now) >= _cut]
            if not _base_rows:
                print(f"  🟡 rung mix {_s}: establishing baseline "
                      f"({len(_rs)} telemetry run(s)) — "
                      f"{json.dumps(_l_mix, sort_keys=True)}")
                continue
            _base_mix = {}
            for r in _base_rows:
                try:
                    _bm = json.loads(r["rung_stats"])
                except Exception:
                    continue
                for _k, _v in (_bm or {}).items():
                    _base_mix[_k] = _base_mix.get(_k, 0) + _v
            _l_tot = sum(_l_mix.values()) or 1
            _b_tot = sum(_base_mix.values()) or 1
            _pm = []
            _new = sorted(set(_l_mix) - set(_base_mix))
            if _new:
                _pm.append(f"NEW rung value(s) {_new} vs 7-day baseline "
                           f"{sorted(_base_mix)} — vendor/repair drift")
            if _l_mix.get("null") and not _base_mix.get("null"):
                _pm.append(f"null-rung (fail-safe/unparsed) {_l_mix['null']} row(s) "
                           "on a previously clean source")
            for _k in sorted(_l_mix):
                if _k in _new or (_k == "null" and not _l_mix[_k]):
                    continue
                _shift = (100.0 * _l_mix[_k] / _l_tot
                          - 100.0 * _base_mix.get(_k, 0) / _b_tot)
                if abs(_shift) > rung_mix_shift_alert:
                    _pm.append(f"rung {_k} share moved {_shift:+.0f} points")
            if _pm:
                _findings[f"rung-mix:{_s}"] = _pm
                print(f"  🔴 rung mix {_s}: " + "; ".join(_pm))
            else:
                print(f"  🟢 rung mix {_s}: stable vs 7-day baseline — "
                      f"{json.dumps(_l_mix, sort_keys=True)}")
    else:
        print("  🟡 rung mix: no telemetry rows yet "
              "(rung_stats NULL = pre-11/19 rows only)")

    # ---- event-time format + field budget (10, 11) — 11/19/2026 -----------
    # The two CHEAP cell-11 detectors promoted to NIGHTLY consumption (one
    # aggregation per silver parent / one run-log read). The functions stay
    # DEFINED in cell 11 — this cell only CALLS them (define vs. consume).
    #   10. EVENT-TIME FORMAT — survivorship orders STRING event times; mixed
    #       format classes/lengths in one column = the WRONG row can survive
    #       as 'current', silently. Fleet = SILVER_MERGE_KEYS parents.
    #   11. FIELD BUDGET — tables over field_budget fields discovered: where
    #       'every field is a column' should give way to a MAP/sidecar tail.
    try:
        for _s in sorted(set(SILVER_MERGE_KEYS.keys())):
            try:
                _etf = audit_event_time_format(_s, quiet=True)
                if _etf:
                    _findings[f"event-time-format:{_s}"] = _etf
            except Exception as _ete:
                print(f"  ℹ️ event-time format {_s} skipped ({str(_ete)[:60]})")
    except Exception as _ete2:
        print(f"  ℹ️ event-time format check skipped ({str(_ete2)[:60]})")
    try:
        _over_budget = check_field_budget(field_budget=field_budget)
        if _over_budget:
            _findings["field-budget"] = [
                f"{t}: over {field_budget} fields — MAP/sidecar tail time"
                for t in _over_budget]
    except Exception as _fbe:
        print(f"  ℹ️ field budget check skipped ({str(_fbe)[:60]})")

    _n = len(_findings)
    print(f"\n🩺 VERDICT: {'🔴 ' + str(_n) + ' finding(s) — investigate before trusting silver' if _n else '🟢 fleet clean'}")
    if not _n:
        print("   (silver is complete for the expected fleet, bronze is fresh, "
              "no storms, quarantine quiet)")
    return _findings


# Hint at load: this command exists; wire it as the job's last task.
print("🩺 engine_health_report() ready — run after the nightly; non-empty findings dict = alert")

# COMMAND ----------

# DBTITLE 1,# Raw Lineage Scan
# ============================================================================
# RAW LINEAGE SCAN 11/17/2026 — every column path from bronze payloads, datatypes attached
# ============================================================================
# Like the dry-run lineage tree, but from the RAW side and complete: scalars
# are printed too (the dry-run only lists array/object child tables), and every
# line carries its datatype. Arrays at the top level also show which silver
# child table they WOULD become (dry-run naming convention).


def _type_label(t):
    try:
        return t.simpleString()
    except Exception:
        return str(t)


def _lineage_walk(field, prefix, depth, silver_name=None):
    """Print one field path with its datatype; recurse into structs/arrays."""
    name = f"{prefix}.{field.name}" if prefix else field.name
    indent = "---" * depth
    t = field.dataType
    if isinstance(t, StructType):
        print(f"  # * {indent}{name} : struct")
        for sub in t.fields:
            _lineage_walk(sub, name, depth + 1, silver_name)
    elif isinstance(t, ArrayType):
        elem = t.elementType
        extra = f"  -> potential child: silver_{silver_name}_{field.name}" \
            if (silver_name and depth == 0) else ""
        print(f"  # * {indent}{name} : array<{_type_label(elem)}>{extra}")
        if isinstance(elem, StructType):
            for sub in elem.fields:
                _lineage_walk(sub, f"{name}[]", depth + 1, silver_name)
    else:
        print(f"  # * {indent}{name} : {_type_label(t)} (scalar)")


def raw_lineage_tree(bronze_name=None, silver_name=None, limit=200):
    """Scan bronze raw payloads and print EVERY column path with its datatype.

    Scalars, structs, and arrays all appear; arrays recurse into their element
    structs. Parse-only (limit rows sampled) — nothing is written.

    Args:
        bronze_name: one bronze table, or None to scan ALL 9 bronze tables.
        silver_name:  optional, to label potential child tables.
        limit:        sample rows used to infer the schema (default 200).
    """
    if bronze_name is None:
        bronze_schema = globals().get("BRONZE_SCHEMA", "bronze")
        names = sorted(t.name for t in spark.catalog.listTables(f"{CATALOG}.{bronze_schema}"))
    else:
        names = [bronze_name]

    for tname in names:
        print(f"\n{'=' * 80}")
        print(f"🧬 RAW LINEAGE — bronze.{tname}")
        print(f"{'=' * 80}")
        try:
            df = parse_bronze_table(tname, limit=limit,
                                    silver_name=silver_name or tname)
        except Exception as e:
            print(f"  !! {type(e).__name__}: {str(e)[:120]}")
            continue
        if df is None:
            print("  (no rows parsed — check the bronze table name)")
            continue
        for f in df.schema.fields:
            _lineage_walk(f, "", 0, silver_name or tname)
        scalars = sum(1 for f in df.schema.fields
                      if not isinstance(f.dataType, (ArrayType, StructType)))
        print(f"  -> {len(df.schema.fields)} top-level columns "
              f"({scalars} scalar, {len(df.schema.fields) - scalars} complex)")