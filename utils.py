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

# DBTITLE 1,Configuration
# ============================================================================
# SILVER LAYER - CONFIGURATION
# ============================================================================
# Bronze: crm_ingestion.bronze  (9 raw tables, full-loaded from Postgres)
# Silver: crm_ingestion.silver  (parsed, flattened, exploded)
# ============================================================================

from pyspark.sql.functions import (
    col, lit, from_json, schema_of_json, explode, explode_outer, udf,
    get_json_object, regexp_replace, to_json, expr, create_map,
    md5, concat_ws, coalesce, when, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, ArrayType, StringType, IntegerType, LongType
)
from builtins import round, max, min
import time, re, json

# JSON repair UDF defined in the 'JSON Repair UDF' cell below (single source of truth — do not duplicate).
# NOTE: _PROBLEM_KEYS (in the JSON Repair UDF cell) is DOMAIN-SPECIFIC to Close CRM, not universal.
# It was discovered empirically by profiling source data — see that cell for methodology.

CATALOG = "crm_ingestion"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Module-level state variables
_TEMP_TABLES = []
_pipeline_counter = 0
_lineage_tree = []  # List of (number, depth, table_name)
_CDC_WATERMARK = None  # Set by process_table in CDC mode; read by write_silver

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

print(f"✅ Config loaded: {CATALOG}.{BRONZE_SCHEMA} → {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,JSON Repair UDF
# ============================================================================
# JSON REPAIR UDF — universal normalization with type-based wrapper detection
# RESOLVED Nov 9 2026 (was: pre-existing limitation, unrelated to the converter): a multi-key single-quoted outer holding a double-quoted stringified inner is now rescued — NOT by generalizing rung 2's anchored regex (multi-key breaks its boundedness), but by extending the rung-4 token scheme: the stringified inner's double quotes are tokenized (*DQUOTE*) in text space before the flip, then reverted in OBJECT space after parse, which turns the value back into a parseable '{...}' string that the existing post-parse wrapper detection (Rule 1) unwraps as usual. Current bronze has zero rows in this shape — insurance for future sources. See _protect_stringified_inner_quotes for the remaining documented residual (single-quoted inner inside a double-quoted value).
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
#      object space too, before wrapper detection. Both tokens assume the
#      literal strings (*APOSTROPHE*)/(*DQUOTE*) never occur in source
#      data — same collision assumption the scheme always had.
#   Rung 5. Escape other breakers (string-level insurance, reached only
#      after every repair above has failed on this row): quote unquoted
#      KEYS (string-aware, never inside string values), escape raw control
#      characters inside strings (pure rescue — parsed values unchanged,
#      a tab stays a tab), quote unquoted values that start with a digit
#      but contain letters (e.g. 12abc), double bare backslashes not
#      already part of a valid escape, and remove trailing commas before
#      } or ]. The former residual — digits-only invalid numbers (007) and
#      bare dates (2026-11-06) — is now OPT-IN via the module switches
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

def _wrap_problem_keys_in_string_and_tokenize(s, token="(*APOSTROPHE*)"):
    """Wrap problem-key values and replace every ' in them with the token.
    Example: 'key': 'O'Reilly' → 'key': ['O(*APOSTROPHE*)Reilly']
    """
    for key in _PROBLEM_KEYS:
        # Pattern: matches the problem key up to the next closing ', next key, or }
        pattern = rf"('{key}'):\s*'(.*?)'(?=\s*,\s*'\w+'\s*:|\s*}})"
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
    s = re.sub(r'(?<!\\)\\(?!["\\/bfnrtu])', r'\\\\', s)
    # Remove trailing commas before } or ]
    s = re.sub(r',\s*([}\]])', r'\1', s)
    return s


def _repair_single_quotes(s, token="(*APOSTROPHE*)", track=None):
    """Escalation inside the quote-repair path.
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
    # 1. Protect word apostrophes globally first (does not use token)
    protected = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1APOSTROWORD\2", s)

    # 1b. Controlled Python-literal conversion (spec: null handling) —
    #     unquoted None/True/False -> null/true/false, outside quoted
    #     strings only. Quoted 'None' strings are source data and survive.
    protected = _convert_python_literals(protected)

    # 2. Try parse without wrap/token
    attempt = protected.replace("'", '"').replace("APOSTROWORD", "'")
    try:
        json.loads(attempt)
        track["rung"] = 3
        return attempt          # clean success
    except (json.JSONDecodeError, TypeError):
        pass

    # 3. Tokenize stringified-inner double quotes, wrap & tokenize problem
    # keys, then flip quotes. _protect_stringified_inner_quotes is a no-op
    # for rows with no double-quoted stringified values, so this attempt is
    # byte-identical to the old one for every such row.
    dquote_protected = _protect_stringified_inner_quotes(protected)
    wrapped = _wrap_problem_keys_in_string_and_tokenize(dquote_protected, token=token)
    attempt = wrapped.replace("'", '"').replace("APOSTROWORD", "'")
    try:
        json.loads(attempt)
        track["rung"] = 4
        return attempt
    except (json.JSONDecodeError, TypeError):
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


def _detect_and_unwrap(parsed):
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
    """
    if isinstance(parsed, dict):
        # Rule 1: Check for stringified inner JSON wrapper (any value that looks like JSON)
        for key, val in parsed.items():
            if isinstance(val, str) and val.strip() and val.strip()[0] in ('{', '['):
                # Try parsing as-is first (might already be valid JSON)
                try:
                    inner = json.loads(val.strip())
                    return _detect_and_unwrap(inner)
                except (json.JSONDecodeError, TypeError):
                    # Try repairing single quotes and re-parsing
                    try:
                        repaired = _repair_single_quotes(val)
                        inner = json.loads(repaired)
                        # Inner repair may have wrapped/tokenized problem keys —
                        # restore HERE: the outer repaired_row flag in
                        # _parse_activity_final never sees this nested repair,
                        # so tokens would otherwise leak into the output.
                        _unwrap_problem_keys(inner)
                        _restore_apostrophe_tokens(inner)
                        # DQUOTE tokens from the extended rung 4 revert here
                        # too, before recursion — same leak-prevention logic.
                        _restore_dquote_tokens(inner)
                        return _detect_and_unwrap(inner)
                    except (json.JSONDecodeError, TypeError):
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
                return _detect_and_unwrap(val)
        # No wrapper detected — actual record
        return parsed
    elif isinstance(parsed, list):
        # Recurse into each element — an element may itself have a wrapper.
        # For example [{"payload": "{ 'records': [...] }"}] needs each element unwrapped.
        # Clean records (dicts with multiple keys, no stringified values) pass through
        # unchanged — _detect_and_unwrap returns them as-is via Rule 3.
        return [_detect_and_unwrap(item) for item in parsed]
    return parsed

def _parse_activity_final(raw, return_flags=False):
    """
    Normalize JSON and return a valid JSON string for ALL tables.
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
    repaired_row = False
    token = "(*APOSTROPHE*)"
    track = {}
    s = raw.strip()
    try:
        outer = json.loads(s)
        track["rung"] = 1
    except (json.JSONDecodeError, TypeError):
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
        except (json.JSONDecodeError, TypeError):
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
                    _nested = _parse_activity_final(_inner)
                    if _nested is not None:
                        track["rung"] = 2
                        if return_flags:
                            try:
                                _obj = json.loads(_nested)
                            except (json.JSONDecodeError, TypeError):
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
                repaired = _repair_single_quotes(normalized, token=token, track=track)
                outer = json.loads(repaired)
                # Revert (*DQUOTE*) tokens in OBJECT space BEFORE wrapper
                # detection: a protected stringified inner becomes a parseable
                # '{...}' string again and Rule 1 unwraps it — the multi-key
                # rescue composes with the existing post-parse machinery.
                if "(*DQUOTE*)" in repaired:
                    _restore_dquote_tokens(outer)
                repaired_row = True
            except (json.JSONDecodeError, TypeError):
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




# COMMAND ----------

# DBTITLE 1,Schema Discovery & Bronze Parsing
# ====================================================================
# SCHEMA DISCOVERY & BRONZE PARSING
# ====================================================================


def get_silver_watermark(silver_table):
    """Get last CDC watermark from Silver parent table.
    Returns MAX(bronze_updated_at) or None if table doesn't exist.
    """
    try:
        result = spark.sql(f"""
            SELECT MAX(bronze_updated_at) as max_date
            FROM {CATALOG}.{SILVER_SCHEMA}.{silver_table}
        """).collect()[0]
        return result['max_date']
    except Exception:
        return None


def parse_bronze_table(bronze_table, watermark=None, limit=None, silver_name=None, schema_sample=200):
    """Read Bronze, dedup snapshot re-dumps, detect wrapper, repair via UDF (runs ONCE), materialize to temp table, parse.

    schema_sample: how many (random) rows schema_of_json_agg sees. Default 200
    is fine for CDC batches (mergeSchema appends tolerate discovery drift).
    None = discover over ALL distinct rows - use for one-off full rebuilds so
    rare historical fields survive: a drifted sample can miss fields present
    on <0.5% of rows (Nov 10 2026: 'SMS Opt-Out' missed -> the REPLACE tried
    to drop SMS_Opt_Out and Delta rejected it because the table's
    workloadBasedColumns stats property pins that column).
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
                _anti = df.join(_silver_tbl.select("payload_hash"), on="payload_hash", how="left_anti")
                _after = _anti.count()  # force full analysis on the temp variable FIRST
                df = _anti              # commit only after the action succeeded
                if _after < _dedup_total:
                    print(f"  🔁 CDC dedup: {_dedup_total:,} → {_after:,} rows (versions already in silver skipped)")

    # Quick check for data
    sample_row = df.select("raw_data").filter(col("raw_data").isNotNull()).first()
    if not sample_row:
        return None

    # Detect if data needs UDF repair — universal wrapper detection by type inspection
    # Checks for: (a) unparseable JSON (single quotes), (b) stringified inner JSON wrapper,
    # (c) single-key array wrapper. Does NOT check for specific key names.
    _needs_repair = False
    try:
        _sample_parsed = json.loads(sample_row["raw_data"].strip())
        if isinstance(_sample_parsed, dict):
            # Check for stringified inner JSON (any value starting with '{' or '[')
            for _val in _sample_parsed.values():
                if isinstance(_val, str) and _val.strip() and _val.strip()[0] in ('{', '['):
                    _needs_repair = True
                    break
            # Check for single-key array wrapper
            if not _needs_repair and len(_sample_parsed) == 1:
                _val = next(iter(_sample_parsed.values()))
                if isinstance(_val, list):
                    _needs_repair = True
    except (json.JSONDecodeError, TypeError):
        # Can't parse as standard JSON — likely single-quote delimiters, needs UDF
        _needs_repair = True

    if _needs_repair:
        # Parse + repair via UDF (handles JSON_OBJECT wrapper, single quotes, data array)
        df = df.withColumn("raw_data_repaired", repair_json_udf(col("raw_data")))

        # NOTE: custom. keys are intentionally kept as nested struct fields so the
        # custom object can be extracted into its own child table (extract_custom)

        # Write repaired data to temp table — UDF runs ONCE during write, then all reads are UDF-free
        full_temp = f"{CATALOG}.{SILVER_SCHEMA}._tmp_{bronze_table}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
        spark.sql(f"DROP TABLE IF EXISTS {full_temp}")
        df.select("insert_date", "bronze_updated_at", "payload_hash", "raw_data_repaired") \
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

    total = df.count()
    if total == 0:
        print(f"  ⏭️ SKIP: No data after repair")
        return None
    print(f"  Source: {total:,} rows")

    # Detect if UDF returns arrays (each row is a JSON array of records) vs single objects
    _first_row = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired").first()
    if not _first_row:
        print(f"  ⏭️ SKIP: Could not find valid JSON after repair")
        return None

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
        _sample_df = _batch_df.sample(False, _SCHEMA_SAMPLE / total).limit(_SCHEMA_SAMPLE)
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
                _extra = (_full_bronze.sample(False, min(1.0, _need * 3.0 / _full_total)).limit(_need)
                    .select(repair_json_udf(col("raw_data")).alias("raw_data_repaired"))
                    .filter(col("raw_data_repaired").isNotNull()))
                _sample_df = _batch_df.unionByName(_extra)
        except Exception as _aug_e:
            print(f"  ⚠️ Schema sample augmentation unavailable ({str(_aug_e)[:60]})")

    _sample_count = _sample_df.count()
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
                _level_df = (_level_df
                    .select(explode(from_json(col("raw_data_repaired"), "ARRAY<STRING>")).alias("raw_data_repaired"))
                    .where(col("raw_data_repaired").isNotNull())
                    .sample(False, 0.25)
                    .limit(200))
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

    # Parse (fast — reads from temp table, only from_json is computed)
    if isinstance(deduped_st, str):
        # DDL string fallback — from_json accepts DDL strings directly
        _from_json_schema = f"ARRAY<{deduped_st}>" if _udf_returns_array else deduped_st
    else:
        _from_json_schema = ArrayType(deduped_st) if _udf_returns_array else deduped_st
    df_parsed = df.withColumn("parsed", from_json(col("raw_data_repaired"), _from_json_schema))
    valid = df_parsed.filter(col("parsed").isNotNull()).count()
    print(f"  Parsed: {valid:,} ({valid/total*100:.1f}%)")

    if valid == 0:
        return None

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
    return df_flat

# COMMAND ----------

# DBTITLE 1,Field Utilities
# ============================================================================
# FIELD UTILITIES
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
    """Ensure all names are unique by appending _1, _2, etc. for duplicates."""
    seen = {}
    result = []
    for name in names:
        if name in seen:
            seen[name] += 1
            result.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
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

    # --- Validate each candidate: non-null + unique on a sample ---
    sample = df.limit(sample_size)
    total = sample.count()

    for priority, col_name in candidates:
        non_null = sample.filter(col(col_name).isNotNull()).count()
        if non_null == 0:
            continue
        distinct = sample.select(col_name).distinct().count()
        if non_null == distinct and non_null == total:
            return col_name

    # --- All candidates failed validation — return first anyway as last resort ---
    return candidates[0][1]

# COMMAND ----------

# DBTITLE 1,Silver Writing
# ============================================================================
# SILVER WRITING
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


def _merge_silver(df, table_name, pk):
    """CDC upsert by PK: UPDATE existing ids in place, INSERT new ids.

    Source batch rules:
      * deduped to latest-version-per-PK first (a multi-dump batch holding
        two versions of one id would make MERGE fail with multiple matches);
        ties broken by bronze_insert_date / bronze_updated_at DESC NULLS
        LAST, then payload_hash DESC — the same ordering as the parent rebuild;
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
    w = Window.partitionBy(pk).orderBy(
        col("bronze_insert_date").desc_nulls_last(),
        col("bronze_updated_at").desc_nulls_last(),
        col("payload_hash").desc())
    src = (df.filter(col(pk).isNotNull())
             .withColumn("_rn", row_number().over(w))
             .filter("_rn = 1").drop("_rn")
             .unionByName(df.filter(col(pk).isNull())))

    # Schema evolution: add source columns the target lacks (nullable).
    src_cols = [f.name for f in df.schema.fields]
    tgt_cols = set(spark.table(full).columns)
    for f in df.schema.fields:
        if f.name not in tgt_cols:
            spark.sql(f"ALTER TABLE {full} ADD COLUMN `{f.name}` {f.dataType.simpleString()}")

    # Explicit-column MERGE so a schema mismatch between batch and target
    # cannot make the write fail after the ALTER step above.
    view = f"_merge_src_{table_name}"
    src.createOrReplaceTempView(view)
    set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in src_cols)
    ins_cols = ", ".join(f"`{c}`" for c in src_cols)
    ins_vals = ", ".join(f"s.`{c}`" for c in src_cols)
    try:
        spark.sql(f"""
            MERGE INTO {full} AS t
            USING {view} AS s
            ON t.`{pk}` = s.`{pk}`
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """)
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")


def write_silver(df, table_name):
    """Write DataFrame to silver table. CDC mode if _CDC_WATERMARK is set.

    CDC mode: MERGE-by-PK upsert for tables with a SILVER_MERGE_KEYS entry
    (one-row-per-id contract); legacy delete+append otherwise. Full-load mode
    (_CDC_WATERMARK=None): atomic full overwrite (overwriteSchema=true).
    """
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    if _CDC_WATERMARK is not None:
        # Upsert-by-PK when the table has a pinned merge key: changed records
        # UPDATE in place, new records INSERT. Errors PROPAGATE — never fall
        # through to the full overwrite below (it would replace the whole
        # table with just the CDC batch).
        _mk = SILVER_MERGE_KEYS.get(table_name)
        if _mk and _mk in df.columns:
            _merge_silver(df, table_name, _mk)
            cnt = spark.table(full).count()
            print(f"  ✅ CDC MERGE: {table_name}: {cnt:,} rows (upsert by {_mk})")
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
            # Keep microseconds: the DELETE window must exactly match the parse filter
            # (bronze_updated_at > watermark). Truncating to seconds made the DELETE
            # wider than the read, silently dropping the previous batch's newest rows.
            wm_str = _CDC_WATERMARK.strftime("%Y-%m-%d %H:%M:%S.%f")
            spark.sql(f"DELETE FROM {full} WHERE bronze_updated_at > '{wm_str}'")
            df.write.format("delta").mode("append") \
                .option("mergeSchema", "true") \
                .option("delta.columnMapping.mode", "name") \
                .option("delta.minReaderVersion", "2") \
                .option("delta.minWriterVersion", "5") \
                .saveAsTable(full)
            cnt = spark.table(full).count()
            print(f"  \u2705 CDC: {table_name}: {cnt:,} rows (delete+insert)")
            return cnt

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

# DBTITLE 1,Array Explosion
# ============================================================================
# ARRAY EXPLOSION (dry_run mode — prints schema without writing tables)
# ============================================================================


def explode_to_child(df, parent_name, array_col, carry_cols, depth=0, num_path="", parent_pk=None, dry_run=False, recurse=True):
    """Explode an array column into a child table. Recurse on nested arrays until scalar.

    When dry_run=True: prints the child table name and columns without writing.
    """
    child_name = f"{parent_name}_{array_col}"

    df_child = df.withColumn("_element", explode_outer(col(array_col)))
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
        write_silver(df_child, child_name)

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

# COMMAND ----------

# DBTITLE 1,Custom Field Extraction
# ============================================================================
# CUSTOM FIELD EXTRACTION — custom_fields MAP + _custom table (no child tables)
# ============================================================================


def extract_custom(df, parent_name, carry_cols, num_path, parent_pk=None, dry_run=False):
    """Extract custom struct AND custom_cf_ columns.

    - cf_ fields (inside custom struct OR as top-level custom_cf_ columns) → custom_fields MAP
    - Non-cf_ scalar fields → _custom child table (one table, no further children)
    - Non-cf_ array fields → array columns in _custom table (preserved for future use, NOT exploded)
    The custom_fields MAP is always built on the parent regardless of dry_run.
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
            write_silver(df_custom, custom_name)
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

# COMMAND ----------

# DBTITLE 1,Orchestrator
# ============================================================================
# ORCHESTRATOR
# ============================================================================


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
        except:
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
    sample = df.limit(sample_size).toPandas()
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


def process_table(bronze_name, silver_name=None, only_arrays=None, flatten=False):
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
                        except:
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
                pk = find_pk(df_sample, table_name=silver_name)
                carry = list(dict.fromkeys(["bronze_insert_date", "bronze_updated_at"] + ([pk] if pk else [])))
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

    # Find PK (carry was used for child tables — retained for re-enablement)
    pk = find_pk(df_flat, table_name=silver_name)
    carry = list(dict.fromkeys(["bronze_insert_date", "bronze_updated_at"] + ([pk] if pk else [])))

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
    write_silver(df_flat, silver_name)

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


print("✅ Silver engine loaded")
print("   Usage: process_table('bronze_table_name', 'silver_table_name')")

# COMMAND ----------

# DBTITLE 1,Raw Lineage Scan
# ============================================================================
# RAW LINEAGE SCAN — every column path from bronze payloads, datatypes attached
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