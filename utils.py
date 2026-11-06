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

# JSON repair UDF defined in cell 3.
# NOTE: _PROBLEM_KEYS (in cell 3) is DOMAIN-SPECIFIC to Close CRM, not universal.
# It was discovered empirically by profiling source data — see cell 3 for methodology.

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
# One pre-existing limitation to be aware of, unrelated to the converter: a multi-key single-quoted outer holding a double-quoted stringified inner still can't be rescued (rung 2 only handles single-key wrappers; the quote flip corrupts nested doubles). That shape was already NULL before this change — current bronze has zero rows in it.============================================================================
# ESCALATION LADDER — first success wins, fail-safe NULL at the end.
# This ladder is the accumulated reasoning from the repair design sessions;
# keep it in sync with _parse_activity_final:
#   Rung 1. Direct parse (fast path). Clean double-quoted JSON needs no
#      surgery — apostrophes are legal inside values, so free text survives
#      untouched. Wrapper unwrap happens AFTER any successful parse (a
#      wrapper IS valid JSON; you cannot extract it from a string you
#      cannot parse without fragile regex surgery) — see _detect_and_unwrap.
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
#   Rung 5. NULL — fail-safe drop, visible in the engine's parse % line.
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

_PROBLEM_KEYS = {"description", "notes", "body_text", "subject", "text", "note"}

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


def _repair_single_quotes(s, token="(*APOSTROPHE*)"):
    """Try clean repair first. If parse fails: wrap problem keys, tokenize all ' inside them with token, then flip quotes.
    """
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
        return attempt          # clean success
    except (json.JSONDecodeError, TypeError):
        pass

    # 3. Wrap & tokenize problem keys, then flip quotes
    wrapped = _wrap_problem_keys_in_string_and_tokenize(protected, token=token)
    attempt = wrapped.replace("'", '"').replace("APOSTROWORD", "'")
    json.loads(attempt)         # validate
    return attempt


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

def _parse_activity_final(raw):
    """
    Normalize JSON and return a valid JSON string for ALL tables.
    Escalation ladder — first success wins, fail-safe NULL at the end:
      1. Direct parse (clean rows are left untouched)
      2. String-level unwrap of a single-key single-quoted wrapper, then
         the FULL ladder recursively on the inner string (insurance rung:
         a single-quoted outer holding double-quoted inner JSON can never
         be reached by the quote flip)
      3. APOSTROWORD-protected quote flip (word apostrophes preserved), plus
         controlled None/True/False -> null/true/false conversion outside
         quoted strings (_convert_python_literals — spec: null handling)
      4. Wrap problem keys + (*APOSTROPHE*) token (non-word apostrophes
         preserved; reverted in object space after parse)
      5. NULL — fail-safe drop, visible in the engine's parse %
    Wrapper detection is by type inspection, not by hardcoded key names, and
    runs post-parse recursively (see _detect_and_unwrap).
    Problem keys: _PROBLEM_KEYS is domain-specific.
    """
    if not raw:
        return None
    repaired_row = False
    token = "(*APOSTROPHE*)"
    try:
        outer = json.loads(raw.strip())
    except (json.JSONDecodeError, TypeError):
        # Rung 2 — unwrap a single-key single-quoted wrapper at the STRING
        # level and run the full ladder on the inner. Strictly additive
        # insurance: no match, an unparseable inner, or any unexpected
        # error falls through to the proven quote-repair rungs below, so
        # this rung can only rescue rows, never break previously-working
        # ones.
        try:
            _inner = _extract_single_quoted_wrapper(raw.strip())
            if _inner is not None:
                _nested = _parse_activity_final(_inner)
                if _nested is not None:
                    return _nested
        except Exception:
            pass  # insurance rung must never break the proven path — fall through
        try:
            repaired = _repair_single_quotes(raw.strip(), token=token)
            outer = json.loads(repaired)
            repaired_row = True
        except (json.JSONDecodeError, TypeError):
            return None
    # Universal wrapper unwrap (unchanged)
    unwrapped = _detect_and_unwrap(outer)
    # Restore apostrophe tokens after unwrapping problem keys
    if repaired_row:
        _unwrap_problem_keys(unwrapped)
        _restore_apostrophe_tokens(unwrapped, token=token)
    return json.dumps(unwrapped)

from pyspark.sql.types import StringType
repair_json_udf = udf(StringType())(_parse_activity_final)




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


def parse_bronze_table(bronze_table, watermark=None, limit=None, silver_name=None):
    """Read Bronze, dedup snapshot re-dumps, detect wrapper, repair via UDF (runs ONCE), materialize to temp table, parse."""
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
    _SCHEMA_SAMPLE = 200
    _batch_df = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired")
    _sample_df = _batch_df
    if total > _SCHEMA_SAMPLE:
        _sample_df = _batch_df.sample(False, _SCHEMA_SAMPLE / total).limit(_SCHEMA_SAMPLE)
    if total < _SCHEMA_SAMPLE:
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


def write_silver(df, table_name):
    """Write DataFrame to silver table. CDC mode if _CDC_WATERMARK is set."""
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    if _CDC_WATERMARK is not None:
        try:
            spark.sql(f"DESCRIBE TABLE {full}")
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
        except Exception:
            pass

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