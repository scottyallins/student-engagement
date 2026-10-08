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
    col, lit, from_json, schema_of_json, explode_outer, udf,
    get_json_object, regexp_replace, to_json, expr, create_map,
    md5, concat_ws, coalesce
)
from pyspark.sql.types import (
    StructType, ArrayType, StringType, IntegerType, LongType
)
from builtins import round, max, min
import time, re, json

# JSON repair UDF defined in cell 3 (teacher's parse_activity_final logic)

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
# JSON REPAIR UDF — one-size-fits-all JSON normalization
# ============================================================================
# Handles ALL Close CRM table shapes:
#   1. JSON_OBJECT wrapper → extracts + repairs inner JSON
#   2. "data" key → returns just the data array
#   3. Preserves apostrophes in words (e.g. Prospect's Name)
#   4. Nulls problem keys (description, notes, etc.) via scan-based string match
#   5. Converts single quotes → double quotes (delimiters only)
#   6. Also nulls problem keys programmatically as a safety net
#   7. Returns clean JSON string ready for from_json()


_PROBLEM_KEYS = {"description", "notes", "body_text", "subject", "text", "note"}


def _null_problem_keys(obj):
    """Recursively null out free-text keys on parsed objects (not strings)."""
    if isinstance(obj, dict):
        for k in obj:
            if k in _PROBLEM_KEYS:
                obj[k] = None
            else:
                _null_problem_keys(obj[k])
    elif isinstance(obj, list):
        for item in obj:
            _null_problem_keys(item)


def _null_problem_keys_in_string(s):
    """Null out problem key values in a single-quoted JSON string (before quote replacement).
    Uses scan-based approach: finds 'key': ' then scans for closing ' followed by next key.
    """
    for key in _PROBLEM_KEYS:
        marker = f"'{key}': '"
        idx = s.find(marker)
        while idx >= 0:
            val_start = idx + len(marker)
            # Find closing ' followed by next key: ', 'next_key':
            m = re.search(r"'\s*,\s*'\w+'\s*:", s[val_start:])
            if m:
                closing_pos = val_start + m.start()
                s = s[:idx] + f"'{key}': null" + s[closing_pos+1:]
            else:
                # Last key in object — look for ' followed by }
                m2 = re.search(r"'\s*}", s[val_start:])
                if m2:
                    closing_pos = val_start + m2.start()
                    s = s[:idx] + f"'{key}': null" + s[closing_pos+1:]
            idx = s.find(marker, idx + len(f"'{key}': null"))
    return s


def _parse_activity_final(raw):
    """Normalize Close CRM JSON and return a valid JSON string for ALL tables."""
    if not raw:
        return None

    try:
        outer = json.loads(raw.strip())

        # Custom-activity / close_crm_users path: JSON_OBJECT contains a stringified payload
        if isinstance(outer, dict) and "JSON_OBJECT" in outer:
            json_obj_str = outer["JSON_OBJECT"]

            if not isinstance(json_obj_str, str) or not json_obj_str:
                return None

            # Step 1: Try parsing as-is (already valid JSON)
            try:
                inner = json.loads(json_obj_str)
            except (json.JSONDecodeError, TypeError):
                # Step 2: Repair single-quoted JSON
                # 2a: Preserve apostrophes in words (e.g. Prospect's Name)
                repaired = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1APOSTROPHE\2", json_obj_str)
                # 2b: Null out problem keys (scan-based, before quote replacement)
                repaired = _null_problem_keys_in_string(repaired)
                # 2c: Replace remaining single quotes with double quotes, restore apostrophes
                repaired = repaired.replace("'", '"').replace("APOSTROPHE", "'")
                try:
                    inner = json.loads(repaired)
                except (json.JSONDecodeError, TypeError):
                    return None

            # Step 3: Null out problem keys programmatically (safety net for already-valid JSON)
            _null_problem_keys(inner)

            # Step 4: Unwrap "data" array if present
            if isinstance(inner, dict) and "data" in inner:
                return json.dumps(inner["data"])
            return json.dumps(inner)

        # Lead-activity path: return only the data array
        if isinstance(outer, dict) and "data" in outer:
            return json.dumps(outer["data"])

        return json.dumps(outer)

    except (json.JSONDecodeError, TypeError, AttributeError):
        return None


repair_json_udf = udf(StringType())(_parse_activity_final)

# COMMAND ----------

# DBTITLE 1,Schema Discovery & Bronze Parsing
# ============================================================================
# SCHEMA DISCOVERY & BRONZE PARSING
# ============================================================================


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


def parse_bronze_table(bronze_table, watermark=None):
    """Read Bronze, detect wrapper, repair via UDF (runs ONCE), materialize to temp table, parse."""
    df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")

    if watermark:
        df = df.filter(col("bronze_updated_at") > lit(watermark))
        print(f"  CDC: reading Bronze rows after {watermark}")

    # Quick check for data
    sample_row = df.select("raw_data").filter(col("raw_data").isNotNull()).first()
    if not sample_row:
        return None

    # Detect if data needs UDF repair (JSON_OBJECT wrapper, data key, or single-quote delimiters)
    _needs_repair = False
    try:
        _sample_parsed = json.loads(sample_row["raw_data"].strip())
        if isinstance(_sample_parsed, dict) and ("JSON_OBJECT" in _sample_parsed or "data" in _sample_parsed):
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
        df.select("insert_date", "bronze_updated_at", "raw_data_repaired") \
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

    # Random sample across multiple sections for better field coverage
    if total <= 200:
        sample_rows = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired").collect()
    else:
        sample_rows = df.filter(col("raw_data_repaired").isNotNull()) \
            .select("raw_data_repaired") \
            .sample(False, 200.0 / total) \
            .limit(200) \
            .collect()
    if not sample_rows:
        print(f"  ⏭️ SKIP: Could not find valid JSON after repair")
        return None

    # Merge schemas: flatten individual records from all samples into one flat array
    # This handles cases where each sample is itself a JSON array (e.g., lead_activites_raw
    # where UDF returns [record1, record2, ...] per row). Flattening avoids ARRAY<ARRAY<...>>
    # which schema_of_json can't merge correctly.
    json_samples = [r["raw_data_repaired"] for r in sample_rows if r["raw_data_repaired"]]
    all_records = []
    _udf_returns_array = False
    for s in json_samples:
        parsed_sample = json.loads(s)
        if isinstance(parsed_sample, list):
            _udf_returns_array = True
            all_records.extend(parsed_sample)
        else:
            all_records.append(parsed_sample)
    # schema_of_json returns ARRAY<STRING> for large inputs (> ~100KB).
    # Limit to 20 records — sufficient for schema discovery and stays under the threshold.
    _schema_records = all_records[:20] if len(all_records) > 20 else all_records
    json_array_str = json.dumps(_schema_records)
    try:
        raw_schema_str = spark.range(1).select(schema_of_json(lit(json_array_str)).alias("s")).collect()[0]["s"]
        # If schema_of_json returned ARRAY<STRING> (records too large), retry with a single record
        if raw_schema_str.upper() == "ARRAY<STRING>" and len(_schema_records) > 1:
            json_array_str = json.dumps(all_records[:1])
            raw_schema_str = spark.range(1).select(schema_of_json(lit(json_array_str)).alias("s")).collect()[0]["s"]
        # If wrapped in ARRAY<...>, extract the inner STRUCT using bracket-depth matching
        schema_upper = raw_schema_str.upper()
        if schema_upper.startswith("ARRAY<") and not schema_upper.startswith("ARRAY<ARRAY<"):
            depth = 0
            for i, c in enumerate(raw_schema_str):
                if c == '<':
                    depth += 1
                elif c == '>':
                    depth -= 1
                    if depth == 0:
                        raw_schema_str = raw_schema_str[6:i]
                        break
        # Parse DDL and deduplicate fields (instead of falling back to single sample)
        try:
            _test_st = StructType.fromDDL(raw_schema_str)
            _seen = set()
            _unique_fields = []
            for _f in _test_st.fields:
                _sn = _sanitize_col_name(_f.name)
                if _sn not in _seen:
                    _seen.add(_sn)
                    _unique_fields.append(_f)
            # Build clean StructType with ORIGINAL field names (avoids DDL parsing issues with dots in nested field names)
            deduped_st = StructType()
            for _f in _unique_fields:
                deduped_st = deduped_st.add(_f.name, _f.dataType, _f.nullable)
        except Exception:
            # fromDDL failed on complex nested schema — use DDL string directly
            # (from_json accepts DDL strings, so this is safe)
            deduped_st = raw_schema_str
    except Exception as e:
        # Fall back to single sample if merged schema has issues
        print(f"  ⚠️ Multi-sample schema failed ({str(e)[:80]}), falling back to single sample")
        sample = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired").first()
        sample_str = sample["raw_data_repaired"]
        # Check if single sample is a JSON array (set _udf_returns_array accordingly)
        try:
            _sample_parsed = json.loads(sample_str)
            if isinstance(_sample_parsed, list):
                _udf_returns_array = True
        except Exception:
            pass
        _fb_ddl = spark.range(1).select(schema_of_json(lit(sample_str)).alias("s")).collect()[0]["s"]
        # Strip ARRAY<...> wrapper if present (same as main path)
        _fb_upper = _fb_ddl.upper()
        if _fb_upper.startswith("ARRAY<") and not _fb_upper.startswith("ARRAY<ARRAY<"):
            depth = 0
            for i, c in enumerate(_fb_ddl):
                if c == '<':
                    depth += 1
                elif c == '>':
                    depth -= 1
                    if depth == 0:
                        _fb_ddl = _fb_ddl[6:i]
                        break
        try:
            deduped_st = StructType.fromDDL(_fb_ddl)
        except Exception:
            # fromDDL failed — use DDL string directly
            deduped_st = _fb_ddl

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

    # Handle top-level type: struct → star-expand, array → explode first
    parsed_type = df_parsed.schema["parsed"].dataType
    if isinstance(parsed_type, StructType):
        field_names = _dedup_names([_sanitize_col_name(f.name) for f in parsed_type.fields])
        select_exprs = [col("insert_date").alias("bronze_insert_date"), col("bronze_updated_at")]
        for f, safe_name in zip(parsed_type.fields, field_names):
            select_exprs.append(col(f"parsed.`{f.name}`").alias(safe_name))
        df_flat = df_parsed.select(*select_exprs)
    elif isinstance(parsed_type, ArrayType):
        print(f"  🔍 Top-level JSON is an array — exploding elements")
        df_exploded = df_parsed.withColumn("_element", explode_outer(col("parsed")))
        element_type = df_exploded.schema["_element"].dataType
        if isinstance(element_type, StructType):
            elem_names = _dedup_names([_sanitize_col_name(f.name) for f in element_type.fields])
            select_exprs = [col("insert_date").alias("bronze_insert_date"), col("bronze_updated_at")]
            for f, safe_name in zip(element_type.fields, elem_names):
                select_exprs.append(col(f"_element.`{f.name}`").alias(safe_name))
            df_flat = df_exploded.select(*select_exprs)
        else:
            df_flat = df_exploded.select(
                col("insert_date").alias("bronze_insert_date"),
                col("bronze_updated_at"),
                col("_element").alias("value")
            )
    else:
        df_flat = df_parsed.select(
            col("insert_date").alias("bronze_insert_date"),
            col("bronze_updated_at"),
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


def find_pk(df):
    """Find the best candidate for a primary key column."""
    for f in df.schema.fields:
        if f.name.lower() == "id" and isinstance(f.dataType, (StringType, IntegerType, LongType)):
            return f.name
    for f in df.schema.fields:
        if f.name.lower().endswith("_id") and isinstance(f.dataType, (StringType, IntegerType, LongType)):
            return f.name
    return None

# COMMAND ----------

# DBTITLE 1,Schema Printing
# # ============================================================================
# # SCHEMA PRINTING
# # ============================================================================


# def print_schema_summary(df, label):
#     """Print schema with data types, highlighting arrays and structs."""
#     # print(f"\n  📘 SCHEMA — {label}")
#     for f in df.schema.fields:
#         type_name = str(f.dataType)
#         if isinstance(f.dataType, ArrayType):
#             print(f"    🟠 ARRAY  {f.name}: {type_name}")
#         elif isinstance(f.dataType, StructType):
#             print(f"    🔵 STRUCT {f.name}: {type_name}")
#         else:
#             print(f"    🟢 {f.name}: {type_name}")


# def print_physical_schema(df, label):
#     """Print full physical schema tree."""
#     print(f"\n  🌳 PHYSICAL SCHEMA TREE — {label}")
#     df.printSchema()

# COMMAND ----------

# DBTITLE 1,Silver Writing
# ============================================================================
# SILVER WRITING
# ============================================================================


def add_record_hash(df):
    """Add an MD5 record_hash column over all business columns.

    Excludes bronze_insert_date, bronze_updated_at (metadata that changes
    per load) and record_hash itself. The hash provides a deterministic
    fingerprint for deduplication and change detection (spec Phase 1).
    """
    exclude = {"bronze_insert_date", "bronze_updated_at", "record_hash"}
    hash_cols = [c for c in df.columns if c not in exclude]
    if not hash_cols:
        return df
    return df.withColumn(
        "record_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in hash_cols]))
    )


def write_silver(df, table_name):
    """Write DataFrame to silver table with MD5 record_hash. CDC mode if _CDC_WATERMARK is set."""
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    # Add MD5 hash over all business columns (spec Phase 1: dedup traceability)
    df = add_record_hash(df)

    if _CDC_WATERMARK is not None:
        try:
            spark.sql(f"DESCRIBE TABLE {full}")
            wm_str = _CDC_WATERMARK.strftime("%Y-%m-%d %H:%M:%S")
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


def explode_to_child(df, parent_name, array_col, carry_cols, depth=0, num_path="", parent_pk=None, dry_run=False):
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

    grandchild_idx = 0
    for ca in child_arrays:
        grandchild_idx += 1
        grandchild_num = f"{num_path}_{grandchild_idx}"
        explode_to_child(df_child, child_name, ca, new_carry, depth + 1, grandchild_num, parent_pk=element_pk, dry_run=dry_run)

# COMMAND ----------

# DBTITLE 1,Custom Field Extraction
# ============================================================================
# CUSTOM FIELD EXTRACTION (dry_run — prints child tables without writing)
# ============================================================================


def extract_custom(df, parent_name, carry_cols, num_path, parent_pk=None, dry_run=False):
    """Extract custom struct AND custom_cf_ columns.

    When dry_run=True: prints _custom and named custom array table info without writing.
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
    grandchild_idx = 0

    # -- 1. Build _custom table (regular scalar fields from custom struct only) --
    if has_custom_struct and struct_scalar_fields:
        select_exprs = list(carry) + [
            col(f"custom.`{orig}`").alias(safe) for orig, safe in struct_scalar_fields
        ]
        df_custom = df.select(*select_exprs)
        df_custom = flatten_structs(df_custom, skip_custom=False)

        if dry_run:
            print(f"    📦 {custom_name} ({len(df_custom.columns)} cols): {df_custom.columns}")
        else:
            print(f"\n  📦 CUSTOM TABLE — {custom_name}")
            write_silver(df_custom, custom_name)
        _lineage_tree.append((num_path, 1, custom_name))

        # Explode arrays that survived flatten in the custom table
        custom_pk = find_pk(df_custom)
        custom_carry = list(dict.fromkeys(
            [c for c in carry_cols if c in df_custom.columns] +
            [c for c in df_custom.columns if c.startswith(f"{parent_name}_")] +
            ([custom_pk] if custom_pk else [])
        ))
        _, custom_arrays_in_table, _ = categorize_fields(df_custom)
        for ca in custom_arrays_in_table:
            grandchild_idx += 1
            grandchild_num = f"{num_path}_{grandchild_idx}"
            explode_to_child(df_custom, custom_name, ca, custom_carry, depth=1, num_path=grandchild_num,
                             parent_pk=(custom_pk if custom_pk and custom_pk not in carry_cols else None),
                             dry_run=dry_run)

    # -- 1a. Explode each named custom array into its own table --
    if has_custom_struct and struct_array_fields:
        array_carry_names = []
        array_carry_exprs = []
        for c in dict.fromkeys(carry_cols):
            if c in df.columns:
                if c == parent_pk:
                    renamed = f"{parent_name}_{c}"
                    array_carry_names.append(renamed)
                    array_carry_exprs.append(col(c).alias(renamed))
                else:
                    array_carry_names.append(c)
                    array_carry_exprs.append(col(c))

        for orig_name, safe_name in struct_array_fields:
            grandchild_idx += 1
            grandchild_num = f"{num_path}_{grandchild_idx}"
            df_one_array = df.select(*array_carry_exprs, col(f"custom.`{orig_name}`").alias(safe_name))
            print(f"    📦 NAMED CUSTOM ARRAY — {custom_name}_{safe_name}")
            explode_to_child(df_one_array, custom_name, safe_name, array_carry_names,
                           depth=1, num_path=grandchild_num, parent_pk=parent_pk, dry_run=dry_run)

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


def process_table(bronze_name, silver_name=None, only_arrays=None):
    """Full Bronze → Silver pipeline: parse, flatten structs, write parent table.
    Child tables are discovered and printed (dry_run) but NOT written.

    Args:
        bronze_name:  Bronze source table name
        silver_name:  Silver target table name (defaults to bronze_name)
        only_arrays:  List of array column names to explode. None = all (default).
                      [] = don't explode any arrays. ["contacts"] = only explode contacts.
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

    df_flat = parse_bronze_table(bronze_name, watermark=_CDC_WATERMARK)
    if df_flat is None:
        # CDC found no new data — print full existing silver table details
        if _CDC_WATERMARK:
            print(f"  ✅ CDC: No new rows since {_CDC_WATERMARK}")
            try:
                tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").collect()
                related = []
                for t in tables:
                    tname = t['tableName']
                    if tname == silver_name or tname.startswith(f"{silver_name}_"):
                        try:
                            cnt = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{tname}").count()
                            related.append((tname, cnt))
                        except:
                            related.append((tname, -1))

                print(f"\n  📊 Existing silver tables ({len(related)}):")
                for tname, cnt in related:
                    print(f"     {tname}: {cnt:,} rows")

                # Print physical schema and summary for each table
                for tname, cnt in related:
                    try:
                        tdf = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{tname}")
                        # print_physical_schema(tdf, tname)
                        # print_schema_summary(tdf, tname)
                    except:
                        pass

                # Auto-discover lineage tree from existing silver table schemas
                table_names = [tname for tname, _ in related]
                _lineage_tree = _discover_lineage_tree(silver_name, pipeline, table_names)
                print_lineage_tree()
            except Exception as e:
                print(f"  ⚠️ Could not read existing silver tables: {str(e)[:80]}")

            elapsed = time.time() - start
            print(f"  ⏱️ Completed in {elapsed:.1f}s (CDC — no new data)")
            print(f"{'='*80}")
        _CDC_WATERMARK = None
        return

    # Flatten structs (keep custom struct as-is on parent)
    df_flat = flatten_structs(df_flat, skip_custom=True)

    # Find PK (carry was used for child tables — retained for re-enablement)
    pk = find_pk(df_flat)
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