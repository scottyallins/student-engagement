# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DEA Student Engagement SILVER FINAL
# MAGIC
# MAGIC Bronze → Silver transformation with **auto-discovered schemas**.
# MAGIC
# MAGIC * No hardcoded schemas — uses `schema_of_json()` for dynamic discovery
# MAGIC * State-machine JSON repair (preserves apostrophes, 14ms/row)
# MAGIC * Custom object → own child table (not mixed into parent)
# MAGIC * Recursive array explosion (no depth limit) — all nested objects become tables
# MAGIC * Lineage numbering: `1_`, `1_1_`, `1_2_1_`, etc. (self-numbering hierarchy)
# MAGIC * Carry columns use parent table name (no `parent_` prefix)
# MAGIC * Physical schema tree (`printSchema`) printed for all tables
# MAGIC * Lineage tree printed at end of each pipeline
# MAGIC * Reads from `crm_ingestion.bronze` → writes to `crm_ingestion.silver`

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
    get_json_object, regexp_replace, to_json
)
from pyspark.sql.types import (
    StructType, ArrayType, StringType, IntegerType, LongType
)
from builtins import round, max, min
import time, re, json

# JSON repair library — handles single quotes, Python literals, spacing issues
try:
    from json_repair import repair_json as _jr
    print("json_repair already installed")
except ImportError:
    %pip install json-repair --quiet
    from json_repair import repair_json as _jr
    print("json_repair installed")

CATALOG = "crm_ingestion"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

print(f"✅ Config loaded: {CATALOG}.{BRONZE_SCHEMA} → {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Silver Engine
# ============================================================================
# SILVER ENGINE — Auto-discover, parse, flatten structs, explode arrays
# ============================================================================
# No hardcoded schemas. No hardcoded field names. No STRUCT columns in output.
# Uses schema_of_json() for dynamic schema discovery from a sample row.
# Recursively explodes ALL nested arrays until scalar (no depth limit).
# ============================================================================

# (STOP_PREFIXES removed — custom fields are now extracted into their own table)


def _targeted_repair_impl(raw):
    """State-machine repair: single-quoted JSON → double-quoted.
    Preserves apostrophes inside values (e.g. O'hare) by looking ahead
    to distinguish delimiter quotes from content quotes.
    14ms/row — 13x faster than json_repair library."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None

    result = []
    i, n = 0, len(s)
    in_string = False
    delim = None

    while i < n:
        c = s[i]
        if not in_string:
            if c == "'":
                result.append('"')
                in_string = True
                delim = "'"
                i += 1
            elif c == '"':
                result.append('"')
                in_string = True
                delim = '"'
                i += 1
            else:
                result.append(c)
                i += 1
        else:
            if c == '\\' and i + 1 < n:
                result.append(c)
                result.append(s[i + 1])
                i += 2
            elif c == '"' and delim == '"':
                result.append('"')
                in_string = False
                delim = None
                i += 1
            elif c == "'" and delim == "'":
                # Look ahead: is this the closing delimiter or an apostrophe in content?
                j = i + 1
                while j < n and s[j] in ' \t\n\r':
                    j += 1
                if j >= n or s[j] in ':,}]':
                    result.append('"')
                    in_string = False
                    delim = None
                    i += 1
                else:
                    result.append(c)
                    i += 1
            else:
                result.append(c)
                i += 1

    out = ''.join(result)
    # Fix Python literals (word-bounded)
    out = re.sub(r'\bNone\b', 'null', out)
    out = re.sub(r'\bTrue\b', 'true', out)
    out = re.sub(r'\bFalse\b', 'false', out)
    # Fix trailing commas before closing brackets
    out = re.sub(r',(\s*[}\]])', r'\1', out)
    return out


repair_json_udf = udf(StringType())(_targeted_repair_impl)


_TEMP_TABLES = []
_pipeline_counter = 0
_lineage_tree = []  # List of (number, depth, table_name)


def parse_bronze_table(bronze_table):
    """Read Bronze, detect wrapper, repair via UDF (runs ONCE), materialize to temp table, parse."""
    df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{bronze_table}")

    # Quick wrapper detection from one sample row (no full table scan)
    sample_row = df.select("raw_data").filter(col("raw_data").isNotNull()).first()
    if not sample_row:
        print(f"  ⏭️ SKIP: No data")
        return None

    raw_sample = sample_row["raw_data"]
    has_wrapper = '"JSON_OBJECT"' in raw_sample or "'JSON_OBJECT'" in raw_sample

    if has_wrapper:
        print(f"  🔍 JSON_OBJECT wrapper detected")
        json_source = get_json_object(col("raw_data"), "$.JSON_OBJECT")
    else:
        print(f"  🔍 No wrapper — using raw_data directly")
        json_source = col("raw_data")

    # Repair JSON using targeted state-machine UDF
    df = df.withColumn("raw_data_repaired", repair_json_udf(json_source))

    # NOTE: custom. keys are intentionally kept as nested struct fields so the
    # custom object can be extracted into its own child table (extract_custom)

    # Write repaired data to temp table — UDF runs ONCE during write, then all reads are UDF-free
    full_temp = f"{CATALOG}.{SILVER_SCHEMA}._tmp_{bronze_table}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    spark.sql(f"DROP TABLE IF EXISTS {full_temp}")
    df.select("insert_date", "raw_data_repaired") \
        .write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(full_temp)
    _TEMP_TABLES.append(full_temp)

    # Read from temp table (fast — no UDF)
    df = spark.table(full_temp)
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

    # Merge schemas: combine multiple JSON objects into an array for a single schema_of_json call
    # Random sampling discovers ALL keys across different rows (e.g., every custom field variant)
    json_samples = [r["raw_data_repaired"] for r in sample_rows if r["raw_data_repaired"]]
    json_array_str = "[" + ",".join(json_samples) + "]"
    try:
        raw_schema_str = df.select(schema_of_json(lit(json_array_str)).alias("s")).collect()[0]["s"]
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
        _dup_count = len(_test_st.fields) - len(_unique_fields)
        print(f"  🔍 Discovered schema (random {len(json_samples)} samples, {len(_unique_fields)} top-level fields"
              + (f", {_dup_count} duplicates removed" if _dup_count else "") + ")")
    except Exception as e:
        # Fall back to single sample if merged schema has issues
        print(f"  ⚠️ Multi-sample schema failed ({str(e)[:80]}), falling back to single sample")
        sample = df.filter(col("raw_data_repaired").isNotNull()).select("raw_data_repaired").first()
        _fb_ddl = df.select(schema_of_json(lit(sample["raw_data_repaired"])).alias("s")).collect()[0]["s"]
        deduped_st = StructType.fromDDL(_fb_ddl)
        print(f"  🔍 Discovered schema (single sample, fallback): {_fb_ddl[:120]}...")

    # Parse (fast — reads from temp table, only from_json is computed)
    df_parsed = df.withColumn("parsed", from_json(col("raw_data_repaired"), deduped_st))
    valid = df_parsed.filter(col("parsed").isNotNull()).count()
    print(f"  Parsed: {valid:,} ({valid/total*100:.1f}%)")

    if valid == 0:
        return None

    # Handle top-level type: struct → star-expand, array → explode first
    parsed_type = df_parsed.schema["parsed"].dataType
    if isinstance(parsed_type, StructType):
        field_names = _dedup_names([_sanitize_col_name(f.name) for f in parsed_type.fields])
        select_exprs = [col("insert_date").alias("bronze_insert_date")]
        for f, safe_name in zip(parsed_type.fields, field_names):
            select_exprs.append(col(f"parsed.`{f.name}`").alias(safe_name))
        df_flat = df_parsed.select(*select_exprs)
    elif isinstance(parsed_type, ArrayType):
        print(f"  🔍 Top-level JSON is an array — exploding elements")
        df_exploded = df_parsed.withColumn("_element", explode_outer(col("parsed")))
        element_type = df_exploded.schema["_element"].dataType
        if isinstance(element_type, StructType):
            elem_names = _dedup_names([_sanitize_col_name(f.name) for f in element_type.fields])
            select_exprs = [col("insert_date").alias("bronze_insert_date")]
            for f, safe_name in zip(element_type.fields, elem_names):
                select_exprs.append(col(f"_element.`{f.name}`").alias(safe_name))
            df_flat = df_exploded.select(*select_exprs)
        else:
            df_flat = df_exploded.select(
                col("insert_date").alias("bronze_insert_date"),
                col("_element").alias("value")
            )
    else:
        df_flat = df_parsed.select(
            col("insert_date").alias("bronze_insert_date"),
            col("parsed").alias("value")
        )
    return df_flat


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


def print_schema_summary(df, label):
    """Print schema with data types, highlighting arrays and structs."""
    print(f"\n  📘 SCHEMA — {label}")
    for f in df.schema.fields:
        type_name = str(f.dataType)
        if isinstance(f.dataType, ArrayType):
            print(f"    🟠 ARRAY  {f.name}: {type_name}")
        elif isinstance(f.dataType, StructType):
            print(f"    🔵 STRUCT {f.name}: {type_name}")
        else:
            print(f"    🟢 {f.name}: {type_name}")


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


def print_physical_schema(df, label):
    """Print full physical schema tree."""
    print(f"\n  🌳 PHYSICAL SCHEMA TREE — {label}")
    df.printSchema()


def find_pk(df):
    """Find the best candidate for a primary key column."""
    for f in df.schema.fields:
        if f.name.lower() == "id" and isinstance(f.dataType, (StringType, IntegerType, LongType)):
            return f.name
    for f in df.schema.fields:
        if f.name.lower().endswith("_id") and isinstance(f.dataType, (StringType, IntegerType, LongType)):
            return f.name
    return None


def write_silver(df, table_name):
    """Write DataFrame to silver schema and return row count."""
    full = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    df.write.format("delta").mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .saveAsTable(full)
    cnt = spark.table(full).count()
    print(f"  ✅ {table_name}: {cnt:,} rows")
    return cnt


def explode_to_child(df, parent_name, array_col, carry_cols, depth=0, num_path=""):
    """Explode an array column into a child table. Recurse on nested arrays until scalar."""
    child_name = f"{parent_name}_{array_col}"

    df_child = df.withColumn("_element", explode_outer(col(array_col)))
    element_type = df_child.schema["_element"].dataType

    if isinstance(element_type, StructType):
        element_field_names = [f.name for f in element_type.fields]
        carry = []
        for c in dict.fromkeys(carry_cols):
            if c in df.columns and c != array_col:
                if c in element_field_names:
                    carry.append(col(c).alias(f"{parent_name}_{c}"))
                else:
                    carry.append(col(c))
        elem_names = _dedup_names([_sanitize_col_name(f.name) for f in element_type.fields])
        element_fields = [col(f"_element.`{f.name}`").alias(safe) for f, safe in zip(element_type.fields, elem_names)]
        df_child = df_child.select(*carry, *element_fields)
        print(f"  {'  '*depth}📦 {array_col}: array of objects — extracted {len(element_type.fields)} fields")
    else:
        carry = [col(c) for c in dict.fromkeys(carry_cols) if c in df.columns and c != array_col]
        df_child = df_child.select(*carry, col("_element").alias(array_col))
        print(f"  {'  '*depth}📦 {array_col}: array of scalars")

    # Flatten any structs from the explosion (keep custom for separate extraction)
    df_child = flatten_structs(df_child, skip_custom=True)

    print_schema_summary(df_child, f"{child_name} (depth {depth})")
    print_physical_schema(df_child, child_name)

    write_silver(df_child, child_name)
    _lineage_tree.append((num_path, depth + 1, child_name))

    # Extract custom struct from child if present
    child_pk = find_pk(df_child)
    child_carry = list(dict.fromkeys(
        [c for c in carry_cols if c in df_child.columns] + ([child_pk] if child_pk else [])
    ))
    has_child_custom = "custom" in df_child.columns and isinstance(df_child.schema["custom"].dataType, StructType)
    has_child_cf = any(c.startswith("custom_cf_") for c in df_child.columns)
    if has_child_custom or has_child_cf:
        custom_num = f"{num_path}_0"
        df_child = extract_custom(df_child, child_name, child_carry, custom_num)

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
        explode_to_child(df_child, child_name, ca, new_carry, depth + 1, grandchild_num)


def extract_custom(df, parent_name, carry_cols, num_path):
    """Extract custom struct AND custom_cf_ columns into separate child tables.

    THREE separate outputs (custom and custom_cf_ are TOTALLY DIFFERENT):
    1. _custom table: regular custom struct scalars → individual columns
    2. _custom_arrays table: ALL custom struct arrays → ONE key-value table (custom_key + value)
    3. _custom_cf table: ALL custom_cf_ fields → ONE EAV table (custom_field_name + custom_field_value)

    custom_cf_ fields are ALL scalars. None are arrays.
    The custom_field_name stores the full name including the custom.cf_ prefix.
    """
    # Detect cf_ from TWO sources:
    # Source 1: Top-level columns starting with "custom_cf_" (from JSON keys like "custom.cf_xxx")
    top_cf_cols = [c for c in df.columns if c.startswith("custom_cf_")]

    # Source 2: cf_ fields inside the custom struct
    has_custom_struct = "custom" in df.columns and isinstance(df.schema["custom"].dataType, StructType)

    if not has_custom_struct and not top_cf_cols:
        return df

    carry = [col(c) for c in dict.fromkeys(carry_cols) if c in df.columns]

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
    cf_name = f"{parent_name}_custom_cf"
    arrays_name = f"{parent_name}_custom_arrays"
    grandchild_idx = 0

    # -- 1. Build _custom table (regular scalar fields from custom struct only) --
    if has_custom_struct and struct_scalar_fields:
        select_exprs = list(carry) + [
            col(f"custom.`{orig}`").alias(safe) for orig, safe in struct_scalar_fields
        ]
        df_custom = df.select(*select_exprs)
        df_custom = flatten_structs(df_custom, skip_custom=False)

        print(f"\n  📦 CUSTOM TABLE — {custom_name}")
        print(f"     Regular custom scalar fields: {len(struct_scalar_fields)}")
        print_schema_summary(df_custom, custom_name)
        print_physical_schema(df_custom, custom_name)
        write_silver(df_custom, custom_name)
        _lineage_tree.append((num_path, 1, custom_name))

        # Explode arrays that survived flatten in the custom table
        custom_pk = find_pk(df_custom)
        custom_carry = list(dict.fromkeys(
            [c for c in carry_cols if c in df_custom.columns] + ([custom_pk] if custom_pk else [])
        ))
        _, custom_arrays_in_table, _ = categorize_fields(df_custom)
        for ca in custom_arrays_in_table:
            grandchild_idx += 1
            grandchild_num = f"{num_path}_{grandchild_idx}"
            explode_to_child(df_custom, custom_name, ca, custom_carry, depth=1, num_path=grandchild_num)

    # -- 2. Build _custom_arrays table (ALL custom struct arrays in ONE key-value table) --
    if has_custom_struct and struct_array_fields:
        array_rows = []
        for orig_name, safe_name in struct_array_fields:
            row = df.select(
                *carry,
                lit(orig_name).alias("custom_key"),
                explode_outer(col(f"custom.`{orig_name}`")).cast("string").alias("value")
            )
            array_rows.append(row)
        df_arrays = array_rows[0]
        for r in array_rows[1:]:
            df_arrays = df_arrays.unionByName(r, allowMissingColumns=True)

        print(f"\n  📦 CUSTOM ARRAYS TABLE — {arrays_name}")
        print(f"     Custom array fields: {len(struct_array_fields)}")
        for orig, safe in struct_array_fields:
            print(f"       {orig}")
        print_schema_summary(df_arrays, arrays_name)
        print_physical_schema(df_arrays, arrays_name)
        write_silver(df_arrays, arrays_name)
        _lineage_tree.append((num_path, 2, arrays_name))

    # -- 3. Build _custom_cf table (ALL cf_ fields as EAV rows) --
    # Collect from top-level custom_cf_ columns AND cf_ fields inside custom struct
    all_cf = []  # (column_ref, field_name_for_eav)

    # From top-level custom_cf_ columns
    for c in top_cf_cols:
        orig_name = c.replace("custom_cf_", "custom.cf_", 1)
        all_cf.append((col(c), orig_name))

    # From custom struct cf_ fields
    if has_custom_struct:
        for f_name in struct_cf_fields:
            all_cf.append((col(f"custom.`{f_name}`"), f"custom.{f_name}"))

    if all_cf:
        cf_rows = []
        for col_ref, field_name in all_cf:
            row = df.select(
                *carry,
                lit(field_name).alias("custom_field_name"),
                col_ref.cast("string").alias("custom_field_value")
            )
            cf_rows.append(row)
        df_cf = cf_rows[0]
        for r in cf_rows[1:]:
            df_cf = df_cf.unionByName(r, allowMissingColumns=True)
        df_cf = df_cf.filter(col("custom_field_value").isNotNull())

        print(f"\n  🔑 CUSTOM_CF TABLE — {cf_name} (EAV)")
        print(f"     cf_ fields: {len(all_cf)}")
        for col_ref, fname in all_cf[:10]:
            print(f"       {fname}")
        if len(all_cf) > 10:
            print(f"       ... and {len(all_cf) - 10} more")
        print_schema_summary(df_cf, cf_name)
        print_physical_schema(df_cf, cf_name)
        write_silver(df_cf, cf_name)
        _lineage_tree.append((num_path, 3, cf_name))

    # -- Drop everything custom-related from parent --
    cols_to_drop = list(top_cf_cols)
    if has_custom_struct:
        cols_to_drop.append("custom")
    for c in cols_to_drop:
        if c in df.columns:
            df = df.drop(c)

    return df


def print_lineage_tree():
    """Print the lineage tree with hierarchical numbering."""
    print(f"\n  📋 LINEAGE TREE:")
    for num, depth, name in _lineage_tree:
        indent = "---" * depth
        print(f"  # * {indent}{num}_{name}")


def process_table(bronze_name, silver_name=None):
    """Full Bronze → Silver pipeline: parse, flatten structs, extract custom, explode arrays."""
    global _pipeline_counter, _lineage_tree
    _pipeline_counter += 1
    pipeline = _pipeline_counter

    if silver_name is None:
        silver_name = bronze_name

    _lineage_tree = [(f"{pipeline}", 0, silver_name)]

    start = time.time()
    print(f"\n{'='*80}")
    print(f"📥 {bronze_name} → {silver_name}")
    print(f"{'='*80}")

    # Print root structure
    print(f"  # * raw_{bronze_name}-> bronze_{bronze_name}-> {silver_name}")

    df_flat = parse_bronze_table(bronze_name)
    if df_flat is None:
        return

    # Flatten structs (keep custom for separate extraction)
    df_flat = flatten_structs(df_flat, skip_custom=True)

    # Find PK for carry
    pk = find_pk(df_flat)
    carry = list(dict.fromkeys(["bronze_insert_date"] + ([pk] if pk else [])))

    # Extract custom as child table if present (struct or flat custom.cf_ columns)
    child_idx = 0
    has_custom = "custom" in df_flat.columns and isinstance(df_flat.schema["custom"].dataType, StructType)
    has_custom_cf = any(c.startswith("custom_cf_") for c in df_flat.columns)
    if has_custom or has_custom_cf:
        child_idx += 1
        custom_num = f"{pipeline}_{child_idx}"
        df_flat = extract_custom(df_flat, silver_name, carry, custom_num)

    # Print physical schema tree and summary for parent (after custom removal)
    print_physical_schema(df_flat, silver_name)
    print_schema_summary(df_flat, f"{silver_name} (parent)")
    write_silver(df_flat, silver_name)

    # Explode arrays
    scalars, arrays, structs = categorize_fields(df_flat)
    if arrays:
        print(f"  Arrays to explode: {arrays}")
        for arr in arrays:
            child_idx += 1
            child_num = f"{pipeline}_{child_idx}"
            explode_to_child(df_flat, silver_name, arr, carry, depth=0, num_path=child_num)

    # Print lineage tree
    print_lineage_tree()

    # Clean up temp tables
    for temp in _TEMP_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {temp}")
    _TEMP_TABLES.clear()

    elapsed = time.time() - start
    print(f"  ⏱️ Completed in {elapsed:.1f}s")
    print(f"{'='*80}")


print("✅ Silver engine loaded")
print("   Usage: process_table('bronze_table_name', 'silver_table_name')")

# COMMAND ----------

# DBTITLE 1,Drop Silver Tables
# # ============================================================================
# # DROP EXISTING SILVER TABLES — Start Fresh
# # ============================================================================
# # Run this cell ONCE before processing to clear old silver tables.
# # ============================================================================

# try:
#     tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").collect()
#     for row in tables:
#         tbl = row["tableName"]
#         fqn = f"{CATALOG}.{SILVER_SCHEMA}.{tbl}"
#         spark.sql(f"DROP TABLE IF EXISTS {fqn}")
#         print(f"  DROPPED {fqn}")
#     if not tables:
#         print("  No silver tables to drop")
# except Exception as e:
#     print(f"  Schema does not exist yet: {str(e)[:80]}")

# print("\n✅ Silver schema cleared. Ready for processing.")

# COMMAND ----------

# DBTITLE 1,1. all_payments
process_table("all_payments")

# COMMAND ----------

# DBTITLE 1,2. calendly_scheduled_events
process_table("calendly_scheduled_events")

# COMMAND ----------

# DBTITLE 1,3. close_crm_users_raw
process_table("close_crm_users_raw")

# COMMAND ----------

# DBTITLE 1,4. custom_activites_raw
process_table("custom_activites_raw")

# COMMAND ----------

# DBTITLE 1,5. lead_activites_raw
process_table("lead_activites_raw")

# COMMAND ----------

# DBTITLE 1,6. lead_merges
process_table("lead_merges")

# COMMAND ----------

# DBTITLE 1,7. leads_raw
process_table("leads_raw")

# COMMAND ----------

# DBTITLE 1,8. mdl_users_raw
process_table("mdl_users_raw")

# COMMAND ----------

# DBTITLE 1,9. student_sentiment
process_table("student_sentiment")

# COMMAND ----------

# DBTITLE 1,Validation: Bronze Counts
# MAGIC %sql --name bronze_counts
# MAGIC -- Bronze table counts
# MAGIC SELECT 'all_payments' AS table_name, COUNT(*) AS bronze_count FROM crm_ingestion.bronze.all_payments
# MAGIC UNION ALL SELECT 'calendly_scheduled_events', COUNT(*) FROM crm_ingestion.bronze.calendly_scheduled_events
# MAGIC UNION ALL SELECT 'close_crm_users_raw', COUNT(*) FROM crm_ingestion.bronze.close_crm_users_raw
# MAGIC UNION ALL SELECT 'custom_activites_raw', COUNT(*) FROM crm_ingestion.bronze.custom_activites_raw
# MAGIC UNION ALL SELECT 'lead_activites_raw', COUNT(*) FROM crm_ingestion.bronze.lead_activites_raw
# MAGIC UNION ALL SELECT 'lead_merges', COUNT(*) FROM crm_ingestion.bronze.lead_merges
# MAGIC UNION ALL SELECT 'leads_raw', COUNT(*) FROM crm_ingestion.bronze.leads_raw
# MAGIC UNION ALL SELECT 'mdl_users_raw', COUNT(*) FROM crm_ingestion.bronze.mdl_users_raw
# MAGIC UNION ALL SELECT 'student_sentiment', COUNT(*) FROM crm_ingestion.bronze.student_sentiment
# MAGIC ORDER BY bronze_count DESC

# COMMAND ----------

# DBTITLE 1,Validation: Silver Table Catalog
# MAGIC %sql
# MAGIC -- All Silver tables (parents + children)
# MAGIC SELECT table_name,
# MAGIC        CASE WHEN table_name LIKE '%\_%\_%' THEN 'child (depth 2+)'
# MAGIC             WHEN table_name LIKE '%\_%' THEN 'child (depth 1)'
# MAGIC             ELSE 'parent'
# MAGIC        END AS type
# MAGIC FROM crm_ingestion.information_schema.tables
# MAGIC WHERE table_schema = 'silver'
# MAGIC ORDER BY type, table_name

# COMMAND ----------

# DBTITLE 1,Validation: Sample Check
# Sample check: verify a few silver tables
tables_to_check = ["leads_raw", "all_payments", "lead_activites_raw"]

for tbl in tables_to_check:
    try:
        df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{tbl}")
        print(f"\n{tbl}: {df.count():,} rows, {len(df.columns)} columns")
        print(f"  Columns: {', '.join(df.columns[:15])}" + ("..." if len(df.columns) > 15 else ""))
    except Exception as e:
        print(f"\n{tbl}: not found or error - {str(e)[:60]}")