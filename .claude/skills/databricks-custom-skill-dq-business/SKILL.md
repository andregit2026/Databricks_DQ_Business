---
name: databricks-dq-professionally
description: Professional data quality framework on Databricks. Implements a generic rule registry, rule-to-field mappings, dynamic rule application, and enriched DQ output tables with a single DQ_RESULT column (e.g. "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"). Automatically sets up the DQ schema, copies source tables, and applies mapped DQ rules. Use when building rule-based DQ pipelines.
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Grep
  - Glob
model: claude-sonnet-4-6
user-invocable: true
---

# Databricks Data Quality

Professional, regulator-grade data quality framework on Databricks. Uses a **generic rule registry**
mapped to specific table fields, producing enriched DQ output tables with standardised per-rule
columns.

---

## When to Use This Skill

- Setting up a DQ framework for a new source table (e.g. `bikes`, `counterparty_exposures`)
- Registering and applying generic DQ rules (NOT_NULL, DATE_FORMAT, RANGE, REFERENTIAL, etc.)
- Producing enriched DQ output tables with a single `DQ_RESULT` string column for downstream reporting
- Establishing audit trails, breach registers, and remediation tracking

---

## DQ Categories

Six standard DQ categories are used throughout this framework.

| `dq_category` | Meaning |
|---------------|---------|
| `ACCURACY` | Values are correct and within valid ranges |
| `COMPLETENESS` | All required fields are populated |
| `TIMELINESS` | Data is available within the required timeframe |
| `CONSISTENCY` | Values are consistent across systems |
| `VALIDITY` | Values conform to domain rules and formats |
| `UNIQUENESS` | No duplicate values in uniqueness-constrained fields |

### Category Inheritance Rule

- `dq_rules_generic.dq_category` — the **default** category for the generic rule type
- `dq_rule_mappings.dq_category` — **optional override** per mapping (NULL = inherit from generic rule)
- Effective category = `COALESCE(mapping.dq_category, generic.dq_category)`

**Example override:** `RULE_3_NUMERIC_NOT_NULL` has default category `COMPLETENESS`.
When applied to `total_revenue` in a financial context, the mapping may override it to `ACCURACY`
because a zero revenue value may be a valid business fact, not a completeness issue.

---

## Catalog & Schema Setup

When this skill is invoked, always set up the workspace schema first:

```python
# Use databricks-unity-catalog skill
create_schema(
    catalog_name="dbdemos",
    schema_name="dbdemos_dq_professional",
    comment="Professional DQ framework. Generic rule registry, rule mappings, and enriched DQ output tables."
)
```

**All tables in this skill live under:** `dbdemos.dbdemos_dq_professional`

---

## Core Table Architecture

```
dbdemos.dbdemos_dq_professional
│
├── dq_rules_generic          ← Catalogue of all available generic DQ rules
│                                (e.g. RULE_1_DATE_NOT_NULL)
│
├── dq_validation_request     ← Groups DQ checks by business entity
│                                dq_val_request_id (IDENTITY) | owner_dept | owner_user
│
├── dq_rule_mappings          ← Maps generic rules to specific table.field combinations
│                                FK: dq_val_request_id → dq_validation_request
│                                FK: generic_rule_desc_short → dq_rules_generic
│                                owner_dept | owner_user | threshold_pct
│
├── bikes                     ← Copy of source table (loaded once at setup)
│
└── bikes_dq                  ← Enriched output: bikes + single DQ_RESULT column
                                 DQ_RESULT = "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"
```

---

## Table 1 — Generic Rules Registry (`dq_rules_generic`)

Catalogue of all reusable DQ rule types. Rules are generic — they are not bound to a specific
table or field until mapped in `dq_rule_mappings`.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `generic_rule_id` | BIGINT IDENTITY | Auto-increment primary key (assigned by Delta, never set manually) |
| `generic_rule_desc_short` | STRING | Short rule identifier, e.g. `RULE_1_DATE_NOT_NULL` |
| `rule_description` | STRING | Human-readable name, e.g. `Date Not Null` |
| `dq_category` | STRING | Regulatory DQ category — one of: `ACCURACY`, `COMPLETENESS`, `TIMELINESS`, `CONSISTENCY`, `VALIDITY`, `UNIQUENESS` (see category table above) |
| `rule_detail` | STRING | Long text explaining what the rule checks |
| `rule_logic_template` | STRING | SQL/Spark template, e.g. `CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END` |
| `applicable_types` | STRING | Field types this rule applies to, e.g. `DATE,TIMESTAMP` |
| `bcbs239_principle` | STRING | e.g. `P3,P4` |
| `solvencyii_pillar` | STRING | e.g. `Pillar1,QRT` |
| `active` | INTEGER | 1 = active, 0 = deprecated |
| `created_date` | DATE | |
| `created_by` | STRING | |

### Seed Data — Standard Generic Rules

```python
from datetime import date

generic_rules = [
    # ── COMPLETENESS ────────────────────────────────────────────────
    ("RULE_1_DATE_NOT_NULL",
     "Date Not Null",
     "COMPLETENESS",
     "Checks that a DATE or TIMESTAMP field is not NULL. "
     "Returns 1 if the field contains a value, 0 if NULL.",
     "CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP",
     "P3,P4", "Pillar1,QRT"),

    ("RULE_2_STRING_NOT_NULL",
     "String Not Null",
     "COMPLETENESS",
     "Checks that a STRING field is not NULL or empty.",
     "CASE WHEN {field} IS NOT NULL AND trim({field}) != '' THEN 1 ELSE 0 END",
     "STRING",
     "P3,P4", "Pillar1,QRT"),

    ("RULE_3_NUMERIC_NOT_NULL",
     "Numeric Not Null",
     "COMPLETENESS",
     "Checks that a numeric field is not NULL.",
     "CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT",
     "P3,P4", "Pillar1"),

    # ── ACCURACY ────────────────────────────────────────────────────
    ("RULE_10_NUMERIC_NON_NEGATIVE",
     "Numeric Non-Negative",
     "ACCURACY",
     "Checks that a numeric field is >= 0. Amounts and counts must not be negative.",
     "CASE WHEN {field} >= 0 THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT",
     "P3", "Pillar1"),

    ("RULE_11_NUMERIC_POSITIVE",
     "Numeric Positive",
     "ACCURACY",
     "Checks that a numeric field is > 0.",
     "CASE WHEN {field} > 0 THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT",
     "P3", "Pillar1"),

    ("RULE_12_DATE_NOT_FUTURE",
     "Date Not in Future",
     "ACCURACY",
     "Checks that a DATE field is not in the future relative to current date.",
     "CASE WHEN {field} <= current_date() THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP",
     "P3", "QRT"),

    ("RULE_13_DATE_NOT_PAST_LIMIT",
     "Date Within Reasonable Past",
     "ACCURACY",
     "Checks that a DATE field is not older than 10 years (data staleness check).",
     "CASE WHEN {field} >= date_sub(current_date(), 3650) THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP",
     "P3,P5", "Pillar1"),

    # ── VALIDITY ────────────────────────────────────────────────────
    ("RULE_20_ISO_COUNTRY_CODE",
     "ISO 3166-1 Alpha-2 Country Code",
     "VALIDITY",
     "Checks that a STRING field contains a valid ISO 3166-1 alpha-2 country code (2 uppercase letters).",
     "CASE WHEN {field} RLIKE '^[A-Z]{2}$' THEN 1 ELSE 0 END",
     "STRING",
     "P3", "QRT"),

    ("RULE_21_BOOLEAN_FLAG",
     "Boolean Flag (0 or 1)",
     "VALIDITY",
     "Checks that an integer field contains only 0 or 1.",
     "CASE WHEN {field} IN (0, 1) THEN 1 ELSE 0 END",
     "INTEGER",
     "P3", "Pillar2"),

    # ── UNIQUENESS ──────────────────────────────────────────────────
    ("RULE_30_UNIQUE_IDENTIFIER",
     "Unique Identifier",
     "UNIQUENESS",
     "Checks that a field has no duplicate values across the dataset. "
     "Applied at dataset level — marks duplicates as 0.",
     "ROW_NUMBER() OVER (PARTITION BY {field} ORDER BY {field}) = 1",
     "STRING,INTEGER,LONG",
     "P3", "Pillar1"),
]

# ── Create table with IDENTITY column (run once) ─────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS dbdemos.dbdemos_dq_professional.dq_rules_generic (
        generic_rule_id         BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        generic_rule_desc_short STRING NOT NULL,
        rule_description        STRING,
        dq_category             STRING,
        rule_detail             STRING,
        rule_logic_template     STRING,
        applicable_types        STRING,
        bcbs239_principle       STRING,
        solvencyii_pillar       STRING,
        active                  INTEGER,
        created_date            DATE,
        created_by              STRING
    )
    USING DELTA
    COMMENT 'Generic DQ rule catalogue. generic_rule_id auto-increments — never set manually.'
""")

# ── Seed rows (INSERT — Delta assigns generic_rule_id automatically) ──
from pyspark.sql import Row
from datetime import date

rows = [Row(
    generic_rule_desc_short=r[0], rule_description=r[1], dq_category=r[2],
    rule_detail=r[3], rule_logic_template=r[4], applicable_types=r[5],
    bcbs239_principle=r[6], solvencyii_pillar=r[7],
    active=1, created_date=date.today(), created_by="dq-framework"
) for r in generic_rules]

insert_df = spark.createDataFrame(rows)
insert_df.createOrReplaceTempView("_dq_rules_generic_staging")

spark.sql("""
    INSERT INTO dbdemos.dbdemos_dq_professional.dq_rules_generic
        (generic_rule_desc_short, rule_description, dq_category, rule_detail,
         rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
         active, created_date, created_by)
    SELECT
        generic_rule_desc_short, rule_description, dq_category, rule_detail,
        rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
        active, created_date, created_by
    FROM _dq_rules_generic_staging
""")

print(f"Seeded {len(rows)} generic rules. generic_rule_id values assigned by Delta.")
```

---

## Table 2 — Validation Request Registry (`dq_validation_request`)

A **validation request** groups all DQ rule mappings that belong to a single business entity
(e.g. the `bikes` table, a `counterparty` entity). Each request has a unique auto-increment
`dq_val_request_id` which is referenced as an FK in `dq_rule_mappings`.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `dq_val_request_id` | BIGINT IDENTITY | Auto-increment PK (assigned by Delta, never set manually) |
| `dq_validation_request_short` | STRING | Short identifier, e.g. `DQ_VAL_REQUEST_1_Customer_entity` |
| `owner_dept` | STRING | Business department responsible for this entity |
| `owner_user` | STRING | Data steward responsible for this entity |
| `active` | INTEGER | 1 = active, 0 = disabled |
| `created_date` | DATE | |
| `created_by` | STRING | |

### Seed Data

```python
# ── Create table with IDENTITY column (run once) ─────────────────────
spark.sql("DROP TABLE IF EXISTS dbdemos.dbdemos_dq_professional.dq_validation_request")
spark.sql("""
    CREATE TABLE dbdemos.dbdemos_dq_professional.dq_validation_request (
        dq_val_request_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        dq_validation_request_short   STRING NOT NULL,
        owner_dept                    STRING,
        owner_user                    STRING,
        active                        INTEGER,
        created_date                  DATE,
        created_by                    STRING
    )
    USING DELTA
    COMMENT 'DQ validation request registry. dq_val_request_id auto-increments — never set manually.'
""")

from pyspark.sql import Row
from datetime import date

validation_requests = [
    # dq_validation_request_short,             owner_dept,            owner_user
    ("DQ_VAL_REQUEST_1_Customer_entity", "Dept. Master Data", "Mr. Alex Smith"),
]

vr_rows = [Row(
    dq_validation_request_short=r[0],
    owner_dept=r[1], owner_user=r[2],
    active=1, created_date=date.today(), created_by="dq-framework"
) for r in validation_requests]

vr_df = spark.createDataFrame(vr_rows)
vr_df.createOrReplaceTempView("_dq_validation_request_staging")

spark.sql("""
    INSERT INTO dbdemos.dbdemos_dq_professional.dq_validation_request
        (dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by)
    SELECT
        dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by
    FROM _dq_validation_request_staging
""")

print(f"Seeded {len(vr_rows)} validation requests. dq_val_request_id values assigned by Delta.")
```

---

## Table 3 — Rule Mappings (`dq_rule_mappings`)

Maps each generic rule to a specific `catalog.schema.table.field` combination.
Each mapping gets a unique `rule_mapping_id` (the **XYZ** used in output column names).

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `rule_mapping_id` | BIGINT | Unique mapping ID — becomes XYZ in `RULE_XYZ_*` columns |
| `dq_rule_no` | STRING | Computed DQ identifier: `DQ_{rule_mapping_id}_{rule_suffix}_{source_field}` e.g. `DQ_101_DATE_NOT_NULL_ride_date` |
| `dq_val_request_id` | INTEGER | FK → `dq_validation_request.dq_val_request_id` — groups rules by entity |
| `generic_rule_desc_short` | STRING | FK → `dq_rules_generic.generic_rule_desc_short` |
| `source_catalog` | STRING | e.g. `dbdemos` |
| `source_schema` | STRING | e.g. `dbdemos_dq_professional` |
| `source_table` | STRING | e.g. `bikes` |
| `source_field` | STRING | e.g. `ride_date` |
| `dq_category` | STRING | **Optional override** of `dq_rules_generic.dq_category`. NULL = inherit from generic rule. Set when this specific mapping belongs to a different category than the rule default. |
| `rule_description` | STRING | Specific description, e.g. "ride_date must not be NULL" |
| `threshold_pct` | DOUBLE | Minimum pass rate required, e.g. 100.0 |
| `owner_dept` | STRING | Business department responsible for this rule mapping |
| `owner_user` | STRING | Data steward responsible for this rule mapping |
| `active` | INTEGER | 1 = active, 0 = disabled |
| `created_date` | DATE | |
| `created_by` | STRING | |

### Example — Mapping Rules to the `bikes` Table

```python
from pyspark.sql import Row
from datetime import date

mappings = [
    # rule_mapping_id, val_req_id, generic_rule_desc_short,           catalog,    schema,                    table,  field,                   description,                                     threshold_pct, dq_category,   owner_dept,            owner_user,   owner_dept,            owner_user
    (101, 1, "RULE_1_DATE_NOT_NULL",          "dbdemos", "dbdemos_dq_professional", "bikes", "ride_date",            1, "ride_date must not be NULL",        100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),
    (102, 1, "RULE_3_NUMERIC_NOT_NULL",       "dbdemos", "dbdemos_dq_professional", "bikes", "total_rides",          1, "total_rides must not be NULL",      100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),
    (103, 1, "RULE_3_NUMERIC_NOT_NULL",       "dbdemos", "dbdemos_dq_professional", "bikes", "total_revenue",        1, "total_revenue must not be NULL",    100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),
    (104, 1, "RULE_10_NUMERIC_NON_NEGATIVE",  "dbdemos", "dbdemos_dq_professional", "bikes", "total_rides",          1, "total_rides must be >= 0",          100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),
    # dq_category override: total_revenue >= 0 is an ACCURACY check in financial context
    (105, 1, "RULE_10_NUMERIC_NON_NEGATIVE",  "dbdemos", "dbdemos_dq_professional", "bikes", "total_revenue",        1, "total_revenue must be >= 0",        100.0, "ACCURACY",  "Dept. Master Data", "Mr. Alex Smith"),
    (106, 1, "RULE_21_BOOLEAN_FLAG",          "dbdemos", "dbdemos_dq_professional", "bikes", "requires_maintenance", 1, "requires_maintenance must be boolean (0/1)",     100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),
    (107, 1, "RULE_12_DATE_NOT_FUTURE",       "dbdemos", "dbdemos_dq_professional", "bikes", "ride_date",            1, "ride_date must not be in the future",            100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),
    (108, 1, "RULE_20_ISO_COUNTRY_CODE",      "dbdemos", "dbdemos_dq_professional", "bikes", "ride_date",            0, "ISO country check — not applicable to ride_date", 100.0, None,       "Dept. Master Data", "Mr. Alex Smith"),
]

rows = [Row(
    rule_mapping_id=m[0],
    dq_rule_no='DQ_' + str(m[0]) + '_' + '_'.join(m[2].split('_')[2:]) + '_' + m[6],
    dq_val_request_id=m[1], generic_rule_desc_short=m[2],
    source_catalog=m[3], source_schema=m[4], source_table=m[5], source_field=m[6],
    rule_description=m[7], threshold_pct=m[8],
    dq_category=m[9], owner_dept=m[10], owner_user=m[11],
    active=1, created_date=date.today(), created_by="dq-framework"
) for m in mappings]

spark.createDataFrame(rows).write \
    .format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("dbdemos.dbdemos_dq_professional.dq_rule_mappings")

print(f"Registered {len(rows)} rule mappings.")
```

---

## Table 4 — Source Table Copy

Copy the source table into `dbdemos_dq_professional` before enrichment:

```python
def copy_source_table(source_full_name: str, target_table: str):
    """
    Copy source table into dbdemos.dbdemos_dq_professional.
    source_full_name: e.g. 'dbdemos.dbdemos_uc_lineage.bikes'
    target_table:     e.g. 'bikes'
    """
    df = spark.table(source_full_name)
    target_full = f"dbdemos.dbdemos_dq_professional.{target_table}"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_full)

    count = spark.table(target_full).count()
    print(f"Copied {count} rows from {source_full_name} → {target_full}")

# Usage:
copy_source_table("dbdemos.dbdemos_uc_lineage.bikes", "bikes")
```

---

## Table 5 — DQ Enriched Output (`bikes_dq`)

The enriched table is the source table **plus one `DQ_RESULT` column** (STRING) that stores all rule
results concatenated. Format: `"RULE_{rule_mapping_id}: {result} | RULE_{rule_mapping_id}: {result} | ..."` where
`result` is `1` (pass), `0` (fail), or `NULL` (rule not applicable).

| Column | Type | Description |
|--------|------|-------------|
| `DQ_RESULT` | STRING | Concatenated rule results for all active mappings |

### Value encoding

| Value | Meaning |
|-------|---------|
| `1` | Rule passed |
| `0` | Rule failed |
| `NULL` (literal string) | Rule not applicable (row filtered by status) |

### Example output for `bikes_dq`

```
ride_date  | bike_id | total_rides | ... | DQ_RESULT
-----------|---------|-------------|-----|-------------------------------------------------------------
2026-02-19 | abc-123 | 10          | ... | RULE_101: 1 | RULE_102: 1 | RULE_103: 1 | RULE_104: 1 | RULE_108: NULL
NULL       | def-456 | 8           | ... | RULE_101: 0 | RULE_102: 1 | RULE_103: 1 | RULE_104: 1 | RULE_108: NULL
2026-02-20 | ghi-789 | NULL        | ... | RULE_101: 1 | RULE_102: 0 | RULE_103: 0 | RULE_104: 0 | RULE_108: NULL
```

> `RULE_108: NULL` — rule not applicable for this row (filtered by status).

---

## Table 6 — Aggregated DQ Results (`dq_results`)

One row per **rule per pipeline run**. `dq_run_id` auto-increments via Delta Lake identity column.
This is the primary table for dashboards, regulatory reporting, and SLA monitoring.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `dq_run_id` | BIGINT IDENTITY | Auto-increment PK — unique per row, assigned by Delta |
| `dq_execution_timestamp` | TIMESTAMP | When the pipeline executed (UTC) |
| `dq_rule_no` | INTEGER | FK → `dq_rule_mappings.rule_mapping_id` |
| `dq_number_relevant_records` | BIGINT | `SUM(RULE_{no}_DQ_RELEVANT_FLAG)` — records the rule was applied to |
| `dq_number_pos_results` | BIGINT | `SUM(RULE_{no}_DQ_RESULT)` — records that passed the rule |

### Create Table (run once)

```python
spark.sql("DROP TABLE IF EXISTS dbdemos.dbdemos_dq_professional.dq_results")
spark.sql("""
    CREATE TABLE dbdemos.dbdemos_dq_professional.dq_results (
        dq_run_id                   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        dq_execution_timestamp      TIMESTAMP     NOT NULL,
        dq_rule_no             STRING        NOT NULL,
        dq_number_relevant_records  BIGINT        NOT NULL,
        dq_number_pos_results       BIGINT        NOT NULL
    )
    USING DELTA
    COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'
""")
print("Table dbdemos.dbdemos_dq_professional.dq_results created.")
```

> **Note:** `GENERATED ALWAYS AS IDENTITY` is native to Databricks Delta Lake. Never include
> `dq_run_id` in INSERT statements — Delta assigns it automatically.

---

## Complete DQ Enrichment Notebook

**`src/dq_professional/notebooks/01_apply_dq_rules.py`**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Rule Application
# MAGIC
# MAGIC Loads all active rule mappings for the target table,
# MAGIC applies each generic rule dynamically, and writes the enriched DQ table.

# COMMAND ----------
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "dbdemos"

try:
    dq_schema = dbutils.widgets.get("dq_schema")
except:
    dq_schema = "dbdemos_dq_professional"

try:
    target_table = dbutils.widgets.get("target_table")  # e.g. "bikes"
except:
    target_table = "bikes"

try:
    report_date = dbutils.widgets.get("report_date")
except:
    from datetime import date
    report_date = str(date.today())

print(f"DQ Enrichment Parameters:")
print(f"  Catalog:       {catalog}")
print(f"  DQ Schema:     {dq_schema}")
print(f"  Target table:  {target_table}")
print(f"  Report date:   {report_date}")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Load source table ────────────────────────────────────────────────
full_source = f"{catalog}.{dq_schema}.{target_table}"
df = spark.table(full_source)
total_rows = df.count()
print(f"\nLoaded {total_rows} rows from {full_source}")
print(f"Columns: {df.columns}")

# COMMAND ----------
# ── Load active rule mappings for this table ─────────────────────────
mappings_df = (
    spark.table(f"{catalog}.{dq_schema}.dq_rule_mappings")
    .filter(
        (F.col("source_catalog") == catalog) &
        (F.col("source_schema")  == dq_schema) &
        (F.col("source_table")   == target_table) &
        (F.col("active")         == 1)
    )
    .orderBy("rule_mapping_id")
)

mappings = mappings_df.collect()
print(f"\nActive rule mappings for '{target_table}': {len(mappings)}")
for m in mappings:
    print(f"  RULE_{m.rule_mapping_id}: {m.generic_rule_desc_short} → {m.source_field} [{flag_label}]")

# COMMAND ----------
# ── Generic Rule Dispatcher ──────────────────────────────────────────

def compute_rule_result(df, rule_mapping_id: int, generic_rule_desc_short: str,
                        field_name: str):
    """
    Adds a temporary column _DQ_{rule_mapping_id} to df with the integer result:
      1    = rule passed
      0    = rule failed
      NULL = rule not applicable (row filtered by status)
    The temporary column is later assembled into the DQ_RESULT string.
    """
    tmp_col = f"_DQ_{rule_mapping_id}"

    if generic_rule_desc_short == "RULE_1_DATE_NOT_NULL":
        result_col = F.when(F.col(field_name).isNotNull(), F.lit(1)).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_2_STRING_NOT_NULL":
        result_col = F.when(
            F.col(field_name).isNotNull() & (F.trim(F.col(field_name)) != ""),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_3_NUMERIC_NOT_NULL":
        result_col = F.when(F.col(field_name).isNotNull(), F.lit(1)).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_10_NUMERIC_NON_NEGATIVE":
        result_col = F.when(
            F.col(field_name).isNotNull() & (F.col(field_name) >= 0),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_11_NUMERIC_POSITIVE":
        result_col = F.when(
            F.col(field_name).isNotNull() & (F.col(field_name) > 0),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_12_DATE_NOT_FUTURE":
        result_col = F.when(
            F.col(field_name).isNotNull() & (F.col(field_name).cast("date") <= F.current_date()),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_13_DATE_NOT_PAST_LIMIT":
        result_col = F.when(
            F.col(field_name).isNotNull() &
            (F.col(field_name).cast("date") >= F.date_sub(F.current_date(), 3650)),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_20_ISO_COUNTRY_CODE":
        result_col = F.when(
            F.col(field_name).isNotNull() & F.col(field_name).rlike("^[A-Z]{2}$"),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_21_BOOLEAN_FLAG":
        result_col = F.when(
            F.col(field_name).cast("integer").isin(0, 1),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_30_UNIQUE_IDENTIFIER":
        # Window function — CRITICAL: add tmp_col BEFORE dropping _rn_tmp.
        # If you drop _rn_tmp first and then call df.withColumn(tmp_col, result_col),
        # Spark raises AnalysisException because result_col still references _rn_tmp.
        w = Window.partitionBy(field_name).orderBy(F.monotonically_increasing_id())
        df = df.withColumn("_rn_tmp", F.row_number().over(w))
        df = df.withColumn(tmp_col,
                           F.when(F.col("_rn_tmp") == 1, F.lit(1))
                            .otherwise(F.lit(0))
                            .cast("integer"))
        return df.drop("_rn_tmp")  # early return — tmp_col already added

    else:
        print(f"  [WARN] Unknown generic_rule_desc_short: {generic_rule_desc_short} — result set to NULL")
        result_col = F.lit(None).cast("integer")

    df = df.withColumn(tmp_col, result_col.cast("integer"))
    return df

# COMMAND ----------
# ── Apply all mapped rules (builds one temp column per rule) ──────────
print("\nApplying rules...")
enriched_df = df

for m in mappings:
    enriched_df = compute_rule_result(
        df                      = enriched_df,
        rule_mapping_id                 = m.rule_mapping_id,
        generic_rule_desc_short = m.generic_rule_desc_short,
        field_name              = m.source_field,
    )
    flag_label = "applied"
    print(f"  RULE_{m.rule_mapping_id} ({m.generic_rule_desc_short} -> {m.source_field}): {flag_label}")

# COMMAND ----------
# ── Build single DQ_RESULT column from all temp columns ───────────────
# Format: "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"
segments = []
for m in mappings:
    tmp_col = f"_DQ_{m.rule_mapping_id}"
    label   = f"RULE_{m.rule_mapping_id}"
    segment = F.when(
        F.col(tmp_col).isNull(), F.lit(f"{label}: NULL")
    ).otherwise(
        F.concat(F.lit(f"{label}: "), F.col(tmp_col).cast("string"))
    )
    segments.append(segment)

enriched_df = enriched_df.withColumn("DQ_RESULT", F.concat_ws(" | ", *segments))

# Drop all temporary rule columns
for m in mappings:
    enriched_df = enriched_df.drop(f"_DQ_{m.rule_mapping_id}")

# COMMAND ----------
# ── Write enriched DQ table ───────────────────────────────────────────
output_table = f"{catalog}.{dq_schema}.{target_table}_dq"

enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

print(f"\nWritten {enriched_df.count()} rows to {output_table}")
print(f"Columns: {len(df.columns)} original + 1 DQ_RESULT = {len(enriched_df.columns)} total")

# COMMAND ----------
# ── DQ Summary Scorecard ──────────────────────────────────────────────
# Uses regex to count passed/relevant per rule from DQ_RESULT string.
# Pattern "RULE_{no}: [01]" matches applicable rules; "RULE_{no}: 1" matches passes.
print(f"\n{'='*65}")
print(f"DQ SCORECARD — {target_table} — {report_date}")
print(f"{'='*65}")
print(f"{'RULE_NO':<10} {'FIELD':<25} {'RELEVANT':<10} {'PASS%':<10} {'STATUS'}")
print(f"{'-'*65}")

import uuid
from datetime import datetime

run_id  = str(uuid.uuid4())
run_ts  = datetime.utcnow()
results = []

result_df = spark.table(output_table)

for m in mappings:
    label = f"RULE_{m.rule_mapping_id}"

        continue

    agg = result_df.select(
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: [01]"), 1).otherwise(0)).alias("relevant"),
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: 1"), 1).otherwise(0)).alias("positive")
    ).collect()[0]

    total   = result_df.count()
    relevant = int(agg["relevant"] or 0)
    passed  = int(agg["positive"] or 0)
    failed  = relevant - passed
    pct     = round(passed / relevant * 100, 2) if relevant > 0 else 0.0
    status  = "PASS" if pct >= m.threshold_pct else "BREACH"
    icon    = "v" if status == "PASS" else "x"

    print(f"{m.rule_mapping_id:<10} {m.source_field:<25} {'YES':<10} {pct:<10} {icon} {status}")

    results.append({
        "run_id": run_id, "run_timestamp": run_ts, "report_date": report_date,
        "source_table": f"{catalog}.{dq_schema}.{target_table}",
        "rule_mapping_id": m.rule_mapping_id, "generic_rule_desc_short": m.generic_rule_desc_short,
        "source_field": m.source_field, "rule_description": m.rule_description,
        "total_records": total, "relevant_records": relevant,
        "passed_records": passed, "failed_records": failed,
        "passed_pct": pct, "threshold_pct": m.threshold_pct, "status": status,
    })

print(f"{'='*65}")

# ── Overall DQ% across all rules ─────────────────────────────────────
# NOTE: regexp_extract_all(col, pattern) requires a capturing group () in the pattern.
#       Without it Databricks raises: "Specified regexp group (1) cannot exceed 0".
#       Use filter+split instead — split on ' | ', then rlike-match each segment.
overall = spark.table(output_table).selectExpr(
    "SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))) AS total",
    "SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$')))    AS passed",
    "ROUND(SUM(size(filter(split(DQ_RESULT,' [|] '),x->x rlike ': 1$')))*1.0 / "
    "NULLIF(SUM(size(filter(split(DQ_RESULT,' [|] '),x->x rlike ': [01]$'))),0)*100,2) AS dq_percentage"
).collect()[0]

print(f"\nOverall DQ%: {overall['dq_percentage']}%  "
      f"({overall['passed']} passed / {overall['total']} applicable checks)")

# Persist scorecard to audit table
if results:
    spark.createDataFrame(results).write \
        .format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{dq_schema}.dq_run_results")
    print(f"\nScorecard written to {catalog}.{dq_schema}.dq_run_results (run_id={run_id})")
```

---

## Aggregation Notebook

**`src/dq_professional/notebooks/02_aggregate_dq_results.py`**

Reads `{table}_dq`, aggregates per-rule counts, and appends one row per rule into `dq_results`.
`dq_run_id` is assigned automatically by Delta's identity column — never set manually.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Aggregation — Populate dq_results Table
# MAGIC
# MAGIC Reads the enriched DQ table, aggregates RELEVANT_FLAG and DQ_RESULT per rule,
# MAGIC and appends one row per rule to dq_results (dq_run_id auto-increments).

# COMMAND ----------
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "dbdemos"

try:
    dq_schema = dbutils.widgets.get("dq_schema")
except:
    dq_schema = "dbdemos_dq_professional"

try:
    target_table = dbutils.widgets.get("target_table")
except:
    target_table = "bikes"

from datetime import datetime
from pyspark.sql import functions as F

execution_ts = datetime.utcnow()
dq_table     = f"{catalog}.{dq_schema}.{target_table}_dq"
results_table = f"{catalog}.{dq_schema}.dq_results"

print(f"Aggregation Parameters:")
print(f"  Source DQ table:  {dq_table}")
print(f"  Results table:    {results_table}")
print(f"  Execution time:   {execution_ts}")

# COMMAND ----------
# ── Load enriched DQ table ────────────────────────────────────────────
dq_df = spark.table(dq_table)
total_rows = dq_df.count()
print(f"\nLoaded {total_rows} rows from {dq_table}")

# ── Load active mappings (to know which rule_nos exist) ───────────────
mappings = (
    spark.table(f"{catalog}.{dq_schema}.dq_rule_mappings")
    .filter(
        (F.col("source_catalog") == catalog) &
        (F.col("source_schema")  == dq_schema) &
        (F.col("source_table")   == target_table) &
        (F.col("active")         == 1)
    )
    .orderBy("rule_mapping_id")
    .collect()
)
print(f"Active mappings: {len(mappings)}")

# COMMAND ----------
# ── Aggregate per rule via regex on DQ_RESULT ─────────────────────────
# DQ_RESULT format: "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"
# "RULE_{no}: [01]" matches applicable checks; "RULE_{no}: 1" matches passes.

agg_rows = []

for m in mappings:
    rule_mapping_id = m.rule_mapping_id
    label   = f"RULE_{rule_mapping_id}"

    # Verify DQ_RESULT column exists
    if "DQ_RESULT" not in dq_df.columns:
        print(f"  [WARN] DQ_RESULT column not found in {dq_table} — skipping")
        break

    agg = dq_df.select(
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: [01]"), F.lit(1)).otherwise(F.lit(0)))
            .cast("bigint").alias("relevant"),
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: 1"), F.lit(1)).otherwise(F.lit(0)))
            .cast("bigint").alias("positive")
    ).collect()[0]

    relevant = int(agg["relevant"] or 0)
    positive = int(agg["positive"] or 0)
    agg_rows.append((m.dq_rule_no, relevant, positive))

    pct = round(positive / relevant * 100, 2) if relevant > 0 else 0.0
    print(f"  RULE_{rule_mapping_id}: relevant={relevant}, passed={positive}, score={pct}%")

# COMMAND ----------
# ── Insert into dq_results (dq_run_id assigned by Delta identity) ─────
# Build INSERT via temp view — never include dq_run_id in the INSERT column list

if agg_rows:
    from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

    schema = StructType([
        StructField("dq_execution_timestamp",     TimestampType(), False),
        StructField("dq_rule_no",                 StringType(),   False),
        StructField("dq_number_relevant_records", LongType(),      False),
        StructField("dq_number_pos_results",      LongType(),      False),
    ])

    insert_rows = [(execution_ts, r[0], r[1], r[2]) for r in agg_rows]
    insert_df = spark.createDataFrame(insert_rows, schema)

    # Register as temp view then INSERT — Delta assigns dq_run_id automatically
    insert_df.createOrReplaceTempView("_dq_agg_staging")

    spark.sql(f"""
        INSERT INTO {results_table}
            (dq_execution_timestamp, dq_rule_no,
             dq_number_relevant_records, dq_number_pos_results)
        SELECT
            dq_execution_timestamp,
            dq_rule_no,
            dq_number_relevant_records,
            dq_number_pos_results
        FROM _dq_agg_staging
    """)

    print(f"\nInserted {len(agg_rows)} rows into {results_table}")
    print("dq_run_id values assigned automatically by Delta identity column.")

# COMMAND ----------
# ── Confirm inserted rows ─────────────────────────────────────────────
print("\nLatest entries in dq_results:")
spark.sql(f"""
    SELECT dq_run_id, dq_execution_timestamp, dq_rule_no,
           dq_number_relevant_records, dq_number_pos_results,
           ROUND(dq_number_pos_results / dq_number_relevant_records * 100, 2) AS pass_pct
    FROM {results_table}
    ORDER BY dq_run_id DESC
    LIMIT 20
""").show(20, truncate=False)
```

---

## Complete DAB Pipeline

Suggested Databricks Asset Bundle with **3 tasks** running in sequence:

```
Task 1: apply_dq_rules          ← enriches {table}_dq with single DQ_RESULT column
    ↓
Task 2: aggregate_dq_results    ← aggregates to dq_results (auto-increment ID)
    ↓
Task 3: dq_scorecard            ← (optional) prints summary scorecard for audit log
```

**`databricks.yml`:**

```yaml
bundle:
  name: dq_professional

variables:
  catalog:
    description: "Unity Catalog name"
    default: "dbdemos"

  dq_schema:
    description: "DQ schema name"
    default: "dbdemos_dq_professional"

  target_table:
    description: "Source table to run DQ against (e.g. bikes)"
    default: "bikes"

targets:
  dev:
    mode: development
    variables:
      catalog: "dbdemos"
      target_table: "bikes"

  prod:
    mode: production
    variables:
      catalog: "dbdemos"
      target_table: "bikes"

resources:
  jobs:
    dq_professional_job:
      name: dq_professional_${var.target_table}_${bundle.target}

      tasks:
        # Task 1 — Apply DQ rules → writes {table}_dq
        - task_key: apply_dq_rules
          notebook_task:
            notebook_path: ../src/dq_professional/notebooks/01_apply_dq_rules.py
            base_parameters:
              catalog:      ${var.catalog}
              dq_schema:    ${var.dq_schema}
              target_table: ${var.target_table}

        # Task 2 — Aggregate results → appends to dq_results (auto-increment dq_run_id)
        - task_key: aggregate_dq_results
          depends_on:
            - task_key: apply_dq_rules
          notebook_task:
            notebook_path: ../src/dq_professional/notebooks/02_aggregate_dq_results.py
            base_parameters:
              catalog:      ${var.catalog}
              dq_schema:    ${var.dq_schema}
              target_table: ${var.target_table}

      # Daily at 07:00 UTC — before regulatory reporting window opens
      schedule:
        quartz_cron_expression: "0 0 7 * * ?"
        timezone_id: "UTC"

      email_notifications:
        on_failure:
          - ${workspace.current_user.userName}
```

**Deploy commands (automatic — no confirmation needed):**
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

**Run job (ask user first):**
```bash
# "Do you want to run the DQ pipeline for 'bikes' now?"
databricks bundle run dq_professional_job -t dev
```

---

## Setup Workflow (Run Once Per New Table)

```python
# ── Step 1: Create schema (databricks-unity-catalog skill) ───────────
create_schema(catalog_name="dbdemos", schema_name="dbdemos_dq_professional",
    comment="Professional DQ framework")

# ── Step 2: Create dq_results table with auto-increment ID ───────────
spark.sql("DROP TABLE IF EXISTS dbdemos.dbdemos_dq_professional.dq_results")
spark.sql("""
    CREATE TABLE dbdemos.dbdemos_dq_professional.dq_results (
        dq_run_id                   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        dq_execution_timestamp      TIMESTAMP  NOT NULL,
        dq_rule_no             STRING        NOT NULL,
        dq_number_relevant_records  BIGINT     NOT NULL,
        dq_number_pos_results       BIGINT     NOT NULL
    )
    USING DELTA
    COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'
""")

# ── Step 3: Seed generic rules (once — reused across all tables) ──────
# Run generic rules seed block (Table 1 section above)

# ── Step 4: Create validation request + seed (once per entity) ────────
# Run dq_validation_request seed block (Table 2 section above)

# ── Step 5: Copy source table into dq schema ─────────────────────────
copy_source_table("dbdemos.dbdemos_uc_lineage.bikes", "bikes")

# ── Step 6: Register rule mappings for the table ─────────────────────
# Run rule mappings seed block (Table 3 section above)

# ── Step 7: Run the pipeline ─────────────────────────────────────────
# Notebook 01 → writes dbdemos.dbdemos_dq_professional.bikes_dq
# Notebook 02 → appends to dbdemos.dbdemos_dq_professional.dq_results
#               dq_run_id auto-assigned: 1, 2, 3, ...
```

---

## Adding a New Generic Rule

To extend the framework with a new rule type:

1. **Add to `dq_rules_generic`** with a new `generic_rule_desc_short`
2. **Add a branch in `compute_rule_result()`** dispatcher for that `generic_rule_desc_short`
3. **Add mappings in `dq_rule_mappings`** for the tables/fields it applies to

```python
# Example: add RULE_40_REVENUE_WITHIN_RANGE
# Step 1 — INSERT into dq_rules_generic (generic_rule_id auto-assigned)
spark.sql("""
    INSERT INTO dbdemos.dbdemos_dq_professional.dq_rules_generic
        (generic_rule_desc_short, rule_description, dq_category, rule_detail,
         rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
         active, created_date, created_by)
    VALUES (
        'RULE_40_REVENUE_WITHIN_RANGE', 'Revenue Within Plausible Range', 'ACCURACY',
        'Revenue must be between 0 and 1,000,000 per ride day.',
        'CASE WHEN {field} BETWEEN 0 AND 1000000 THEN 1 ELSE 0 END',
        'DOUBLE,DECIMAL,FLOAT', 'P3', 'Pillar1',
        1, current_date(), 'dq-framework'
    )
""")

# Step 2 — add dispatcher branch in compute_rule_result():
# elif generic_rule_desc_short == "RULE_40_REVENUE_WITHIN_RANGE":
#     result_col = F.when(
#         F.col(field_name).between(0, 1_000_000), F.lit(1)
#     ).otherwise(F.lit(0))

# Step 3 — register mapping
new_mapping = Row(
    rule_mapping_id=109, generic_rule_desc_short="RULE_40_REVENUE_WITHIN_RANGE",
    source_catalog="dbdemos", source_schema="dbdemos_dq_professional",
    source_table="bikes", source_field="total_revenue",
    rule_description="total_revenue must be between 0 and 1,000,000",
    threshold_pct=99.5, active=1, created_date=date.today(), created_by="dq-framework"
)
```

---

## Output Column Reference

For a table with mappings for rule_mapping_id 101, 102, 103, the enriched table adds **one column**:

```
DQ_RESULT   STRING   Concatenated results for all active rule mappings

Example value:
  "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"
   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   RULE_101: 1    — mapping 101 passed
   RULE_102: 0    — mapping 102 failed
   RULE_103: NULL — rule not applicable (row filtered by status)
```

### Regex patterns for querying DQ_RESULT

| Purpose | Pattern |
|---------|---------|
| Count applicable checks for rule 101 | `RULE_101: [01]` |
| Count passed checks for rule 101 | `RULE_101: 1` |
| Overall DQ% across all rules | `': [01]'` (total) vs `': 1'` (passed) |

```sql
-- Overall DQ% scorecard
-- IMPORTANT: regexp_extract_all(col, pattern) requires a capturing group () in Databricks.
-- Without it you get: "Specified regexp group (1) cannot exceed 0"
-- Use filter+split instead:
SELECT
    SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))) AS total,
    SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$')))    AS passed,
    SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$'))) * 1.0 /
    NULLIF(SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))), 0) AS dq_percentage
FROM bikes_dq;
```

---

## Dashboard Query

The dashboard joins `dq_results` → `dq_rule_mappings` → `dq_rules_generic` to resolve
the effective category (`COALESCE(mapping override, generic default)`).

**`src/dq_professional/notebooks/03_dq_dashboard.py`**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Dashboard
# MAGIC
# MAGIC Overall DQ score and per-category breakdown.
# MAGIC Effective category = mapping override if set, otherwise inherited from generic rule.

# COMMAND ----------
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "dbdemos"

try:
    dq_schema = dbutils.widgets.get("dq_schema")
except:
    dq_schema = "dbdemos_dq_professional"

from pyspark.sql import functions as F

# ── Overall Summary ──────────────────────────────────────────────────
print("=" * 65)
print("  DQ DASHBOARD — Overall Summary")
print("=" * 65)

spark.sql(f"""
    SELECT
        COUNT(DISTINCT r.dq_rule_no)                              AS TOTAL_RULES,
        COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records > 0
              THEN r.dq_rule_no END)                              AS RULES_APPLIED,
        COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records = 0
              THEN r.dq_rule_no END)                              AS RULES_SKIPPED,
        SUM(r.dq_number_relevant_records)                         AS TOTAL_RELEVANT_RECORDS,
        SUM(r.dq_number_pos_results)                              AS TOTAL_POS_RESULTS,
        SUM(r.dq_number_relevant_records - r.dq_number_pos_results) AS TOTAL_FAILED,
        CASE WHEN SUM(r.dq_number_relevant_records) = 0 THEN NULL
             ELSE ROUND(SUM(r.dq_number_pos_results)
                  / SUM(r.dq_number_relevant_records) * 100, 2)
        END                                                        AS OVERALL_PASS_PCT
    FROM {catalog}.{dq_schema}.dq_results r
""").show(1, truncate=False)

# ── Per-Category Breakdown ───────────────────────────────────────────
print("=" * 65)
print("  DQ DASHBOARD — Per Category")
print("=" * 65)

spark.sql(f"""
    SELECT
        COALESCE(m.dq_category, g.dq_category)    AS DQ_CATEGORY,
        COUNT(DISTINCT r.dq_rule_no)               AS TOTAL_RULES,
        COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records > 0
              THEN r.dq_rule_no END)               AS RULES_APPLIED,
        SUM(r.dq_number_relevant_records)          AS TOTAL_RELEVANT_RECORDS,
        SUM(r.dq_number_pos_results)               AS TOTAL_POS_RESULTS,
        SUM(r.dq_number_relevant_records
            - r.dq_number_pos_results)             AS TOTAL_FAILED,
        CASE WHEN SUM(r.dq_number_relevant_records) = 0 THEN NULL
             ELSE ROUND(SUM(r.dq_number_pos_results)
                  / SUM(r.dq_number_relevant_records) * 100, 2)
        END                                         AS PASS_PCT
    FROM {catalog}.{dq_schema}.dq_results r
    JOIN {catalog}.{dq_schema}.dq_rule_mappings m
      ON r.dq_rule_no = m.dq_rule_no
    JOIN {catalog}.{dq_schema}.dq_rules_generic g
      ON m.generic_rule_desc_short = g.generic_rule_desc_short
    GROUP BY
        COALESCE(m.dq_category, g.dq_category)
    ORDER BY DQ_CATEGORY
""").show(20, truncate=False)

# ── Per-Rule Detail ──────────────────────────────────────────────────
print("=" * 65)
print("  DQ DASHBOARD — Per Rule Detail")
print("=" * 65)

spark.sql(f"""
    SELECT
        r.dq_run_id,
        r.dq_rule_no,
        COALESCE(m.dq_category, g.dq_category)    AS DQ_CATEGORY,
        m.source_field,
        g.generic_rule_desc_short,
        r.dq_number_relevant_records,
        r.dq_number_pos_results,
        r.dq_number_relevant_records
            - r.dq_number_pos_results              AS FAILED,
        CASE WHEN r.dq_number_relevant_records = 0 THEN NULL
             ELSE ROUND(r.dq_number_pos_results
                  / r.dq_number_relevant_records * 100, 2)
        END                                         AS PASS_PCT,
        CASE WHEN r.dq_number_relevant_records = 0 THEN 'SKIPPED'
             WHEN r.dq_number_pos_results
                = r.dq_number_relevant_records     THEN 'PASS'
             ELSE 'BREACH'
        END                                         AS STATUS
    FROM {catalog}.{dq_schema}.dq_results r
    JOIN {catalog}.{dq_schema}.dq_rule_mappings m
      ON r.dq_rule_no = m.dq_rule_no
    JOIN {catalog}.{dq_schema}.dq_rules_generic g
      ON m.generic_rule_desc_short = g.generic_rule_desc_short
    ORDER BY DQ_CATEGORY, r.dq_rule_no
""").show(50, truncate=False)
```

---

## Best Practices

- **Never delete breach records** — append only to `dq_run_results`
- **rule_mapping_id is immutable** once created — it is embedded as the label in the DQ_RESULT string (e.g. `DQ101`)
- **Threshold 100.0** for mandatory fields, 99.5 for enrichment fields
- **One enrichment notebook per table** — parameterise with `target_table` widget

---

## Integration with Other Skills

| Skill | Role |
|-------|------|
| `databricks-unity-catalog` | Create `dbdemos_dq_professional` schema |
| `databricks-testing` | Test individual rule logic on cluster before pipeline deployment |
| `databricks-bundle-deploy` | Package and schedule the DQ pipeline as a DAB |
