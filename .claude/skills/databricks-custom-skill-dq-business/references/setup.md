# DQ Framework - Setup Reference

CREATE TABLE DDL and seed data for all registry tables.
Run steps in order. For 2nd+ entities, use MERGE instead of INSERT for shared tables (see SKILL.md).

---

## CREATE TABLE - dq_rules_generic

```python
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
    COMMENT 'Generic DQ rule catalogue. generic_rule_id auto-increments - never set manually.'
""")
```

---

## Seed Data - Generic Rules

Tuple order: `(generic_rule_desc_short, rule_description, dq_category, rule_detail,
rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar)`

```python
from pyspark.sql import Row
from datetime import date

generic_rules = [
    # -- COMPLETENESS -------------------------------------------------------
    ("RULE_1_DATE_NOT_NULL",
     "Date Not Null", "COMPLETENESS",
     "Checks that a DATE or TIMESTAMP field is not NULL. Returns 1 if populated, 0 if NULL.",
     "CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3,P4", "Pillar1,QRT"),

    ("RULE_2_STRING_NOT_NULL",
     "String Not Null", "COMPLETENESS",
     "Checks that a STRING field is not NULL or empty (whitespace-only counts as empty).",
     "CASE WHEN {field} IS NOT NULL AND trim({field}) != '' THEN 1 ELSE 0 END",
     "STRING", "P3,P4", "Pillar1,QRT"),

    ("RULE_3_NUMERIC_NOT_NULL",
     "Numeric Not Null", "COMPLETENESS",
     "Checks that a numeric field is not NULL.",
     "CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT", "P3,P4", "Pillar1"),

    # -- ACCURACY -----------------------------------------------------------
    ("RULE_10_NUMERIC_NON_NEGATIVE",
     "Numeric Non-Negative", "ACCURACY",
     "Checks that a numeric field is >= 0. Amounts and counts must not be negative.",
     "CASE WHEN {field} >= 0 THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT", "P3", "Pillar1"),

    ("RULE_11_NUMERIC_POSITIVE",
     "Numeric Positive", "ACCURACY",
     "Checks that a numeric field is > 0.",
     "CASE WHEN {field} > 0 THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT", "P3", "Pillar1"),

    ("RULE_12_DATE_NOT_FUTURE",
     "Date Not in Future", "ACCURACY",
     "Checks that a DATE field is not in the future relative to current_date().",
     "CASE WHEN {field} <= current_date() THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3", "QRT"),

    ("RULE_13_DATE_NOT_PAST_LIMIT",
     "Date Within Reasonable Past", "ACCURACY",
     "Checks that a DATE field is not older than 10 years (data staleness check).",
     "CASE WHEN {field} >= date_sub(current_date(), 3650) THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3,P5", "Pillar1"),

    ("RULE_14_DATE_NOT_FUTURE_LIMIT",
     "Date Not Beyond Future Limit", "ACCURACY",
     "Checks that a DATE field is not more than 1 year in the future. "
     "Used for regulatory value dates that may legitimately be T+1 or end-of-year.",
     "CASE WHEN {field} <= date_add(current_date(), 365) THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3", "Pillar1,QRT"),

    # -- VALIDITY -----------------------------------------------------------
    ("RULE_20_ISO_COUNTRY_CODE",
     "ISO 3166-1 Alpha-2 Country Code", "VALIDITY",
     "Checks that a STRING field contains a valid ISO 3166-1 alpha-2 country code "
     "(exactly 2 uppercase letters).",
     "CASE WHEN {field} RLIKE '^[A-Z]{2}$' THEN 1 ELSE 0 END",
     "STRING", "P3", "QRT"),

    ("RULE_21_BOOLEAN_FLAG",
     "Boolean Flag (0 or 1)", "VALIDITY",
     "Checks that an integer field contains only 0 or 1.",
     "CASE WHEN {field} IN (0, 1) THEN 1 ELSE 0 END",
     "INTEGER", "P3", "Pillar2"),

    # -- UNIQUENESS ---------------------------------------------------------
    ("RULE_30_UNIQUE_IDENTIFIER",
     "Unique Identifier", "UNIQUENESS",
     "Checks that a field has no duplicate values across the dataset. "
     "Applied at dataset level - marks first occurrence as 1, duplicates as 0.",
     "ROW_NUMBER() OVER (PARTITION BY {field} ORDER BY {field}) = 1",
     "STRING,INTEGER,LONG", "P3", "Pillar1"),

    # -- CONSISTENCY --------------------------------------------------------
    ("RULE_40_TEMPORAL_WINDOW_CONSISTENCY",
     "Temporal Window Consistency", "CONSISTENCY",
     "Checks that field_a (e.g. transaction_date) is <= field_b (e.g. settlement_date). "
     "Use pipe-delimited source_field: 'date_col|window_col'. "
     "dq_rule_no sanitizes | to _vs_ automatically.",
     "CASE WHEN {field_a} IS NOT NULL AND {field_b} IS NOT NULL "
     "AND {field_a} <= {field_b} THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3,P4", "Pillar1,QRT"),

    # -- ACCURACY (multi-field) ---------------------------------------------
    ("RULE_41_POSITIVE_IF_COUNT_NONZERO",
     "Positive Amount If Count Non-Zero", "ACCURACY",
     "Checks that an amount column is > 0 when a count column is > 0. "
     "Passes if count is 0 or NULL (no transactions, no required amount). "
     "Use pipe-delimited source_field: 'amount_col|count_col'. "
     "dq_rule_no sanitizes | to _vs_ automatically.",
     "CASE WHEN {count_field} > 0 THEN (CASE WHEN {amount_field} > 0 THEN 1 ELSE 0 END) "
     "ELSE 1 END",
     "DECIMAL,DOUBLE,FLOAT,INTEGER,LONG", "P3", "Pillar1"),
]

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
print(f"Seeded {len(rows)} generic rules.")
```

---

## CREATE TABLE - dq_validation_request

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS dbdemos.dbdemos_dq_professional.dq_validation_request (
        dq_val_request_id             BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        dq_validation_request_short   STRING NOT NULL,
        owner_dept                    STRING,
        owner_user                    STRING,
        active                        INTEGER,
        created_date                  DATE,
        created_by                    STRING
    )
    USING DELTA
    COMMENT 'DQ validation request registry. dq_val_request_id auto-increments - never set manually.'
""")
```

### Seed - Validation Request (entity 1: bikes)

```python
from pyspark.sql import Row
from datetime import date

vr_rows = [Row(
    dq_validation_request_short="DQ_VAL_REQUEST_1_Customer_entity",
    owner_dept="Dept. Master Data", owner_user="Mr. Alex Smith",
    active=1, created_date=date.today(), created_by="dq-framework"
)]

vr_df = spark.createDataFrame(vr_rows)
vr_df.createOrReplaceTempView("_dq_vr_staging")

spark.sql("""
    INSERT INTO dbdemos.dbdemos_dq_professional.dq_validation_request
        (dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by)
    SELECT
        dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by
    FROM _dq_vr_staging
""")
print("Seeded validation request. dq_val_request_id assigned by Delta.")
```

---

## CREATE TABLE - dq_rule_mappings

Note: `rule_mapping_id` is **manually assigned** (NOT IDENTITY) so we control the RULE_XYZ label.

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS dbdemos.dbdemos_dq_professional.dq_rule_mappings (
        rule_mapping_id          BIGINT        NOT NULL,
        dq_rule_no               STRING        NOT NULL,
        dq_val_request_id        INTEGER       NOT NULL,
        generic_rule_desc_short  STRING        NOT NULL,
        source_catalog           STRING,
        source_schema            STRING,
        source_table             STRING,
        source_field             STRING,
        rule_description         STRING,
        threshold_pct            DOUBLE,
        dq_category              STRING,
        owner_dept               STRING,
        owner_user               STRING,
        active                   INTEGER,
        created_date             DATE,
        created_by               STRING
    )
    USING DELTA
    COMMENT 'Maps generic rules to specific catalog.schema.table.field combinations. rule_mapping_id is manually assigned.'
""")
```

### Seed - Rule Mappings (bikes example)

Tuple order: `(rule_mapping_id, val_req_id, generic_rule_desc_short, catalog, schema, table, field,
description, threshold_pct, dq_category, owner_dept, owner_user)`

Note: `active` is hardcoded as 1 in the Row constructor - do NOT include it in the tuple.

```python
from pyspark.sql import Row
from datetime import date

mappings = [
    # (rule_mapping_id, val_req_id, generic_rule_desc_short, catalog, schema, table, field,
    #  description, threshold_pct, dq_category, owner_dept, owner_user)
    (101, 1, "RULE_1_DATE_NOT_NULL",         "dbdemos", "dbdemos_dq_professional", "bikes", "ride_date",
     "ride_date must not be NULL",            100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),

    (102, 1, "RULE_3_NUMERIC_NOT_NULL",       "dbdemos", "dbdemos_dq_professional", "bikes", "total_rides",
     "total_rides must not be NULL",          100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),

    (103, 1, "RULE_3_NUMERIC_NOT_NULL",       "dbdemos", "dbdemos_dq_professional", "bikes", "total_revenue",
     "total_revenue must not be NULL",        100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),

    (104, 1, "RULE_10_NUMERIC_NON_NEGATIVE",  "dbdemos", "dbdemos_dq_professional", "bikes", "total_rides",
     "total_rides must be >= 0",              100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),

    # dq_category override: total_revenue >= 0 is an ACCURACY check in financial context
    (105, 1, "RULE_10_NUMERIC_NON_NEGATIVE",  "dbdemos", "dbdemos_dq_professional", "bikes", "total_revenue",
     "total_revenue must be >= 0",            100.0, "ACCURACY",  "Dept. Master Data", "Mr. Alex Smith"),

    (106, 1, "RULE_21_BOOLEAN_FLAG",          "dbdemos", "dbdemos_dq_professional", "bikes", "requires_maintenance",
     "requires_maintenance must be 0 or 1",   100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),

    (107, 1, "RULE_12_DATE_NOT_FUTURE",       "dbdemos", "dbdemos_dq_professional", "bikes", "ride_date",
     "ride_date must not be in the future",   100.0, None,        "Dept. Master Data", "Mr. Alex Smith"),

    (108, 1, "RULE_20_ISO_COUNTRY_CODE",      "dbdemos", "dbdemos_dq_professional", "bikes", "country_code",
     "country_code must be ISO 3166-1 alpha-2", 100.0, None,      "Dept. Master Data", "Mr. Alex Smith"),
]

rows = [Row(
    rule_mapping_id          = m[0],
    dq_rule_no               = ("DQ_" + str(m[0]) + "_"
                                + "_".join(m[2].split("_")[2:]) + "_"
                                + m[6].replace("|", "_vs_")),
    dq_val_request_id        = m[1],
    generic_rule_desc_short  = m[2],
    source_catalog           = m[3],
    source_schema            = m[4],
    source_table             = m[5],
    source_field             = m[6],
    rule_description         = m[7],
    threshold_pct            = m[8],
    dq_category              = m[9],
    owner_dept               = m[10],
    owner_user               = m[11],
    active=1, created_date=date.today(), created_by="dq-framework"
) for m in mappings]

spark.createDataFrame(rows).write \
    .format("delta").mode("append") \
    .saveAsTable("dbdemos.dbdemos_dq_professional.dq_rule_mappings")

print(f"Registered {len(rows)} rule mappings.")
```

---

## CREATE TABLE - dq_results (aggregated, written by notebook 02)

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS dbdemos.dbdemos_dq_professional.dq_results (
        dq_run_id                   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
        dq_execution_timestamp      TIMESTAMP NOT NULL,
        dq_rule_no                  STRING    NOT NULL,
        dq_number_relevant_records  BIGINT    NOT NULL,
        dq_number_pos_results       BIGINT    NOT NULL
    )
    USING DELTA
    COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'
""")
print("Table dq_results created.")
```

---

## CREATE TABLE - dq_run_results (rich scorecard, written by notebook 01)

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS dbdemos.dbdemos_dq_professional.dq_run_results (
        run_id                  STRING,
        run_timestamp           TIMESTAMP,
        report_date             STRING,
        source_table            STRING,
        rule_mapping_id         INTEGER,
        generic_rule_desc_short STRING,
        source_field            STRING,
        rule_description        STRING,
        total_records           BIGINT,
        relevant_records        BIGINT,
        passed_records          BIGINT,
        failed_records          BIGINT,
        passed_pct              DOUBLE,
        threshold_pct           DOUBLE,
        status                  STRING
    )
    USING DELTA
    COMMENT 'Rich per-rule per-run scorecard with pass%, threshold, and PASS/BREACH status.'
""")
print("Table dq_run_results created.")
```

---

## Copy Source Table

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
    print(f"Copied {count} rows from {source_full_name} -> {target_full}")

# Usage:
copy_source_table("dbdemos.dbdemos_uc_lineage.bikes", "bikes")
```
