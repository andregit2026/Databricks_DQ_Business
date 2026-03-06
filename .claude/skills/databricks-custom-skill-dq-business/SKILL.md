---
name: databricks-custom-skill-dq-business
description: "Production-grade BCBS239 and Solvency II-compliant data quality framework for Databricks. Implements a generic rule registry, rule-to-field mappings, dynamic rule dispatching, and enriched DQ output tables with a single DQ_RESULT column (e.g. 'RULE_101: 1 | RULE_102: 0 | RULE_103: N'). Covers all six DQ categories: ACCURACY, COMPLETENESS, TIMELINESS, CONSISTENCY, VALIDITY, UNIQUENESS. Produces breach registers, audit trails, and per-category dashboards. Use this skill whenever the user mentions: DQ framework, data quality pipeline, BCBS239, Solvency II, breach register, DQ rule registry, validation request, DQ scorecard, completeness or accuracy or validity checks, or wants to build or extend a rule-based data quality system on any Databricks Delta table."
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - mcp__databricks__execute_sql
model: claude-sonnet-4-6
user-invocable: true
---

# Databricks Data Quality (BCBS239 / Solvency II)

Professional, regulator-grade data quality framework on Databricks. Uses a **generic rule registry**
mapped to specific table fields, producing enriched DQ output tables with a standardised
`DQ_RESULT` column and per-run aggregated results for regulatory reporting.

---

## When to Use This Skill

- Setting up a DQ framework for a new source table (e.g. `bikes`, `counterparty_exposures`, `fund_trans_gold`)
- Registering and applying generic DQ rules (NOT_NULL, DATE_FORMAT, RANGE, REFERENTIAL, UNIQUENESS, etc.)
- Producing enriched DQ output tables with a single `DQ_RESULT` string column for downstream reporting
- Establishing audit trails, breach registers, and SLA monitoring for BCBS239 / Solvency II compliance
- Extending the framework with new rule types or adding a second entity to the shared registry

---

## DQ Categories

| `dq_category` | Meaning |
|---------------|---------|
| `ACCURACY` | Values are correct and within valid ranges |
| `COMPLETENESS` | All required fields are populated |
| `TIMELINESS` | Data is available within the required timeframe |
| `CONSISTENCY` | Values are consistent across systems or over time |
| `VALIDITY` | Values conform to domain rules and formats |
| `UNIQUENESS` | No duplicate values in uniqueness-constrained fields |

**Category inheritance:** `COALESCE(dq_rule_mappings.dq_category, dq_rules_generic.dq_category)`
Mapping-level override is NULL by default; set it only when a rule's category differs in a specific business context (e.g. override COMPLETENESS -> ACCURACY for a financial amount field).

---

## Core Architecture

```
dbdemos.dbdemos_dq_professional
│
├── dq_rules_generic         <- Catalogue of all reusable generic DQ rules
├── dq_validation_request    <- Groups DQ checks by business entity (one row per entity)
├── dq_rule_mappings         <- Maps generic rules to specific catalog.schema.table.field
│
├── {entity}                 <- Copy of source table (e.g. bikes, fund_trans_gold)
├── {entity}_dq              <- Source table + single DQ_RESULT column
│
├── dq_results               <- Aggregated per-rule per-run counts (for dashboards)
│                               Written by 02_aggregate_dq_results.py
│                               dq_run_id: IDENTITY auto-increment
└── dq_run_results           <- Rich per-rule per-run scorecard with pass%, status, threshold
                                Written by 01_apply_dq_rules.py scorecard section
```

---

## Table Schemas (Key Columns)

### dq_rules_generic
| Column | Type | Note |
|--------|------|------|
| `generic_rule_id` | BIGINT IDENTITY | Auto-assigned - never set manually |
| `generic_rule_desc_short` | STRING NOT NULL | e.g. `RULE_1_DATE_NOT_NULL` - natural PK |
| `dq_category` | STRING | Default category for this rule type |
| `rule_logic_template` | STRING | SQL template with `{field}` placeholder |
| `bcbs239_principle` | STRING | e.g. `P3,P4` |
| `solvencyii_pillar` | STRING | e.g. `Pillar1,QRT` |
| `active` | INTEGER | 1 = active, 0 = deprecated |

### dq_rule_mappings
| Column | Type | Note |
|--------|------|------|
| `rule_mapping_id` | BIGINT | Manually assigned - becomes XYZ in `RULE_XYZ` labels |
| `dq_rule_no` | STRING | e.g. `DQ_101_DATE_NOT_NULL_ride_date` |
| `dq_val_request_id` | INTEGER | FK -> dq_validation_request |
| `generic_rule_desc_short` | STRING | FK -> dq_rules_generic |
| `source_field` | STRING | Column to check; pipe-separated for multi-field rules |
| `dq_category` | STRING | Optional override (NULL = inherit from generic rule) |
| `threshold_pct` | DOUBLE | Min pass rate, e.g. 100.0 |
| `active` | INTEGER | 1 = apply in pipeline, 0 = skip |

### dq_results (written by notebook 02)
| Column | Type | Note |
|--------|------|------|
| `dq_run_id` | BIGINT IDENTITY | Auto-assigned by Delta - never include in INSERT |
| `dq_execution_timestamp` | TIMESTAMP | UTC |
| `dq_rule_no` | STRING | e.g. `DQ_101_DATE_NOT_NULL_ride_date` |
| `dq_number_relevant_records` | BIGINT | Records where rule was applicable |
| `dq_number_pos_results` | BIGINT | Records that passed |

---

## DQ_RESULT Format

```
DQ_RESULT = "RULE_101: 1 | RULE_102: 0 | RULE_103: N"
```

| Value | Meaning |
|-------|---------|
| `1` | Rule passed |
| `0` | Rule failed |
| `N` | Rule not applicable for this row |

**Querying DQ_RESULT - IMPORTANT: use `filter+split`, NOT `regexp_extract_all()`.**
`regexp_extract_all()` requires a capturing group `()` in the pattern; without it Databricks raises
`"Specified regexp group (1) cannot exceed 0"`.

```sql
-- Overall DQ% scorecard
SELECT
    SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))) AS total_checks,
    SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$')))    AS passed_checks,
    ROUND(
        SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$'))) * 1.0 /
        NULLIF(SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))), 0) * 100, 2
    ) AS dq_percentage
FROM {entity}_dq;

-- Count applicable checks for a single rule
SELECT COUNT(*) FROM {entity}_dq WHERE DQ_RESULT RLIKE 'RULE_101: [01]';
```

---

## Generic Rules Registry

Full CREATE TABLE DDL and INSERT seed data are in `references/setup.md`.
Read it when setting up a schema or adding rules.

| Rule | Short Name | Category | Multi-field? |
|------|-----------|---------|--------------|
| RULE_1 | DATE_NOT_NULL | COMPLETENESS | No |
| RULE_2 | STRING_NOT_NULL | COMPLETENESS | No |
| RULE_3 | NUMERIC_NOT_NULL | COMPLETENESS | No |
| RULE_10 | NUMERIC_NON_NEGATIVE | ACCURACY | No |
| RULE_11 | NUMERIC_POSITIVE | ACCURACY | No |
| RULE_12 | DATE_NOT_FUTURE | ACCURACY | No |
| RULE_13 | DATE_NOT_PAST_LIMIT | ACCURACY | No |
| RULE_14 | DATE_NOT_FUTURE_LIMIT | ACCURACY | No |
| RULE_20 | ISO_COUNTRY_CODE | VALIDITY | No |
| RULE_21 | BOOLEAN_FLAG | VALIDITY | No |
| RULE_30 | UNIQUE_IDENTIFIER | UNIQUENESS | No |
| RULE_40 | TEMPORAL_WINDOW_CONSISTENCY | CONSISTENCY | Yes - `"date_col\|window_col"` |
| RULE_41 | POSITIVE_IF_COUNT_NONZERO | ACCURACY | Yes - `"amount_col\|count_col"` |

RULE_40 and RULE_41 use **pipe-delimited** `source_field`. `dq_rule_no` sanitizes `|` to `_vs_`
(e.g. `DQ_201_TEMPORAL_WINDOW_CONSISTENCY_transaction_date_vs_settlement_date`).

---

## Rule Dispatcher - `compute_rule_result()`

This function is the core of notebook 01. It adds a temporary `_DQ_{rule_mapping_id}` column.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def compute_rule_result(df, rule_mapping_id: int, generic_rule_desc_short: str,
                        field_name: str):
    """
    Adds tmp column _DQ_{rule_mapping_id}:
      1    = rule passed
      0    = rule failed
      NULL = rule not applicable (set by caller for status-filtered rows)
    Window-function rules (RULE_30) use early return - tmp_col is added inside
    before any drop to avoid AnalysisException.
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
            F.col(field_name).isNotNull() &
            (F.col(field_name).cast("date") <= F.current_date()),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_13_DATE_NOT_PAST_LIMIT":
        result_col = F.when(
            F.col(field_name).isNotNull() &
            (F.col(field_name).cast("date") >= F.date_sub(F.current_date(), 3650)),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_14_DATE_NOT_FUTURE_LIMIT":
        # Date must not be more than 1 year in the future (regulatory value-date tolerance)
        result_col = F.when(
            F.col(field_name).isNotNull() &
            (F.col(field_name).cast("date") <= F.date_add(F.current_date(), 365)),
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
        # CRITICAL: add tmp_col to df BEFORE dropping _rn_tmp.
        # Dropping _rn_tmp first and then calling df.withColumn(tmp_col, result_col)
        # raises AnalysisException because result_col still references the dropped column.
        w = Window.partitionBy(field_name).orderBy(F.monotonically_increasing_id())
        df = df.withColumn("_rn_tmp", F.row_number().over(w))
        df = df.withColumn(tmp_col,
                           F.when(F.col("_rn_tmp") == 1, F.lit(1))
                            .otherwise(F.lit(0))
                            .cast("integer"))
        return df.drop("_rn_tmp")  # early return - tmp_col already added

    elif generic_rule_desc_short == "RULE_40_TEMPORAL_WINDOW_CONSISTENCY":
        # source_field = "date_col|window_col" (pipe-separated)
        # Checks date_col <= window_col (e.g. transaction_date <= settlement_date)
        fields = field_name.split("|")
        date_col, window_col = fields[0], fields[1]
        result_col = F.when(
            F.col(date_col).isNotNull() & F.col(window_col).isNotNull() &
            (F.col(date_col).cast("date") <= F.col(window_col).cast("date")),
            F.lit(1)
        ).otherwise(F.lit(0))

    elif generic_rule_desc_short == "RULE_41_POSITIVE_IF_COUNT_NONZERO":
        # source_field = "amount_col|count_col" (pipe-separated)
        # IF count_col > 0 THEN amount_col must be > 0, ELSE pass (count is 0 or NULL)
        fields = field_name.split("|")
        amount_col, count_col = fields[0], fields[1]
        result_col = F.when(
            F.col(count_col).isNotNull() & (F.col(count_col) > 0),
            F.when(
                F.col(amount_col).isNotNull() & (F.col(amount_col) > 0),
                F.lit(1)
            ).otherwise(F.lit(0))
        ).otherwise(F.lit(1))  # count is 0 or NULL -> rule not violated -> pass

    else:
        print(f"  [WARN] Unknown rule: {generic_rule_desc_short} - result set to NULL")
        result_col = F.lit(None).cast("integer")

    df = df.withColumn(tmp_col, result_col.cast("integer"))
    return df
```

---

## Critical Patterns

### Status-column conditional filter
Not all source tables have a `status` column. Check before filtering:
```python
df = spark.table(full_source)
if "status" in df.columns:
    df = df.filter(F.col("status").isin("active", "ACTIVE"))
```

### Multi-field rules (pipe-delimited source_field)
RULE_40 and RULE_41 operate on two columns. Store as pipe-separated in `source_field`:
```python
source_field = "transaction_date|settlement_date"  # RULE_40
source_field = "total_amount|num_transactions"      # RULE_41
```
Always sanitize `|` to `_vs_` when computing `dq_rule_no`:
```python
sanitized_field = source_field.replace("|", "_vs_")
dq_rule_no = (
    "DQ_" + str(rule_mapping_id) + "_"
    + "_".join(generic_rule_desc_short.split("_")[2:]) + "_"
    + sanitized_field
)
```

### IDENTITY column INSERT
`generic_rule_id`, `dq_val_request_id`, and `dq_run_id` are `GENERATED ALWAYS AS IDENTITY`.
Never include them in INSERT column lists - Delta assigns them automatically.

### Building DQ_RESULT string
```python
segments = []
for m in mappings:
    tmp_col = f"_DQ_{m.rule_mapping_id}"
    label   = f"RULE_{m.rule_mapping_id}"
    segment = F.when(
        F.col(tmp_col).isNull(), F.lit(f"{label}: N")
    ).otherwise(
        F.concat(F.lit(f"{label}: "), F.col(tmp_col).cast("string"))
    )
    segments.append(segment)

enriched_df = enriched_df.withColumn("DQ_RESULT", F.concat_ws(" | ", *segments))
for m in mappings:
    enriched_df = enriched_df.drop(f"_DQ_{m.rule_mapping_id}")
```

---

## Setup Workflow (Run Once Per New Table)

Read `references/setup.md` for full CREATE TABLE DDL and seed data.

1. Create schema `dbdemos.dbdemos_dq_professional`
2. CREATE TABLE `dq_rules_generic` (IDENTITY column)
3. Seed generic rules - INSERT for 1st entity; MERGE for 2nd+ (see below)
4. CREATE TABLE `dq_validation_request` (IDENTITY column)
5. Seed validation request - INSERT for 1st entity; MERGE for 2nd+
6. CREATE TABLE `dq_rule_mappings` (`rule_mapping_id` is manually assigned, NOT IDENTITY)
7. CREATE TABLE `dq_results` (IDENTITY column) and `dq_run_results`
8. Copy source table into DQ schema
9. Seed rule mappings for the new entity
10. Run notebook 01 -> writes `{entity}_dq` + appends to `dq_run_results`
11. Run notebook 02 -> appends to `dq_results`

---

## Multi-Entity MERGE Pattern (2nd+ Entities)

The registry tables (`dq_rules_generic`, `dq_validation_request`) are **shared** across all entities.
Adding a 2nd entity with plain INSERT duplicates rows already seeded by entity 1.
Use MERGE to insert only new rows:

```python
# 00_setup.py for 2nd+ entity
new_rules_df.createOrReplaceTempView("_dq_rules_staging")
spark.sql("""
    MERGE INTO dbdemos.dbdemos_dq_professional.dq_rules_generic AS tgt
    USING _dq_rules_staging AS src
      ON tgt.generic_rule_desc_short = src.generic_rule_desc_short
    WHEN NOT MATCHED THEN INSERT
        (generic_rule_desc_short, rule_description, dq_category, rule_detail,
         rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
         active, created_date, created_by)
        VALUES
        (src.generic_rule_desc_short, src.rule_description, src.dq_category, src.rule_detail,
         src.rule_logic_template, src.applicable_types, src.bcbs239_principle, src.solvencyii_pillar,
         src.active, src.created_date, src.created_by)
""")

vr_df.createOrReplaceTempView("_dq_vr_staging")
spark.sql("""
    MERGE INTO dbdemos.dbdemos_dq_professional.dq_validation_request AS tgt
    USING _dq_vr_staging AS src
      ON tgt.dq_validation_request_short = src.dq_validation_request_short
    WHEN NOT MATCHED THEN INSERT
        (dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by)
        VALUES
        (src.dq_validation_request_short, src.owner_dept, src.owner_user,
         src.active, src.created_date, src.created_by)
""")
```

Dashboard notebook (03) must filter `dq_results` by `source_table` to isolate per-entity results:
```python
# In 03_dq_dashboard.py join chain:
.filter(F.col("m.source_table") == target_table)
```

---

## Adding a New Generic Rule

1. INSERT into `dq_rules_generic` (`generic_rule_id` auto-assigned)
2. Add an `elif` branch in `compute_rule_result()` for the new `generic_rule_desc_short`
3. INSERT row(s) into `dq_rule_mappings` for each table/field it applies to

```python
# Step 1
spark.sql("""
    INSERT INTO dbdemos.dbdemos_dq_professional.dq_rules_generic
        (generic_rule_desc_short, rule_description, dq_category, rule_detail,
         rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
         active, created_date, created_by)
    VALUES ('RULE_50_REVENUE_WITHIN_RANGE', 'Revenue Within Plausible Range', 'ACCURACY',
            'Revenue must be between 0 and 1,000,000 per day.',
            'CASE WHEN {field} BETWEEN 0 AND 1000000 THEN 1 ELSE 0 END',
            'DOUBLE,DECIMAL,FLOAT', 'P3', 'Pillar1', 1, current_date(), 'dq-framework')
""")

# Step 2 - add to dispatcher:
# elif generic_rule_desc_short == "RULE_50_REVENUE_WITHIN_RANGE":
#     result_col = F.when(
#         F.col(field_name).between(0, 1_000_000), F.lit(1)
#     ).otherwise(F.lit(0))
```

---

## Known Pitfalls

- **RULE_30 window function** - always add `tmp_col` to `df` BEFORE dropping `_rn_tmp`. The early-return pattern in the dispatcher handles this correctly.
- **regexp_extract_all** - requires a capturing group `()`; use `filter+split` for DQ_RESULT parsing instead (see above).
- **IDENTITY columns in INSERT** - never list `generic_rule_id`, `dq_val_request_id`, or `dq_run_id` in INSERT column lists.
- **Multi-entity INSERT** - 1st entity uses INSERT; 2nd+ entities MUST use MERGE to avoid duplicates in shared registry tables.
- **Notebook markdown** - never use em dashes (`-`) in cell titles; they render as `a?-` due to encoding. Use a plain hyphen `-`.
- **Pipe fields in dq_rule_no** - always sanitize `|` to `_vs_` before computing `dq_rule_no`.
- **Mapping tuple indices** - in seed scripts, `active` is hardcoded in the `Row()` constructor; do not include it in the tuple. Tuple order: `(rule_mapping_id, val_req_id, generic_rule_desc_short, catalog, schema, table, field, description, threshold_pct, dq_category, owner_dept, owner_user)`.

---

## Best Practices

- **Never delete breach records** - `dq_results` and `dq_run_results` are append-only
- **`rule_mapping_id` is immutable** once created - it is embedded as the label in DQ_RESULT strings
- **Threshold 100.0** for mandatory fields; 99.5 for enrichment/optional fields
- **One enrichment notebook per table** - parametrise with `target_table` widget
- **Job naming convention** - `DQ_<entity_name>` only (e.g. `DQ_fund_trans_gold`, `DQ_customer_gold`)

---

## Integration with Other Skills

| Skill | Role |
|-------|------|
| `databricks-unity-catalog` | Create `dbdemos_dq_professional` schema |
| `databricks-testing` | Test individual rule logic on cluster before pipeline deployment |
| `databricks-bundle-deploy` | Package and schedule the DQ pipeline as a DAB |

---

## Reference Files

- **`references/setup.md`** - All CREATE TABLE DDL, full generic-rules seed data (including RULE_40/41), and example rule mappings. Read when setting up a new schema or seeding data.
- **`references/notebooks.md`** - Complete notebook templates for `01_apply_dq_rules.py`, `02_aggregate_dq_results.py`, and `03_dq_dashboard.py`. Read when generating notebooks for a new entity.
