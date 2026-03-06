# DQ Framework - Notebook Templates

Complete copy-paste-ready notebooks for a new entity.
Substitute `{entity}` with the actual table name (e.g. `bikes`, `fund_trans_gold`).

The dispatcher function (`compute_rule_result`) lives in SKILL.md - paste it into notebook 01.

---

## 01_apply_dq_rules.py

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
    target_table = dbutils.widgets.get("target_table")
except:
    target_table = "bikes"

try:
    report_date = dbutils.widgets.get("report_date")
except:
    from datetime import date
    report_date = str(date.today())

print("DQ Enrichment Parameters:")
print(f"  Catalog:       {catalog}")
print(f"  DQ Schema:     {dq_schema}")
print(f"  Target table:  {target_table}")
print(f"  Report date:   {report_date}")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Paste compute_rule_result() dispatcher here (from SKILL.md)

# COMMAND ----------
# -- Load source table -------------------------------------------------------
full_source = f"{catalog}.{dq_schema}.{target_table}"
df = spark.table(full_source)

# Status filter: only evaluate active records when a status column exists.
# Tables without a status column (e.g. fund_trans_gold) skip this filter.
if "status" in df.columns:
    df = df.filter(F.col("status").isin("active", "ACTIVE"))

total_rows = df.count()
print(f"\nLoaded {total_rows} rows from {full_source}")
print(f"Columns: {df.columns}")

# COMMAND ----------
# -- Load active rule mappings for this table --------------------------------
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
    print(f"  RULE_{m.rule_mapping_id}: {m.generic_rule_desc_short} -> {m.source_field}")

# COMMAND ----------
# -- Apply all mapped rules (builds one temp column per rule) ----------------
print("\nApplying rules...")
enriched_df = df

for m in mappings:
    enriched_df = compute_rule_result(
        df                      = enriched_df,
        rule_mapping_id         = m.rule_mapping_id,
        generic_rule_desc_short = m.generic_rule_desc_short,
        field_name              = m.source_field,
    )
    print(f"  RULE_{m.rule_mapping_id} ({m.generic_rule_desc_short} -> {m.source_field}): applied")

# COMMAND ----------
# -- Build single DQ_RESULT column from all temp columns --------------------
# Format: "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"
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

# Drop all temporary rule columns
for m in mappings:
    enriched_df = enriched_df.drop(f"_DQ_{m.rule_mapping_id}")

# COMMAND ----------
# -- Write enriched DQ table ------------------------------------------------
output_table = f"{catalog}.{dq_schema}.{target_table}_dq"

enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

row_count = spark.table(output_table).count()
print(f"\nWritten {row_count} rows to {output_table}")
print(f"Columns: {len(df.columns)} original + 1 DQ_RESULT = {len(enriched_df.columns)} total")

# COMMAND ----------
# -- DQ Summary Scorecard ---------------------------------------------------
# Counts applicable checks (': [01]$') and passed checks (': 1$') per rule.
# Uses filter+split — NOT regexp_extract_all() which requires a capturing group.
print(f"\n{'='*65}")
print(f"DQ SCORECARD - {target_table} - {report_date}")
print(f"{'='*65}")
print(f"{'RULE_NO':<10} {'FIELD':<25} {'RELEVANT':<10} {'PASS%':<10} STATUS")
print(f"{'-'*65}")

import uuid
from datetime import datetime

run_id  = str(uuid.uuid4())
run_ts  = datetime.utcnow()
results = []

result_df = spark.table(output_table)
total     = result_df.count()

for m in mappings:
    label = f"RULE_{m.rule_mapping_id}"

    agg = result_df.select(
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: [01]"), 1).otherwise(0)).alias("relevant"),
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: 1"),    1).otherwise(0)).alias("positive")
    ).collect()[0]

    relevant = int(agg["relevant"] or 0)
    passed   = int(agg["positive"] or 0)
    failed   = relevant - passed
    pct      = round(passed / relevant * 100, 2) if relevant > 0 else 0.0
    status   = "PASS" if pct >= m.threshold_pct else "BREACH"
    icon     = "v" if status == "PASS" else "x"

    print(f"{m.rule_mapping_id:<10} {m.source_field:<25} {relevant:<10} {pct:<10} {icon} {status}")

    results.append({
        "run_id": run_id, "run_timestamp": run_ts, "report_date": report_date,
        "source_table": f"{catalog}.{dq_schema}.{target_table}",
        "rule_mapping_id": m.rule_mapping_id,
        "generic_rule_desc_short": m.generic_rule_desc_short,
        "source_field": m.source_field, "rule_description": m.rule_description,
        "total_records": total, "relevant_records": relevant,
        "passed_records": passed, "failed_records": failed,
        "passed_pct": pct, "threshold_pct": m.threshold_pct, "status": status,
    })

print(f"{'='*65}")

# -- Overall DQ% across all rules ------------------------------------------
overall = spark.table(output_table).selectExpr(
    "SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))) AS total",
    "SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$')))    AS passed",
    "ROUND(SUM(size(filter(split(DQ_RESULT,' [|] '),x->x rlike ': 1$')))*1.0 / "
    "NULLIF(SUM(size(filter(split(DQ_RESULT,' [|] '),x->x rlike ': [01]$'))),0)*100,2) AS dq_percentage"
).collect()[0]

print(f"\nOverall DQ%: {overall['dq_percentage']}%  "
      f"({overall['passed']} passed / {overall['total']} applicable checks)")

# -- Persist scorecard to audit table --------------------------------------
if results:
    spark.createDataFrame(results).write \
        .format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{dq_schema}.dq_run_results")
    print(f"\nScorecard written to {catalog}.{dq_schema}.dq_run_results (run_id={run_id})")
```

---

## 02_aggregate_dq_results.py

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Aggregation - Populate dq_results Table
# MAGIC
# MAGIC Reads the enriched DQ table, aggregates RELEVANT and PASSED counts per rule,
# MAGIC and appends one row per rule to dq_results (dq_run_id auto-increments via IDENTITY).

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

execution_ts  = datetime.utcnow()
dq_table      = f"{catalog}.{dq_schema}.{target_table}_dq"
results_table = f"{catalog}.{dq_schema}.dq_results"

print("Aggregation Parameters:")
print(f"  Source DQ table:  {dq_table}")
print(f"  Results table:    {results_table}")
print(f"  Execution time:   {execution_ts}")

# COMMAND ----------
# -- Load enriched DQ table -------------------------------------------------
dq_df = spark.table(dq_table)
total_rows = dq_df.count()
print(f"\nLoaded {total_rows} rows from {dq_table}")

if "DQ_RESULT" not in dq_df.columns:
    raise ValueError(f"DQ_RESULT column not found in {dq_table}. Run notebook 01 first.")

# -- Load active mappings ---------------------------------------------------
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
# -- Aggregate per rule via regex on DQ_RESULT ------------------------------
# "RULE_{no}: [01]" matches applicable checks; "RULE_{no}: 1" matches passes.
agg_rows = []

for m in mappings:
    rule_mapping_id = m.rule_mapping_id
    label = f"RULE_{rule_mapping_id}"

    agg = dq_df.select(
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: [01]"), F.lit(1)).otherwise(F.lit(0)))
            .cast("bigint").alias("relevant"),
        F.sum(F.when(F.col("DQ_RESULT").rlike(f"{label}: 1"),    F.lit(1)).otherwise(F.lit(0)))
            .cast("bigint").alias("positive")
    ).collect()[0]

    relevant = int(agg["relevant"] or 0)
    positive = int(agg["positive"] or 0)
    agg_rows.append((m.dq_rule_no, relevant, positive))

    pct = round(positive / relevant * 100, 2) if relevant > 0 else 0.0
    print(f"  RULE_{rule_mapping_id}: relevant={relevant}, passed={positive}, score={pct}%")

# COMMAND ----------
# -- Insert into dq_results (dq_run_id assigned by Delta IDENTITY) ----------
if agg_rows:
    from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

    schema = StructType([
        StructField("dq_execution_timestamp",     TimestampType(), False),
        StructField("dq_rule_no",                 StringType(),    False),
        StructField("dq_number_relevant_records", LongType(),      False),
        StructField("dq_number_pos_results",      LongType(),      False),
    ])

    insert_rows = [(execution_ts, r[0], r[1], r[2]) for r in agg_rows]
    insert_df   = spark.createDataFrame(insert_rows, schema)
    insert_df.createOrReplaceTempView("_dq_agg_staging")

    # Never include dq_run_id - Delta IDENTITY assigns it automatically
    spark.sql(f"""
        INSERT INTO {results_table}
            (dq_execution_timestamp, dq_rule_no,
             dq_number_relevant_records, dq_number_pos_results)
        SELECT
            dq_execution_timestamp, dq_rule_no,
            dq_number_relevant_records, dq_number_pos_results
        FROM _dq_agg_staging
    """)

    print(f"\nInserted {len(agg_rows)} rows into {results_table}")
    print("dq_run_id values assigned automatically by Delta IDENTITY column.")

# COMMAND ----------
# -- Confirm inserted rows --------------------------------------------------
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

## 03_dq_dashboard.py

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Dashboard
# MAGIC
# MAGIC Overall DQ score and per-category breakdown for a specific entity.
# MAGIC Effective category = mapping override if set, else inherited from generic rule.
# MAGIC Filter by source_table to isolate per-entity results in shared dq_results table.

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

from pyspark.sql import functions as F

# COMMAND ----------
# -- Overall Summary --------------------------------------------------------
print("=" * 65)
print(f"  DQ DASHBOARD - Overall Summary - {target_table}")
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
        END                                                       AS OVERALL_PASS_PCT
    FROM {catalog}.{dq_schema}.dq_results r
    JOIN {catalog}.{dq_schema}.dq_rule_mappings m
      ON r.dq_rule_no = m.dq_rule_no
    WHERE m.source_table = '{target_table}'
""").show(1, truncate=False)

# COMMAND ----------
# -- Per-Category Breakdown -------------------------------------------------
print("=" * 65)
print(f"  DQ DASHBOARD - Per Category - {target_table}")
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
        END                                        AS PASS_PCT
    FROM {catalog}.{dq_schema}.dq_results r
    JOIN {catalog}.{dq_schema}.dq_rule_mappings m
      ON r.dq_rule_no = m.dq_rule_no
    JOIN {catalog}.{dq_schema}.dq_rules_generic g
      ON m.generic_rule_desc_short = g.generic_rule_desc_short
    WHERE m.source_table = '{target_table}'
    GROUP BY COALESCE(m.dq_category, g.dq_category)
    ORDER BY DQ_CATEGORY
""").show(20, truncate=False)

# COMMAND ----------
# -- Per-Rule Detail --------------------------------------------------------
print("=" * 65)
print(f"  DQ DASHBOARD - Per Rule Detail - {target_table}")
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
        END                                        AS PASS_PCT,
        CASE WHEN r.dq_number_relevant_records = 0    THEN 'SKIPPED'
             WHEN r.dq_number_pos_results
                = r.dq_number_relevant_records      THEN 'PASS'
             ELSE 'BREACH'
        END                                        AS STATUS
    FROM {catalog}.{dq_schema}.dq_results r
    JOIN {catalog}.{dq_schema}.dq_rule_mappings m
      ON r.dq_rule_no = m.dq_rule_no
    JOIN {catalog}.{dq_schema}.dq_rules_generic g
      ON m.generic_rule_desc_short = g.generic_rule_desc_short
    WHERE m.source_table = '{target_table}'
    ORDER BY DQ_CATEGORY, r.dq_rule_no
""").show(50, truncate=False)
```
