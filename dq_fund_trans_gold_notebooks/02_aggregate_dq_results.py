# Databricks notebook source

# MAGIC %md

# MAGIC # DQ Aggregation - Populate dq_results Table

# MAGIC

# MAGIC Reads the enriched fund_trans_gold_dq table, aggregates RELEVANT and positive

# MAGIC result counts per rule via regex on DQ_RESULT, and appends one row per rule

# MAGIC to dq_results. dq_run_id is assigned automatically by Delta's IDENTITY column.

# MAGIC dq_rule_no identifies the rule (e.g. DQ_212_TEMPORAL_WINDOW_CONSISTENCY_sent_txn_cnt_3m_vs_sent_txn_cnt_12m).



# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "databricks_snippets_7405610928938750"

try:
    dq_schema = dbutils.widgets.get("dq_schema")
except:
    dq_schema = "dbdemos_dq_business_arausch"

try:
    target_table = dbutils.widgets.get("target_table")
except:
    target_table = "fund_trans_gold"

from datetime import datetime
from pyspark.sql import functions as F

execution_ts  = datetime.utcnow()
dq_table      = f"{catalog}.{dq_schema}.{target_table}_dq"
results_table = f"{catalog}.{dq_schema}.dq_results"

print(f"Aggregation Parameters:")
print(f"  Source DQ table:  {dq_table}")
print(f"  Results table:    {results_table}")
print(f"  Execution time:   {execution_ts}")

# COMMAND ----------

# Load enriched DQ table
dq_df = spark.table(dq_table)
total_rows = dq_df.count()
print(f"\nLoaded {total_rows} rows from {dq_table}")

# Load active mappings to know which rule_mapping_ids exist
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

# Aggregate per rule via regex on DQ_RESULT
# "RULE_{no}: [01]" matches applicable checks; "RULE_{no}: 1" matches passes

agg_rows = []

for m in mappings:
    rule_mapping_id = m.rule_mapping_id
    label   = f"RULE_{rule_mapping_id}"

    if "DQ_RESULT" not in dq_df.columns:
        print(f"  [WARN] DQ_RESULT column not found in {dq_table} - skipping")
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
    print(f"  {m.dq_rule_no}: relevant={relevant}, passed={positive}, score={pct}%")

# COMMAND ----------

# Insert into dq_results (dq_run_id assigned by Delta IDENTITY - never include in INSERT)
if agg_rows:
    from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

    schema = StructType([
        StructField("dq_execution_timestamp",     TimestampType(), False),
        StructField("dq_rule_no",                 StringType(),    False),
        StructField("dq_number_relevant_records", LongType(),      False),
        StructField("dq_number_pos_results",      LongType(),      False),
    ])

    insert_rows = [(execution_ts, r[0], r[1], r[2]) for r in agg_rows]
    insert_df = spark.createDataFrame(insert_rows, schema)
    insert_df.createOrReplaceTempView("_dq_agg_staging")

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
    print("dq_run_id assigned by Delta IDENTITY. dq_rule_no from dq_rule_mappings.")

# COMMAND ----------

# Confirm latest inserted rows
print("\nLatest entries in dq_results:")
spark.sql(f"""
    SELECT dq_run_id, dq_execution_timestamp, dq_rule_no,
           dq_number_relevant_records, dq_number_pos_results,
           ROUND(dq_number_pos_results * 1.0 /
                 NULLIF(dq_number_relevant_records, 0) * 100, 2) AS pass_pct
    FROM {results_table}
    ORDER BY dq_run_id DESC
    LIMIT 30
""").show(30, truncate=False)
