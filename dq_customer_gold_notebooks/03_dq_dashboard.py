# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Dashboard - customer_gold
# MAGIC
# MAGIC Overall DQ score and per-category breakdown.
# MAGIC Effective category = mapping dq_category override if set,
# MAGIC otherwise inherited from the generic rule (COALESCE pattern).

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "databricks_snippets_7405610928938750"

try:
    dq_schema = dbutils.widgets.get("dq_schema")
except:
    dq_schema = "dbdemos_dq_business_arausch"

from pyspark.sql import functions as F

results_table  = f"{catalog}.{dq_schema}.dq_results"
mappings_table = f"{catalog}.{dq_schema}.dq_rule_mappings"
generic_table  = f"{catalog}.{dq_schema}.dq_rules_generic"

# COMMAND ----------

# MAGIC %md ## Overall Summary

# COMMAND ----------

print("=" * 70)
print("  DQ DASHBOARD - Overall Summary - customer_gold")
print("=" * 70)

spark.sql(f"""
    SELECT
        COUNT(DISTINCT r.dq_rule_no)                                AS TOTAL_RULES,
        COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records > 0
              THEN r.dq_rule_no END)                                AS RULES_APPLIED,
        COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records = 0
              THEN r.dq_rule_no END)                                AS RULES_SKIPPED,
        SUM(r.dq_number_relevant_records)                           AS TOTAL_RELEVANT_RECORDS,
        SUM(r.dq_number_pos_results)                                AS TOTAL_POS_RESULTS,
        SUM(r.dq_number_relevant_records - r.dq_number_pos_results) AS TOTAL_FAILED,
        CASE WHEN SUM(r.dq_number_relevant_records) = 0 THEN NULL
             ELSE ROUND(SUM(r.dq_number_pos_results) * 1.0
                  / SUM(r.dq_number_relevant_records) * 100, 2)
        END                                                          AS OVERALL_PASS_PCT
    FROM {results_table} r
""").show(1, truncate=False)

# COMMAND ----------

# MAGIC %md ## Per-Category Breakdown

# COMMAND ----------

print("=" * 70)
print("  DQ DASHBOARD - Per Category")
print("=" * 70)

spark.sql(f"""
    SELECT
        COALESCE(m.dq_category, g.dq_category)     AS DQ_CATEGORY,
        COUNT(DISTINCT r.dq_rule_no)                AS TOTAL_RULES,
        COUNT(DISTINCT CASE WHEN r.dq_number_relevant_records > 0
              THEN r.dq_rule_no END)                AS RULES_APPLIED,
        SUM(r.dq_number_relevant_records)           AS TOTAL_RELEVANT_RECORDS,
        SUM(r.dq_number_pos_results)                AS TOTAL_POS_RESULTS,
        SUM(r.dq_number_relevant_records
            - r.dq_number_pos_results)              AS TOTAL_FAILED,
        CASE WHEN SUM(r.dq_number_relevant_records) = 0 THEN NULL
             ELSE ROUND(SUM(r.dq_number_pos_results) * 1.0
                  / SUM(r.dq_number_relevant_records) * 100, 2)
        END                                          AS PASS_PCT
    FROM {results_table} r
    JOIN {mappings_table} m
      ON r.dq_rule_no = m.dq_rule_no
    JOIN {generic_table} g
      ON m.generic_rule_desc_short = g.generic_rule_desc_short
    GROUP BY
        COALESCE(m.dq_category, g.dq_category)
    ORDER BY DQ_CATEGORY
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md ## Per-Rule Detail

# COMMAND ----------

print("=" * 70)
print("  DQ DASHBOARD - Per Rule Detail")
print("=" * 70)

spark.sql(f"""
    SELECT
        r.dq_run_id,
        r.dq_rule_no,
        COALESCE(m.dq_category, g.dq_category)     AS DQ_CATEGORY,
        m.source_field,
        g.generic_rule_desc_short,
        r.dq_number_relevant_records,
        r.dq_number_pos_results,
        r.dq_number_relevant_records
            - r.dq_number_pos_results               AS FAILED,
        CASE WHEN r.dq_number_relevant_records = 0 THEN NULL
             ELSE ROUND(r.dq_number_pos_results * 1.0
                  / r.dq_number_relevant_records * 100, 2)
        END                                          AS PASS_PCT,
        CASE WHEN r.dq_number_relevant_records = 0 THEN 'SKIPPED'
             WHEN r.dq_number_pos_results
                = r.dq_number_relevant_records      THEN 'PASS'
             ELSE 'BREACH'
        END                                          AS STATUS,
        m.threshold_pct,
        m.owner_dept,
        m.owner_user
    FROM {results_table} r
    JOIN {mappings_table} m
      ON r.dq_rule_no = m.dq_rule_no
    JOIN {generic_table} g
      ON m.generic_rule_desc_short = g.generic_rule_desc_short
    ORDER BY DQ_CATEGORY, r.dq_rule_no
""").show(50, truncate=False)

# COMMAND ----------

# MAGIC %md ## Breach Register

# COMMAND ----------

print("=" * 70)
print("  BREACH REGISTER - Rules below threshold")
print("=" * 70)

spark.sql(f"""
    SELECT
        r.dq_rule_no,
        COALESCE(m.dq_category, g.dq_category)     AS DQ_CATEGORY,
        m.source_field,
        g.generic_rule_desc_short,
        m.rule_description,
        r.dq_number_relevant_records,
        r.dq_number_pos_results,
        r.dq_number_relevant_records
            - r.dq_number_pos_results               AS FAILED_RECORDS,
        ROUND(r.dq_number_pos_results * 1.0
              / NULLIF(r.dq_number_relevant_records, 0) * 100, 2) AS PASS_PCT,
        m.threshold_pct                              AS REQUIRED_PCT,
        m.owner_dept,
        m.owner_user,
        r.dq_execution_timestamp
    FROM {results_table} r
    JOIN {mappings_table} m ON r.dq_rule_no = m.dq_rule_no
    JOIN {generic_table}  g ON m.generic_rule_desc_short = g.generic_rule_desc_short
    WHERE r.dq_number_relevant_records > 0
      AND ROUND(r.dq_number_pos_results * 1.0
                / r.dq_number_relevant_records * 100, 2) < m.threshold_pct
    ORDER BY PASS_PCT ASC
""").show(50, truncate=False)
