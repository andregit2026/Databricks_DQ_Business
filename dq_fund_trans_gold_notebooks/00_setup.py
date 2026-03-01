# Databricks notebook source

# MAGIC %md

# MAGIC # DQ Framework Setup - fund_trans_gold

# MAGIC

# MAGIC Idempotent setup: creates DQ registry tables if they do not exist, merges two new

# MAGIC generic rules (RULE_40, RULE_41) and a new validation request into the shared registry,

# MAGIC copies fund_trans_gold into the DQ schema, and merges 18 rule mappings (IDs 201-218).



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
    source_catalog = dbutils.widgets.get("source_catalog")
except:
    source_catalog = "databricks_snippets_7405610928938750"

try:
    source_schema = dbutils.widgets.get("source_schema")
except:
    source_schema = "dbdemos_fsi_credit"

try:
    target_table = dbutils.widgets.get("target_table")
except:
    target_table = "fund_trans_gold"

print(f"Setup Parameters:")
print(f"  DQ Catalog:     {catalog}")
print(f"  DQ Schema:      {dq_schema}")
print(f"  Source Catalog: {source_catalog}")
print(f"  Source Schema:  {source_schema}")
print(f"  Target Table:   {target_table}")

# COMMAND ----------

from pyspark.sql import Row, functions as F
from datetime import date

# COMMAND ----------

# MAGIC %md ## Step 1 - Create dq_rules_generic (if not exists)

# COMMAND ----------

# NOTE: CREATE TABLE uses single-line SQL - multi-line f-strings with blank lines
# inside the SQL cause spark.sql() to fail silently in Databricks serverless.
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_rules_generic (generic_rule_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), generic_rule_desc_short STRING NOT NULL, rule_description STRING, dq_category STRING, rule_detail STRING, rule_logic_template STRING, applicable_types STRING, bcbs239_principle STRING, solvencyii_pillar STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'Generic DQ rule catalogue. generic_rule_id auto-increments - never set manually.'")
print("dq_rules_generic ready (created if not existed)")

# COMMAND ----------

# MAGIC %md ## Step 2 - Merge new generic rules RULE_40 and RULE_41

# COMMAND ----------

# RULE_40 and RULE_41 are new rules introduced for fund_trans_gold.
# MERGE ensures they are added only once even if this notebook is re-run.
new_generic_rules = [
    # -- CONSISTENCY ---------------------------------------------------------
    ("RULE_40_TEMPORAL_WINDOW_CONSISTENCY", "Temporal Window Consistency", "CONSISTENCY",
     "Checks that a 3-month metric does not exceed the 12-month metric for the same field. "
     "If the 3m value is greater than the 12m value the data has an aggregation bug. "
     "field_name is pipe-delimited: {field_3m}|{field_12m}.",
     "CASE WHEN {field_3m} <= {field_12m} THEN 1 ELSE 0 END",
     "LONG,DOUBLE", "P3", "Pillar3"),

    # -- ACCURACY (conditional) ----------------------------------------------
    ("RULE_41_POSITIVE_IF_COUNT_NONZERO", "Positive Amount if Count Non-Zero", "ACCURACY",
     "Checks that an amount field is > 0 when the corresponding transaction count is > 0. "
     "A zero amount with a non-zero count indicates a data integrity failure. "
     "field_name is pipe-delimited: {count_field}|{amount_field}.",
     "CASE WHEN {count_field} = 0 OR {amount_field} > 0 THEN 1 ELSE 0 END",
     "LONG,DOUBLE", "P2", "Pillar1"),
]

new_rule_rows = [Row(
    generic_rule_desc_short=r[0], rule_description=r[1], dq_category=r[2],
    rule_detail=r[3], rule_logic_template=r[4], applicable_types=r[5],
    bcbs239_principle=r[6], solvencyii_pillar=r[7],
    active=1, created_date=date.today(), created_by="dq-framework"
) for r in new_generic_rules]

spark.createDataFrame(new_rule_rows).createOrReplaceTempView("_new_rules_staging")

spark.sql(f"""
    MERGE INTO {catalog}.{dq_schema}.dq_rules_generic AS target
    USING _new_rules_staging AS source
    ON target.generic_rule_desc_short = source.generic_rule_desc_short
    WHEN NOT MATCHED THEN INSERT (
        generic_rule_desc_short, rule_description, dq_category, rule_detail,
        rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
        active, created_date, created_by
    ) VALUES (
        source.generic_rule_desc_short, source.rule_description, source.dq_category,
        source.rule_detail, source.rule_logic_template, source.applicable_types,
        source.bcbs239_principle, source.solvencyii_pillar, source.active,
        source.created_date, source.created_by
    )
""")
print("Merged RULE_40 and RULE_41 into dq_rules_generic.")

spark.table(f"{catalog}.{dq_schema}.dq_rules_generic") \
    .select("generic_rule_id", "generic_rule_desc_short", "dq_category") \
    .show(25, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 3 - Create dq_validation_request (if not exists)

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_validation_request (dq_val_request_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), dq_validation_request_short STRING NOT NULL, owner_dept STRING, owner_user STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'DQ validation request registry. dq_val_request_id auto-increments - never set manually.'")
print("dq_validation_request ready (created if not existed)")

vr_rows = [Row(
    dq_validation_request_short="DQ_VAL_REQUEST_2_fund_trans_gold",
    owner_dept="Dept. Transaction Data", owner_user="Mr. Alex Smith",
    active=1, created_date=date.today(), created_by="dq-framework"
)]
spark.createDataFrame(vr_rows).createOrReplaceTempView("_dq_vr_staging")

spark.sql(f"""
    MERGE INTO {catalog}.{dq_schema}.dq_validation_request AS target
    USING _dq_vr_staging AS source
    ON target.dq_validation_request_short = source.dq_validation_request_short
    WHEN NOT MATCHED THEN INSERT (
        dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by
    ) VALUES (
        source.dq_validation_request_short, source.owner_dept, source.owner_user,
        source.active, source.created_date, source.created_by
    )
""")
print("Merged DQ_VAL_REQUEST_2_fund_trans_gold into dq_validation_request.")

spark.table(f"{catalog}.{dq_schema}.dq_validation_request").show(truncate=False)

# Lookup actual assigned dq_val_request_id for use in mappings below
vr_id_row = (
    spark.table(f"{catalog}.{dq_schema}.dq_validation_request")
    .filter(F.col("dq_validation_request_short") == "DQ_VAL_REQUEST_2_fund_trans_gold")
    .select("dq_val_request_id")
    .collect()
)
dq_vr_id = int(vr_id_row[0]["dq_val_request_id"]) if vr_id_row else 2
print(f"fund_trans_gold dq_val_request_id = {dq_vr_id}")

# COMMAND ----------

# MAGIC %md ## Step 4 - Copy source table into DQ schema

# COMMAND ----------

source_full = f"{source_catalog}.{source_schema}.{target_table}"
target_full = f"{catalog}.{dq_schema}.{target_table}"

if spark.catalog.tableExists(target_full):
    count = spark.table(target_full).count()
    print(f"Table {target_full} already exists with {count} rows - skipping copy.")
else:
    df = spark.table(source_full)
    df.write.format("delta").saveAsTable(target_full)
    count = spark.table(target_full).count()
    print(f"Copied {count} rows from {source_full} to {target_full}")
    print(f"Columns ({len(df.columns)}): {df.columns}")

# COMMAND ----------

# MAGIC %md ## Step 5 - Create dq_rule_mappings (if not exists) and merge 18 mappings

# COMMAND ----------

# Create table if it does not exist (handles the case where customer_gold setup has not run yet)
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_rule_mappings (rule_mapping_id BIGINT, dq_rule_no STRING NOT NULL, dq_val_request_id BIGINT, generic_rule_desc_short STRING NOT NULL, source_catalog STRING, source_schema STRING, source_table STRING, source_field STRING, rule_description STRING, threshold_pct DOUBLE, dq_category STRING, owner_dept STRING, owner_user STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'DQ rule-to-field mappings. rule_mapping_id is set explicitly (not auto-assigned).'")

# 18 mappings for fund_trans_gold:
#   201       UNIQUENESS  - cust_id unique
#   202-205   COMPLETENESS - core 12m metrics not null
#   206-211   ACCURACY     - amount fields >= 0 across all windows
#   212-215   CONSISTENCY  - RULE_40 temporal window consistency (3m <= 12m)
#   216-218   ACCURACY     - RULE_41 positive amount if count non-zero
#
# field_name convention for two-column rules: "col_a|col_b" (pipe-delimited)

raw_mappings = [
    # -- UNIQUENESS --
    (201, dq_vr_id, "RULE_30_UNIQUE_IDENTIFIER",  catalog, dq_schema, target_table, "cust_id",
     "cust_id is the primary key - must be unique across all fund_trans_gold records",
     100.0, "UNIQUENESS",    "Dept. Transaction Data", "Mr. Alex Smith"),

    # -- COMPLETENESS - core 12m metrics must not be NULL --
    (202, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",     catalog, dq_schema, target_table, "sent_txn_cnt_12m",
     "sent_txn_cnt_12m must not be NULL - core sent volume metric (12m)",
     100.0, "COMPLETENESS",  "Dept. Transaction Data", "Mr. Alex Smith"),
    (203, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",     catalog, dq_schema, target_table, "sent_txn_amt_12m",
     "sent_txn_amt_12m must not be NULL - core sent amount metric (12m)",
     100.0, "COMPLETENESS",  "Dept. Transaction Data", "Mr. Alex Smith"),
    (204, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",     catalog, dq_schema, target_table, "rcvd_txn_cnt_12m",
     "rcvd_txn_cnt_12m must not be NULL - core received volume metric (12m)",
     100.0, "COMPLETENESS",  "Dept. Transaction Data", "Mr. Alex Smith"),
    (205, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",     catalog, dq_schema, target_table, "rcvd_txn_amt_12m",
     "rcvd_txn_amt_12m must not be NULL - core received amount metric (12m)",
     100.0, "COMPLETENESS",  "Dept. Transaction Data", "Mr. Alex Smith"),

    # -- ACCURACY - amount fields must be >= 0 (all three windows) --
    (206, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "sent_txn_amt_12m",
     "sent_txn_amt_12m must be >= 0 - transaction amount cannot be negative",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (207, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "rcvd_txn_amt_12m",
     "rcvd_txn_amt_12m must be >= 0 - transaction amount cannot be negative",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (208, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "sent_txn_amt_6m",
     "sent_txn_amt_6m must be >= 0 - transaction amount cannot be negative",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (209, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "rcvd_txn_amt_6m",
     "rcvd_txn_amt_6m must be >= 0 - transaction amount cannot be negative",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (210, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "sent_txn_amt_3m",
     "sent_txn_amt_3m must be >= 0 - transaction amount cannot be negative",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (211, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "rcvd_txn_amt_3m",
     "rcvd_txn_amt_3m must be >= 0 - transaction amount cannot be negative",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),

    # -- CONSISTENCY - RULE_40: 3m value must not exceed 12m value --
    # field_name is pipe-delimited: {field_3m}|{field_12m}
    (212, dq_vr_id, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY", catalog, dq_schema, target_table,
     "sent_txn_cnt_3m|sent_txn_cnt_12m",
     "sent_txn_cnt_3m must be <= sent_txn_cnt_12m - 3m window cannot exceed 12m window",
     100.0, "CONSISTENCY",   "Dept. Transaction Data", "Mr. Alex Smith"),
    (213, dq_vr_id, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY", catalog, dq_schema, target_table,
     "rcvd_txn_cnt_3m|rcvd_txn_cnt_12m",
     "rcvd_txn_cnt_3m must be <= rcvd_txn_cnt_12m - 3m window cannot exceed 12m window",
     100.0, "CONSISTENCY",   "Dept. Transaction Data", "Mr. Alex Smith"),
    (214, dq_vr_id, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY", catalog, dq_schema, target_table,
     "sent_txn_amt_3m|sent_txn_amt_12m",
     "sent_txn_amt_3m must be <= sent_txn_amt_12m - 3m window cannot exceed 12m window",
     100.0, "CONSISTENCY",   "Dept. Transaction Data", "Mr. Alex Smith"),
    (215, dq_vr_id, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY", catalog, dq_schema, target_table,
     "rcvd_txn_amt_3m|rcvd_txn_amt_12m",
     "rcvd_txn_amt_3m must be <= rcvd_txn_amt_12m - 3m window cannot exceed 12m window",
     100.0, "CONSISTENCY",   "Dept. Transaction Data", "Mr. Alex Smith"),

    # -- ACCURACY - RULE_41: amount must be > 0 when count is non-zero --
    # field_name is pipe-delimited: {count_field}|{amount_field}
    (216, dq_vr_id, "RULE_41_POSITIVE_IF_COUNT_NONZERO", catalog, dq_schema, target_table,
     "sent_txn_cnt_12m|sent_txn_amt_12m",
     "sent_txn_amt_12m must be > 0 when sent_txn_cnt_12m > 0 - zero amount with non-zero count is a data integrity failure",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (217, dq_vr_id, "RULE_41_POSITIVE_IF_COUNT_NONZERO", catalog, dq_schema, target_table,
     "rcvd_txn_cnt_12m|rcvd_txn_amt_12m",
     "rcvd_txn_amt_12m must be > 0 when rcvd_txn_cnt_12m > 0 - zero amount with non-zero count is a data integrity failure",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
    (218, dq_vr_id, "RULE_41_POSITIVE_IF_COUNT_NONZERO", catalog, dq_schema, target_table,
     "sent_txn_cnt_3m|sent_txn_amt_3m",
     "sent_txn_amt_3m must be > 0 when sent_txn_cnt_3m > 0 - shortest window is most sensitive to integrity failures",
     100.0, "ACCURACY",      "Dept. Transaction Data", "Mr. Alex Smith"),
]

mapping_rows = []
for m in raw_mappings:
    rule_mapping_id       = m[0]
    generic_rule          = m[2]
    field_name            = m[6]
    safe_field            = field_name.replace("|", "_vs_")
    rule_short            = "_".join(generic_rule.split("_")[2:])
    dq_rule_no            = f"DQ_{rule_mapping_id}_{rule_short}_{safe_field}"
    mapping_rows.append(Row(
        rule_mapping_id=rule_mapping_id,
        dq_rule_no=dq_rule_no,
        dq_val_request_id=m[1],
        generic_rule_desc_short=m[2],
        source_catalog=m[3], source_schema=m[4], source_table=m[5], source_field=m[6],
        rule_description=m[7], threshold_pct=m[8],
        dq_category=m[9], owner_dept=m[10], owner_user=m[11],
        active=1, created_date=date.today(), created_by="dq-framework"
    ))

spark.createDataFrame(mapping_rows).createOrReplaceTempView("_dq_mappings_staging")

spark.sql(f"""
    MERGE INTO {catalog}.{dq_schema}.dq_rule_mappings AS target
    USING _dq_mappings_staging AS source
    ON target.rule_mapping_id = source.rule_mapping_id
    WHEN NOT MATCHED THEN INSERT (
        rule_mapping_id, dq_rule_no, dq_val_request_id, generic_rule_desc_short,
        source_catalog, source_schema, source_table, source_field,
        rule_description, threshold_pct, dq_category, owner_dept, owner_user,
        active, created_date, created_by
    ) VALUES (
        source.rule_mapping_id, source.dq_rule_no, source.dq_val_request_id, source.generic_rule_desc_short,
        source.source_catalog, source.source_schema, source.source_table, source.source_field,
        source.rule_description, source.threshold_pct, source.dq_category, source.owner_dept, source.owner_user,
        source.active, source.created_date, source.created_by
    )
""")
print(f"Merged {len(mapping_rows)} rule mappings for {target_table} (IDs 201-218).")

spark.table(f"{catalog}.{dq_schema}.dq_rule_mappings") \
    .filter(F.col("source_table") == target_table) \
    .select("rule_mapping_id", "dq_rule_no", "generic_rule_desc_short", "source_field", "dq_category") \
    .orderBy("rule_mapping_id") \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 6 - Create dq_results table (if not exists)

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_results (dq_run_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), dq_execution_timestamp TIMESTAMP NOT NULL, dq_rule_no STRING NOT NULL, dq_number_relevant_records BIGINT NOT NULL, dq_number_pos_results BIGINT NOT NULL) USING DELTA COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'")
print("Table dq_results ready.")

# COMMAND ----------

print("\nSetup complete. All tables in DQ schema:")
spark.sql(f"SHOW TABLES IN {catalog}.{dq_schema}").show(truncate=False)
