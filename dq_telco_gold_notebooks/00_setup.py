# Databricks notebook source

# MAGIC %md

# MAGIC # DQ Framework Setup - telco_gold

# MAGIC

# MAGIC Idempotent setup: creates DQ registry tables if they do not exist, merges a new

# MAGIC validation request into the shared registry, copies telco_gold into the DQ schema

# MAGIC (dropping the streaming artifact column _rescued_data), and merges 10 rule mappings

# MAGIC (IDs 301-310). All 10 rules use existing generic rules - no new rules needed.



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
    target_table = "telco_gold"

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

# No new generic rules needed for telco_gold - all 10 rules use existing rule types.
# Show current rule catalogue for confirmation.
spark.table(f"{catalog}.{dq_schema}.dq_rules_generic") \
    .select("generic_rule_id", "generic_rule_desc_short", "dq_category") \
    .show(25, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 2 - Create dq_validation_request (if not exists) and merge telco_gold request

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_validation_request (dq_val_request_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), dq_validation_request_short STRING NOT NULL, owner_dept STRING, owner_user STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'DQ validation request registry. dq_val_request_id auto-increments - never set manually.'")
print("dq_validation_request ready (created if not existed)")

vr_rows = [Row(
    dq_validation_request_short="DQ_VAL_REQUEST_3_telco_gold",
    owner_dept="Dept. Telco Risk", owner_user="Mrs. Elena Fischer",
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
print("Merged DQ_VAL_REQUEST_3_telco_gold into dq_validation_request.")

spark.table(f"{catalog}.{dq_schema}.dq_validation_request").show(truncate=False)

# Lookup actual assigned dq_val_request_id for use in mappings below
vr_id_row = (
    spark.table(f"{catalog}.{dq_schema}.dq_validation_request")
    .filter(F.col("dq_validation_request_short") == "DQ_VAL_REQUEST_3_telco_gold")
    .select("dq_val_request_id")
    .collect()
)
dq_vr_id = int(vr_id_row[0]["dq_val_request_id"]) if vr_id_row else 3
print(f"telco_gold dq_val_request_id = {dq_vr_id}")

# COMMAND ----------

# MAGIC %md ## Step 3 - Copy source table into DQ schema

# COMMAND ----------

# telco_gold contains a streaming artifact column _rescued_data that must be excluded.
# Drop it during the copy so DQ rules only operate on business fields.

source_full = f"{source_catalog}.{source_schema}.{target_table}"
target_full = f"{catalog}.{dq_schema}.{target_table}"

if spark.catalog.tableExists(target_full):
    count = spark.table(target_full).count()
    print(f"Table {target_full} already exists with {count} rows - skipping copy.")
else:
    df = spark.table(source_full).drop("_rescued_data")
    df.write.format("delta").saveAsTable(target_full)
    count = spark.table(target_full).count()
    print(f"Copied {count} rows from {source_full} to {target_full}")
    print(f"Columns ({len(df.columns)}): {df.columns}")

# COMMAND ----------

# MAGIC %md ## Step 4 - Create dq_rule_mappings (if not exists) and merge 10 mappings

# COMMAND ----------

# Create table if it does not exist
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_rule_mappings (rule_mapping_id BIGINT, dq_rule_no STRING NOT NULL, dq_val_request_id BIGINT, generic_rule_desc_short STRING NOT NULL, source_catalog STRING, source_schema STRING, source_table STRING, source_field STRING, rule_description STRING, threshold_pct DOUBLE, dq_category STRING, owner_dept STRING, owner_user STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'DQ rule-to-field mappings. rule_mapping_id is set explicitly (not auto-assigned).'")

# 10 mappings for telco_gold:
#   301       UNIQUENESS   - cust_id unique
#   302-305   COMPLETENESS - key numeric/string fields not null
#   306-309   ACCURACY     - amount/count fields non-negative or positive
#   310       VALIDITY     - is_pre_paid boolean flag

raw_mappings = [
    # -- UNIQUENESS --
    (301, dq_vr_id, "RULE_30_UNIQUE_IDENTIFIER",  catalog, dq_schema, target_table, "cust_id",
     "cust_id is the primary key - must be unique across all telco_gold records",
     100.0, "UNIQUENESS",    "Dept. Telco Risk", "Mrs. Elena Fischer"),

    # -- COMPLETENESS --
    (302, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",    catalog, dq_schema, target_table, "avg_phone_bill_amt_lst12mo",
     "avg_phone_bill_amt_lst12mo must not be NULL - core billing metric",
     100.0, "COMPLETENESS",  "Dept. Telco Risk", "Mrs. Elena Fischer"),
    (303, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",    catalog, dq_schema, target_table, "phone_bill_amt",
     "phone_bill_amt must not be NULL - current billing amount",
     100.0, "COMPLETENESS",  "Dept. Telco Risk", "Mrs. Elena Fischer"),
    (304, dq_vr_id, "RULE_3_NUMERIC_NOT_NULL",    catalog, dq_schema, target_table, "number_payment_delays_last12mo",
     "number_payment_delays_last12mo must not be NULL - payment behaviour metric",
     100.0, "COMPLETENESS",  "Dept. Telco Risk", "Mrs. Elena Fischer"),
    (305, dq_vr_id, "RULE_2_STRING_NOT_NULL",     catalog, dq_schema, target_table, "user_phone",
     "user_phone must not be NULL or blank - required contact identifier",
     100.0, "COMPLETENESS",  "Dept. Telco Risk", "Mrs. Elena Fischer"),

    # -- ACCURACY --
    (306, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "avg_phone_bill_amt_lst12mo",
     "avg_phone_bill_amt_lst12mo must be >= 0 - billing average cannot be negative",
     100.0, "ACCURACY",      "Dept. Telco Risk", "Mrs. Elena Fischer"),
    (307, dq_vr_id, "RULE_11_NUMERIC_POSITIVE",     catalog, dq_schema, target_table, "phone_bill_amt",
     "phone_bill_amt must be > 0 - active subscribers always have a positive bill",
     100.0, "ACCURACY",      "Dept. Telco Risk", "Mrs. Elena Fischer"),
    (308, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "number_payment_delays_last12mo",
     "number_payment_delays_last12mo must be >= 0 - delay count cannot be negative",
     100.0, "ACCURACY",      "Dept. Telco Risk", "Mrs. Elena Fischer"),
    (309, dq_vr_id, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, target_table, "pct_increase_annual_number_of_delays_last_3_year",
     "pct_increase_annual_number_of_delays_last_3_year must be >= 0 - percentage increase cannot be negative",
     100.0, "ACCURACY",      "Dept. Telco Risk", "Mrs. Elena Fischer"),

    # -- VALIDITY --
    (310, dq_vr_id, "RULE_21_BOOLEAN_FLAG",          catalog, dq_schema, target_table, "is_pre_paid",
     "is_pre_paid must be 0 or 1 - boolean prepaid indicator",
     100.0, "VALIDITY",      "Dept. Telco Risk", "Mrs. Elena Fischer"),
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
print(f"Merged {len(mapping_rows)} rule mappings for {target_table} (IDs 301-310).")

spark.table(f"{catalog}.{dq_schema}.dq_rule_mappings") \
    .filter(F.col("source_table") == target_table) \
    .select("rule_mapping_id", "dq_rule_no", "generic_rule_desc_short", "source_field", "dq_category") \
    .orderBy("rule_mapping_id") \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 5 - Create dq_results table (if not exists)

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_results (dq_run_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), dq_execution_timestamp TIMESTAMP NOT NULL, dq_rule_no STRING NOT NULL, dq_number_relevant_records BIGINT NOT NULL, dq_number_pos_results BIGINT NOT NULL) USING DELTA COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'")
print("Table dq_results ready.")

# COMMAND ----------

print("\nSetup complete. All tables in DQ schema:")
spark.sql(f"SHOW TABLES IN {catalog}.{dq_schema}").show(truncate=False)
