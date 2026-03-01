# Databricks notebook source

# MAGIC %md

# MAGIC # DQ Framework Setup - customer_gold

# MAGIC

# MAGIC Idempotent setup: creates DQ registry tables if they do not exist, seeds all rules

# MAGIC and mappings only when the tables are empty, and copies the source table into the DQ schema

# MAGIC only when it does not already exist.



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
    target_table = "customer_gold"

print(f"Setup Parameters:")
print(f"  DQ Catalog:     {catalog}")
print(f"  DQ Schema:      {dq_schema}")
print(f"  Source Catalog: {source_catalog}")
print(f"  Source Schema:  {source_schema}")
print(f"  Target Table:   {target_table}")

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

# COMMAND ----------

# MAGIC %md ## Step 1 - Create dq_rules_generic (IDENTITY column)

# COMMAND ----------

# NOTE: CREATE TABLE uses single-line SQL - multi-line f-strings with blank lines
# inside the SQL cause spark.sql() to fail silently in Databricks serverless.
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_rules_generic (generic_rule_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), generic_rule_desc_short STRING NOT NULL, rule_description STRING, dq_category STRING, rule_detail STRING, rule_logic_template STRING, applicable_types STRING, bcbs239_principle STRING, solvencyii_pillar STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'Generic DQ rule catalogue. generic_rule_id auto-increments - never set manually.'")
print("dq_rules_generic ready (created if not existed)")

# COMMAND ----------

# MAGIC %md ## Step 2 - Seed generic rules

# COMMAND ----------

generic_rules = [
    # -- COMPLETENESS --------------------------------------------------------
    ("RULE_1_DATE_NOT_NULL", "Date Not Null", "COMPLETENESS",
     "Checks that a DATE or TIMESTAMP field is not NULL. Returns 1 if the field contains a value, 0 if NULL.",
     "CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3,P4", "Pillar1,QRT"),

    ("RULE_2_STRING_NOT_NULL", "String Not Null", "COMPLETENESS",
     "Checks that a STRING field is not NULL or empty.",
     "CASE WHEN {field} IS NOT NULL AND trim({field}) != '' THEN 1 ELSE 0 END",
     "STRING", "P3,P4", "Pillar1,QRT"),

    ("RULE_3_NUMERIC_NOT_NULL", "Numeric Not Null", "COMPLETENESS",
     "Checks that a numeric field is not NULL.",
     "CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT", "P3,P4", "Pillar1"),

    # -- ACCURACY ------------------------------------------------------------
    ("RULE_10_NUMERIC_NON_NEGATIVE", "Numeric Non-Negative", "ACCURACY",
     "Checks that a numeric field is >= 0. Amounts and counts must not be negative.",
     "CASE WHEN {field} >= 0 THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT", "P3", "Pillar1"),

    ("RULE_11_NUMERIC_POSITIVE", "Numeric Positive", "ACCURACY",
     "Checks that a numeric field is > 0.",
     "CASE WHEN {field} > 0 THEN 1 ELSE 0 END",
     "INTEGER,LONG,DOUBLE,DECIMAL,FLOAT", "P3", "Pillar1"),

    ("RULE_12_DATE_NOT_FUTURE", "Date Not in Future", "ACCURACY",
     "Checks that a DATE field is not in the future relative to current date.",
     "CASE WHEN {field} <= current_date() THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3", "QRT"),

    ("RULE_13_DATE_NOT_PAST_LIMIT", "Date Within Reasonable Past", "ACCURACY",
     "Checks that a DATE field is not older than 10 years (data staleness check).",
     "CASE WHEN {field} >= date_sub(current_date(), 3650) THEN 1 ELSE 0 END",
     "DATE,TIMESTAMP", "P3,P5", "Pillar1"),

    # -- VALIDITY ------------------------------------------------------------
    ("RULE_20_ISO_COUNTRY_CODE", "ISO 3166-1 Alpha-2 Country Code", "VALIDITY",
     "Checks that a STRING field contains a valid ISO 3166-1 alpha-2 country code (2 uppercase letters).",
     "CASE WHEN {field} RLIKE '^[A-Z]{2}$' THEN 1 ELSE 0 END",
     "STRING", "P3", "QRT"),

    ("RULE_21_BOOLEAN_FLAG", "Boolean Flag (0 or 1)", "VALIDITY",
     "Checks that an integer field contains only 0 or 1.",
     "CASE WHEN {field} IN (0, 1) THEN 1 ELSE 0 END",
     "INTEGER", "P3", "Pillar2"),

    # -- UNIQUENESS ----------------------------------------------------------
    ("RULE_30_UNIQUE_IDENTIFIER", "Unique Identifier", "UNIQUENESS",
     "Checks that a field has no duplicate values across the dataset. Applied at dataset level - marks duplicates as 0.",
     "ROW_NUMBER() OVER (PARTITION BY {field} ORDER BY {field}) = 1",
     "STRING,INTEGER,LONG", "P3", "Pillar1"),
]

cnt = spark.table(f"{catalog}.{dq_schema}.dq_rules_generic").count()
if cnt == 0:
    rows = [Row(
        generic_rule_desc_short=r[0], rule_description=r[1], dq_category=r[2],
        rule_detail=r[3], rule_logic_template=r[4], applicable_types=r[5],
        bcbs239_principle=r[6], solvencyii_pillar=r[7],
        active=1, created_date=date.today(), created_by="dq-framework"
    ) for r in generic_rules]
    spark.createDataFrame(rows).createOrReplaceTempView("_dq_rules_generic_staging")
    spark.sql(f"INSERT INTO {catalog}.{dq_schema}.dq_rules_generic (generic_rule_desc_short, rule_description, dq_category, rule_detail, rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar, active, created_date, created_by) SELECT generic_rule_desc_short, rule_description, dq_category, rule_detail, rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar, active, created_date, created_by FROM _dq_rules_generic_staging")
    print(f"Seeded {len(rows)} generic rules. generic_rule_id values assigned by Delta.")
else:
    print(f"dq_rules_generic already has {cnt} row(s) - skipping seed.")

spark.table(f"{catalog}.{dq_schema}.dq_rules_generic") \
    .select("generic_rule_id", "generic_rule_desc_short", "dq_category") \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 3 - Create dq_validation_request

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_validation_request (dq_val_request_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), dq_validation_request_short STRING NOT NULL, owner_dept STRING, owner_user STRING, active INTEGER, created_date DATE, created_by STRING) USING DELTA COMMENT 'DQ validation request registry. dq_val_request_id auto-increments - never set manually.'")
print("dq_validation_request ready (created if not existed)")

cnt = spark.table(f"{catalog}.{dq_schema}.dq_validation_request").count()
if cnt == 0:
    vr_rows = [Row(
        dq_validation_request_short="DQ_VAL_REQUEST_1_customer_gold",
        owner_dept="Dept. Master Data", owner_user="Mr. Alex Smith",
        active=1, created_date=date.today(), created_by="dq-framework"
    )]
    spark.createDataFrame(vr_rows).createOrReplaceTempView("_dq_vr_staging")
    spark.sql(f"INSERT INTO {catalog}.{dq_schema}.dq_validation_request (dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by) SELECT dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by FROM _dq_vr_staging")
    print("Seeded 1 validation request.")
else:
    print(f"dq_validation_request already has {cnt} row(s) - skipping seed.")

spark.table(f"{catalog}.{dq_schema}.dq_validation_request").show(truncate=False)

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

# MAGIC %md ## Step 5 - Create dq_rule_mappings

# COMMAND ----------

# 25 rules covering UNIQUENESS, COMPLETENESS, ACCURACY, VALIDITY for customer_gold
mappings = [
    # -- UNIQUENESS --
    (101, 1, "RULE_30_UNIQUE_IDENTIFIER", catalog, dq_schema, "customer_gold", "id",
     "id must be unique across all customer records", 100.0, "UNIQUENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (102, 1, "RULE_30_UNIQUE_IDENTIFIER", catalog, dq_schema, "customer_gold", "cust_id",
     "cust_id must be unique", 100.0, "UNIQUENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (103, 1, "RULE_30_UNIQUE_IDENTIFIER", catalog, dq_schema, "customer_gold", "email",
     "email must be unique - no duplicate customer records", 100.0, "UNIQUENESS", "Dept. Master Data", "Mr. Alex Smith"),
    # -- COMPLETENESS - identifiers --
    (104, 1, "RULE_3_NUMERIC_NOT_NULL", catalog, dq_schema, "customer_gold", "id",
     "id must not be NULL", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (105, 1, "RULE_3_NUMERIC_NOT_NULL", catalog, dq_schema, "customer_gold", "cust_id",
     "cust_id must not be NULL", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    # -- COMPLETENESS - personal data --
    (106, 1, "RULE_2_STRING_NOT_NULL", catalog, dq_schema, "customer_gold", "first_name",
     "first_name must not be NULL or empty", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (107, 1, "RULE_2_STRING_NOT_NULL", catalog, dq_schema, "customer_gold", "last_name",
     "last_name must not be NULL or empty", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (108, 1, "RULE_2_STRING_NOT_NULL", catalog, dq_schema, "customer_gold", "email",
     "email must not be NULL or empty", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (109, 1, "RULE_2_STRING_NOT_NULL", catalog, dq_schema, "customer_gold", "document_id",
     "document_id must not be NULL - required for KYC", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    # -- COMPLETENESS/ACCURACY - dates --
    (110, 1, "RULE_1_DATE_NOT_NULL", catalog, dq_schema, "customer_gold", "join_date",
     "join_date must not be NULL", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (111, 1, "RULE_12_DATE_NOT_FUTURE", catalog, dq_schema, "customer_gold", "join_date",
     "join_date must not be in the future", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (112, 1, "RULE_13_DATE_NOT_PAST_LIMIT", catalog, dq_schema, "customer_gold", "join_date",
     "join_date must not be older than 10 years", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    # -- COMPLETENESS/ACCURACY - passport --
    (113, 1, "RULE_1_DATE_NOT_NULL", catalog, dq_schema, "customer_gold", "passport_expiry",
     "passport_expiry must not be NULL (KYC)", 99.5, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (114, 1, "RULE_12_DATE_NOT_FUTURE", catalog, dq_schema, "customer_gold", "passport_expiry",
     "DATE_NOT_FUTURE not applicable to expiry dates - expected to be in the future", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    # -- ACCURACY - financial amounts --
    (115, 1, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, "customer_gold", "tot_rel_bal",
     "tot_rel_bal must be >= 0 - total relationship balance", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (116, 1, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, "customer_gold", "revenue_tot",
     "revenue_tot must be >= 0 - total revenue", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (117, 1, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, "customer_gold", "revenue_12m",
     "revenue_12m must be >= 0 - 12-month revenue", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (118, 1, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, "customer_gold", "card_balance_amount",
     "card_balance_amount must be >= 0", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (119, 1, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, "customer_gold", "loan_balance_amount",
     "loan_balance_amount must be >= 0", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (120, 1, "RULE_10_NUMERIC_NON_NEGATIVE", catalog, dq_schema, "customer_gold", "total_deposits_amount",
     "total_deposits_amount must be >= 0", 100.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    # -- COMPLETENESS + ACCURACY - income --
    (121, 1, "RULE_3_NUMERIC_NOT_NULL", catalog, dq_schema, "customer_gold", "income_monthly",
     "income_monthly must not be NULL", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (122, 1, "RULE_11_NUMERIC_POSITIVE", catalog, dq_schema, "customer_gold", "income_monthly",
     "income_monthly must be > 0 - financial capacity check", 99.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    (123, 1, "RULE_3_NUMERIC_NOT_NULL", catalog, dq_schema, "customer_gold", "income_annual",
     "income_annual must not be NULL", 100.0, "COMPLETENESS", "Dept. Master Data", "Mr. Alex Smith"),
    (124, 1, "RULE_11_NUMERIC_POSITIVE", catalog, dq_schema, "customer_gold", "income_annual",
     "income_annual must be > 0 - financial capacity check", 99.0, "ACCURACY", "Dept. Master Data", "Mr. Alex Smith"),
    # -- VALIDITY - boolean --
    (125, 1, "RULE_21_BOOLEAN_FLAG", catalog, dq_schema, "customer_gold", "is_resident",
     "is_resident must be 0 or 1 - residency status flag", 100.0, "VALIDITY", "Dept. Master Data", "Mr. Alex Smith"),
]

mapping_rows = [Row(
    rule_mapping_id=m[0],
    dq_rule_no="DQ_" + str(m[0]) + "_" + "_".join(m[2].split("_")[2:]) + "_" + m[6],
    dq_val_request_id=m[1], generic_rule_desc_short=m[2],
    source_catalog=m[3], source_schema=m[4], source_table=m[5], source_field=m[6],
    rule_description=m[7], threshold_pct=m[8],
    dq_category=m[9], owner_dept=m[10], owner_user=m[11],
    active=1, created_date=date.today(), created_by="dq-framework"
) for m in mappings]

mappings_full = f"{catalog}.{dq_schema}.dq_rule_mappings"
if spark.catalog.tableExists(mappings_full):
    cnt = spark.table(mappings_full).count()
    print(f"dq_rule_mappings already has {cnt} row(s) - skipping seed.")
else:
    spark.createDataFrame(mapping_rows).write.format("delta").saveAsTable(mappings_full)
    print(f"Registered {len(mapping_rows)} rule mappings for customer_gold.")

# COMMAND ----------

# MAGIC %md ## Step 6 - Create dq_results table (auto-increment dq_run_id)

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{dq_schema}.dq_results (dq_run_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), dq_execution_timestamp TIMESTAMP NOT NULL, dq_rule_no STRING NOT NULL, dq_number_relevant_records BIGINT NOT NULL, dq_number_pos_results BIGINT NOT NULL) USING DELTA COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'")
print("Table dq_results ready.")

# COMMAND ----------

print("\nSetup complete. All tables in DQ schema:")
spark.sql(f"SHOW TABLES IN {catalog}.{dq_schema}").show(truncate=False)
