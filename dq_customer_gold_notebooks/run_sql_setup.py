"""
Execute DQ framework setup via SQL warehouse (Statement Execution API).
Drops old tables, creates new schema tables, inserts seed data, copies source table.
"""
import configparser, os, json, urllib.request, urllib.error, time

cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.databrickscfg"))
d = cfg.defaults()
host  = d["host"].rstrip("/")
token = d["token"]
WH_ID = "94cbf5bc4ec73e30"
CAT   = "databricks_snippets_7405610928938750"
SCH   = "dbdemos_dq_business_arausch"
SRC   = f"{CAT}.dbdemos_fsi_credit.customer_gold"
DQ    = f"{CAT}.{SCH}"
TODAY = "2026-03-01"

def sql_run(statement, label="", timeout=180):
    """Execute a SQL statement, polling until complete."""
    # Submit with 50s synchronous wait (API max), then poll if still running
    req = urllib.request.Request(
        f"{host}/api/2.0/sql/statements",
        data=json.dumps({
            "warehouse_id": WH_ID,
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE"
        }).encode(),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as r:
            result = json.loads(r.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"  HTTP ERROR {e.code} [{label}]: {err[:300]}")
        return None

    # Poll if still pending/running
    stmt_id = result.get("statement_id")
    deadline = time.time() + timeout
    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            print(f"  TIMEOUT [{label}]")
            return result
        time.sleep(3)
        poll_req = urllib.request.Request(
            f"{host}/api/2.0/sql/statements/{stmt_id}",
            headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(poll_req) as r:
            result = json.loads(r.read())

    state = result.get("status", {}).get("state", "?")
    if state != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        print(f"  FAILED [{label}]: {state} - {err.get('message','')[:300]}")
    else:
        rows = result.get("result", {}).get("data_array", [])
        print(f"  OK [{label}] ({len(rows)} rows returned)")
    return result

# ── Step 1: Drop old tables ──────────────────────────────────────────────────
print("\n=== Step 1: Drop old tables ===")
sql_run(f"DROP TABLE IF EXISTS {DQ}.dq_rules_generic",   "drop dq_rules_generic")
sql_run(f"DROP TABLE IF EXISTS {DQ}.dq_rule_mappings",   "drop dq_rule_mappings")
sql_run(f"DROP TABLE IF EXISTS {DQ}.dq_validation_request", "drop dq_validation_request")
sql_run(f"DROP TABLE IF EXISTS {DQ}.dq_results",         "drop dq_results")
sql_run(f"DROP TABLE IF EXISTS {DQ}.dq_run_results",     "drop dq_run_results")
sql_run(f"DROP TABLE IF EXISTS {DQ}.customer_gold_dq",   "drop customer_gold_dq")

# ── Step 2: Create dq_rules_generic ─────────────────────────────────────────
print("\n=== Step 2: Create dq_rules_generic ===")
sql_run(f"""
CREATE TABLE {DQ}.dq_rules_generic (
    generic_rule_id         BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    generic_rule_desc_short STRING NOT NULL,
    rule_description        STRING,
    dq_category             STRING,
    rule_detail             STRING,
    rule_logic_template     STRING,
    applicable_types        STRING,
    bcbs239_principle       STRING,
    solvencyii_pillar       STRING,
    active                  INT,
    created_date            DATE,
    created_by              STRING
)
USING DELTA
COMMENT 'Generic DQ rule catalogue. generic_rule_id auto-increments - never set manually.'
""", "create dq_rules_generic")

# ── Step 3: Seed generic rules ───────────────────────────────────────────────
print("\n=== Step 3: Seed generic rules ===")
rules_sql = f"""
INSERT INTO {DQ}.dq_rules_generic
    (generic_rule_desc_short, rule_description, dq_category, rule_detail,
     rule_logic_template, applicable_types, bcbs239_principle, solvencyii_pillar,
     active, created_date, created_by)
VALUES
  ('RULE_1_DATE_NOT_NULL','Date Not Null','COMPLETENESS',
   'Checks that a DATE or TIMESTAMP field is not NULL. Returns 1 if the field contains a value, 0 if NULL.',
   'CASE WHEN {{field}} IS NOT NULL THEN 1 ELSE 0 END',
   'DATE,TIMESTAMP','P3,P4','Pillar1,QRT',1,DATE'{TODAY}','dq-framework'),
  ('RULE_2_STRING_NOT_NULL','String Not Null','COMPLETENESS',
   'Checks that a STRING field is not NULL or empty.',
   'CASE WHEN {{field}} IS NOT NULL AND trim({{field}}) != \\'\\' THEN 1 ELSE 0 END',
   'STRING','P3,P4','Pillar1,QRT',1,DATE'{TODAY}','dq-framework'),
  ('RULE_3_NUMERIC_NOT_NULL','Numeric Not Null','COMPLETENESS',
   'Checks that a numeric field is not NULL.',
   'CASE WHEN {{field}} IS NOT NULL THEN 1 ELSE 0 END',
   'INTEGER,LONG,DOUBLE,DECIMAL,FLOAT','P3,P4','Pillar1',1,DATE'{TODAY}','dq-framework'),
  ('RULE_10_NUMERIC_NON_NEGATIVE','Numeric Non-Negative','ACCURACY',
   'Checks that a numeric field is >= 0. Amounts and counts must not be negative.',
   'CASE WHEN {{field}} >= 0 THEN 1 ELSE 0 END',
   'INTEGER,LONG,DOUBLE,DECIMAL,FLOAT','P3','Pillar1',1,DATE'{TODAY}','dq-framework'),
  ('RULE_11_NUMERIC_POSITIVE','Numeric Positive','ACCURACY',
   'Checks that a numeric field is > 0.',
   'CASE WHEN {{field}} > 0 THEN 1 ELSE 0 END',
   'INTEGER,LONG,DOUBLE,DECIMAL,FLOAT','P3','Pillar1',1,DATE'{TODAY}','dq-framework'),
  ('RULE_12_DATE_NOT_FUTURE','Date Not in Future','ACCURACY',
   'Checks that a DATE field is not in the future relative to current date.',
   'CASE WHEN {{field}} <= current_date() THEN 1 ELSE 0 END',
   'DATE,TIMESTAMP','P3','QRT',1,DATE'{TODAY}','dq-framework'),
  ('RULE_13_DATE_NOT_PAST_LIMIT','Date Within Reasonable Past','ACCURACY',
   'Checks that a DATE field is not older than 10 years (data staleness check).',
   'CASE WHEN {{field}} >= date_sub(current_date(), 3650) THEN 1 ELSE 0 END',
   'DATE,TIMESTAMP','P3,P5','Pillar1',1,DATE'{TODAY}','dq-framework'),
  ('RULE_20_ISO_COUNTRY_CODE','ISO 3166-1 Alpha-2 Country Code','VALIDITY',
   'Checks that a STRING field contains a valid ISO 3166-1 alpha-2 country code.',
   'CASE WHEN {{field}} RLIKE ''^[A-Z]{{2}}$'' THEN 1 ELSE 0 END',
   'STRING','P3','QRT',1,DATE'{TODAY}','dq-framework'),
  ('RULE_21_BOOLEAN_FLAG','Boolean Flag (0 or 1)','VALIDITY',
   'Checks that an integer field contains only 0 or 1.',
   'CASE WHEN {{field}} IN (0, 1) THEN 1 ELSE 0 END',
   'INTEGER','P3','Pillar2',1,DATE'{TODAY}','dq-framework'),
  ('RULE_30_UNIQUE_IDENTIFIER','Unique Identifier','UNIQUENESS',
   'Checks that a field has no duplicate values across the dataset. Applied at dataset level - marks duplicates as 0.',
   'ROW_NUMBER() OVER (PARTITION BY {{field}} ORDER BY {{field}}) = 1',
   'STRING,INTEGER,LONG','P3','Pillar1',1,DATE'{TODAY}','dq-framework')
"""
sql_run(rules_sql, "insert generic rules")

# ── Step 4: Create dq_validation_request ────────────────────────────────────
print("\n=== Step 4: Create dq_validation_request ===")
sql_run(f"""
CREATE TABLE {DQ}.dq_validation_request (
    dq_val_request_id           BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    dq_validation_request_short STRING NOT NULL,
    owner_dept                  STRING,
    owner_user                  STRING,
    active                      INT,
    created_date                DATE,
    created_by                  STRING
)
USING DELTA
COMMENT 'DQ validation request registry. dq_val_request_id auto-increments - never set manually.'
""", "create dq_validation_request")

sql_run(f"""
INSERT INTO {DQ}.dq_validation_request
    (dq_validation_request_short, owner_dept, owner_user, active, created_date, created_by)
VALUES
  ('DQ_VAL_REQUEST_1_customer_gold','Dept. Master Data','Mr. Alex Smith',1,DATE'{TODAY}','dq-framework')
""", "insert validation request")

# ── Step 5: Create dq_rule_mappings ─────────────────────────────────────────
print("\n=== Step 5: Create dq_rule_mappings ===")
sql_run(f"""
CREATE TABLE {DQ}.dq_rule_mappings (
    rule_mapping_id         INT         NOT NULL,
    dq_rule_no              STRING      NOT NULL,
    dq_val_request_id       INT,
    generic_rule_desc_short STRING      NOT NULL,
    source_catalog          STRING,
    source_schema           STRING,
    source_table            STRING,
    source_field            STRING,
    rule_description        STRING,
    threshold_pct           DOUBLE,
    dq_category             STRING,
    owner_dept              STRING,
    owner_user              STRING,
    active                  INT,
    created_date            DATE,
    created_by              STRING
)
USING DELTA
COMMENT 'Maps generic DQ rules to customer_gold fields. rule_mapping_id is the integer key, dq_rule_no is the human-readable label.'
""", "create dq_rule_mappings")

# ── Step 6: Insert rule mappings ─────────────────────────────────────────────
print("\n=== Step 6: Insert rule mappings ===")
def mp(rule_mapping_id, rule_desc_short, field, desc, threshold, cat=None):
    cat_val = f"'{cat}'" if cat else "NULL"
    parts = rule_desc_short.split("_")[2:]  # e.g. RULE_30_UNIQUE_IDENTIFIER -> ['UNIQUE','IDENTIFIER']
    dq_rule_no = f"DQ_{rule_mapping_id}_{'_'.join(parts)}_{field}"
    return (f"({rule_mapping_id},'{dq_rule_no}',1,'{rule_desc_short}','{CAT}','{SCH}','customer_gold','{field}',"
            f"'{desc}',{threshold},{cat_val},"
            f"'Dept. Master Data','Mr. Alex Smith',1,DATE'{TODAY}','dq-framework')")

rows = ",\n  ".join([
    # UNIQUENESS
    mp(101,"RULE_30_UNIQUE_IDENTIFIER","id",             "id must be unique across all customer records (BCBS239 P3)",100.0),
    mp(102,"RULE_30_UNIQUE_IDENTIFIER","cust_id",        "cust_id must be unique (BCBS239 P3)",100.0),
    mp(103,"RULE_30_UNIQUE_IDENTIFIER","email",          "email must be unique - no duplicate customer records (BCBS239 P3)",100.0),
    # COMPLETENESS - identifiers
    mp(104,"RULE_3_NUMERIC_NOT_NULL","id",               "id must not be NULL (BCBS239 P4)",100.0),
    mp(105,"RULE_3_NUMERIC_NOT_NULL","cust_id",          "cust_id must not be NULL (BCBS239 P4)",100.0),
    # COMPLETENESS - personal data
    mp(106,"RULE_2_STRING_NOT_NULL","first_name",        "first_name must not be NULL or empty (BCBS239 P4)",100.0),
    mp(107,"RULE_2_STRING_NOT_NULL","last_name",         "last_name must not be NULL or empty (BCBS239 P4)",100.0),
    mp(108,"RULE_2_STRING_NOT_NULL","email",             "email must not be NULL or empty (BCBS239 P4)",100.0),
    mp(109,"RULE_2_STRING_NOT_NULL","document_id",       "document_id must not be NULL - required for KYC (BCBS239 P4)",100.0),
    # ACCURACY - dates
    mp(110,"RULE_1_DATE_NOT_NULL","join_date",           "join_date must not be NULL (BCBS239 P4)",100.0),
    mp(111,"RULE_12_DATE_NOT_FUTURE","join_date",        "join_date must not be in the future (BCBS239 P3)",100.0),
    mp(112,"RULE_13_DATE_NOT_PAST_LIMIT","join_date",    "join_date must not be older than 10 years (BCBS239 P3,P5)",100.0),
    # passport_expiry - NULL check applies; DATE_NOT_FUTURE is NOT applicable
    mp(113,"RULE_1_DATE_NOT_NULL","passport_expiry",     "passport_expiry must not be NULL (KYC / Solvency II QRT)",99.5),
    mp(114,"RULE_12_DATE_NOT_FUTURE","passport_expiry",  "DATE_NOT_FUTURE not applicable to expiry dates - expected to be in the future",100.0),
    # ACCURACY - financial amounts
    mp(115,"RULE_10_NUMERIC_NON_NEGATIVE","tot_rel_bal",          "tot_rel_bal must be >= 0 - total relationship balance (BCBS239 P3)",100.0,"ACCURACY"),
    mp(116,"RULE_10_NUMERIC_NON_NEGATIVE","revenue_tot",          "revenue_tot must be >= 0 - total revenue (BCBS239 P3)",100.0,"ACCURACY"),
    mp(117,"RULE_10_NUMERIC_NON_NEGATIVE","revenue_12m",          "revenue_12m must be >= 0 - 12-month revenue (BCBS239 P3)",100.0,"ACCURACY"),
    mp(118,"RULE_10_NUMERIC_NON_NEGATIVE","card_balance_amount",  "card_balance_amount must be >= 0 (BCBS239 P3)",100.0,"ACCURACY"),
    mp(119,"RULE_10_NUMERIC_NON_NEGATIVE","loan_balance_amount",  "loan_balance_amount must be >= 0 (BCBS239 P3)",100.0,"ACCURACY"),
    mp(120,"RULE_10_NUMERIC_NON_NEGATIVE","total_deposits_amount","total_deposits_amount must be >= 0 (BCBS239 P3)",100.0,"ACCURACY"),
    # COMPLETENESS + ACCURACY - income
    mp(121,"RULE_3_NUMERIC_NOT_NULL","income_monthly",   "income_monthly must not be NULL (BCBS239 P4)",100.0),
    mp(122,"RULE_11_NUMERIC_POSITIVE","income_monthly",  "income_monthly must be > 0 - financial capacity check (Solvency II Pillar1)",99.0,"ACCURACY"),
    mp(123,"RULE_3_NUMERIC_NOT_NULL","income_annual",    "income_annual must not be NULL (BCBS239 P4)",100.0),
    mp(124,"RULE_11_NUMERIC_POSITIVE","income_annual",   "income_annual must be > 0 - financial capacity check (Solvency II Pillar1)",99.0,"ACCURACY"),
    # VALIDITY
    mp(125,"RULE_21_BOOLEAN_FLAG","is_resident",         "is_resident must be 0 or 1 - residency status flag (BCBS239 P3)",100.0),
])

sql_run(f"""
INSERT INTO {DQ}.dq_rule_mappings
    (rule_mapping_id, dq_rule_no, dq_val_request_id, generic_rule_desc_short, source_catalog, source_schema,
     source_table, source_field, rule_description, threshold_pct,
     dq_category, owner_dept, owner_user, active, created_date, created_by)
VALUES
  {rows}
""", "insert 25 rule mappings")

# ── Step 7: Copy customer_gold from source ───────────────────────────────────
print("\n=== Step 7: Copy customer_gold from source (CTAS) ===")
sql_run(f"""
CREATE OR REPLACE TABLE {DQ}.customer_gold
AS SELECT * FROM {SRC}
""", "CTAS customer_gold")

# ── Step 8: Create dq_results (auto-increment) ───────────────────────────────
print("\n=== Step 8: Create dq_results ===")
sql_run(f"""
CREATE TABLE IF NOT EXISTS {DQ}.dq_results (
    dq_run_id                   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    dq_execution_timestamp      TIMESTAMP NOT NULL,
    dq_rule_no                  STRING    NOT NULL,
    dq_number_relevant_records  BIGINT    NOT NULL,
    dq_number_pos_results       BIGINT    NOT NULL
)
USING DELTA
COMMENT 'Aggregated DQ results per rule per pipeline run. dq_run_id auto-increments.'
""", "create dq_results")

# ── Verify ───────────────────────────────────────────────────────────────────
print("\n=== Verify ===")
res = sql_run(f"SELECT COUNT(*) AS cnt FROM {DQ}.dq_rules_generic", "count rules")
if res:
    rows_data = res.get("result", {}).get("data_array", [])
    print(f"  dq_rules_generic rows: {rows_data[0][0] if rows_data else '?'}")

res = sql_run(f"SELECT COUNT(*) AS cnt FROM {DQ}.dq_rule_mappings", "count mappings")
if res:
    rows_data = res.get("result", {}).get("data_array", [])
    print(f"  dq_rule_mappings rows: {rows_data[0][0] if rows_data else '?'}")

res = sql_run(f"SELECT COUNT(*) AS cnt FROM {DQ}.customer_gold", "count customer_gold")
if res:
    rows_data = res.get("result", {}).get("data_array", [])
    print(f"  customer_gold rows: {rows_data[0][0] if rows_data else '?'}")

print("\nSQL setup complete.")
