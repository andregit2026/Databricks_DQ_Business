"""
Entity configuration for customer_gold.
Validation request: DQ_VAL_REQUEST_1_customer_gold (first entry, auto-assigned ID 1).
Rule mapping IDs: 101-125 (25 rules).
"""

ENTITY               = "customer_gold"
SOURCE_CATALOG       = "databricks_snippets_7405610928938750"
SOURCE_SCHEMA        = "dbdemos_fsi_credit"
DQ_VAL_REQUEST_SHORT = "DQ_VAL_REQUEST_1_customer_gold"
OWNER_DEPT           = "Dept. Master Data"
OWNER_USER           = "Mr. Alex Smith"
SCHEDULE_CRON        = None   # no schedule

# New generic rules introduced by this entity (none — uses the base set)
NEW_GENERIC_RULES = []

# Rule mappings: (mapping_id, generic_rule_desc_short, field, description, threshold_pct, dq_category)
MAPPINGS = [
    # -- UNIQUENESS --
    (101, "RULE_30_UNIQUE_IDENTIFIER",    "id",                   "id must be unique across all customer records",                    100.0, "UNIQUENESS"),
    (102, "RULE_30_UNIQUE_IDENTIFIER",    "cust_id",              "cust_id must be unique",                                           100.0, "UNIQUENESS"),
    (103, "RULE_30_UNIQUE_IDENTIFIER",    "email",                "email must be unique - no duplicate customer records",             100.0, "UNIQUENESS"),
    # -- COMPLETENESS - identifiers --
    (104, "RULE_3_NUMERIC_NOT_NULL",      "id",                   "id must not be NULL",                                              100.0, "COMPLETENESS"),
    (105, "RULE_3_NUMERIC_NOT_NULL",      "cust_id",              "cust_id must not be NULL",                                         100.0, "COMPLETENESS"),
    # -- COMPLETENESS - personal data --
    (106, "RULE_2_STRING_NOT_NULL",       "first_name",           "first_name must not be NULL or empty",                             100.0, "COMPLETENESS"),
    (107, "RULE_2_STRING_NOT_NULL",       "last_name",            "last_name must not be NULL or empty",                              100.0, "COMPLETENESS"),
    (108, "RULE_2_STRING_NOT_NULL",       "email",                "email must not be NULL or empty",                                  100.0, "COMPLETENESS"),
    (109, "RULE_2_STRING_NOT_NULL",       "document_id",          "document_id must not be NULL - required for KYC",                  100.0, "COMPLETENESS"),
    # -- COMPLETENESS/ACCURACY - dates --
    (110, "RULE_1_DATE_NOT_NULL",         "join_date",            "join_date must not be NULL",                                       100.0, "COMPLETENESS"),
    (111, "RULE_12_DATE_NOT_FUTURE",      "join_date",            "join_date must not be in the future",                              100.0, "ACCURACY"),
    (112, "RULE_13_DATE_NOT_PAST_LIMIT",  "join_date",            "join_date must not be older than 10 years",                        100.0, "ACCURACY"),
    # -- COMPLETENESS/ACCURACY - passport --
    (113, "RULE_1_DATE_NOT_NULL",         "passport_expiry",      "passport_expiry must not be NULL (KYC)",                            99.5, "COMPLETENESS"),
    (114, "RULE_14_DATE_NOT_FUTURE_LIMIT","passport_expiry",      "DATE_NOT_FUTURE not applicable to expiry dates - expected in future",100.0, "ACCURACY"),
    # -- ACCURACY - financial amounts --
    (115, "RULE_10_NUMERIC_NON_NEGATIVE", "tot_rel_bal",          "tot_rel_bal must be >= 0 - total relationship balance",             100.0, "ACCURACY"),
    (116, "RULE_10_NUMERIC_NON_NEGATIVE", "revenue_tot",          "revenue_tot must be >= 0 - total revenue",                         100.0, "ACCURACY"),
    (117, "RULE_10_NUMERIC_NON_NEGATIVE", "revenue_12m",          "revenue_12m must be >= 0 - 12-month revenue",                      100.0, "ACCURACY"),
    (118, "RULE_10_NUMERIC_NON_NEGATIVE", "card_balance_amount",  "card_balance_amount must be >= 0",                                 100.0, "ACCURACY"),
    (119, "RULE_10_NUMERIC_NON_NEGATIVE", "loan_balance_amount",  "loan_balance_amount must be >= 0",                                 100.0, "ACCURACY"),
    (120, "RULE_10_NUMERIC_NON_NEGATIVE", "total_deposits_amount","total_deposits_amount must be >= 0",                               100.0, "ACCURACY"),
    # -- COMPLETENESS + ACCURACY - income --
    (121, "RULE_3_NUMERIC_NOT_NULL",      "income_monthly",       "income_monthly must not be NULL",                                  100.0, "COMPLETENESS"),
    (122, "RULE_11_NUMERIC_POSITIVE",     "income_monthly",       "income_monthly must be > 0 - financial capacity check",             99.0, "ACCURACY"),
    (123, "RULE_3_NUMERIC_NOT_NULL",      "income_annual",        "income_annual must not be NULL",                                   100.0, "COMPLETENESS"),
    (124, "RULE_11_NUMERIC_POSITIVE",     "income_annual",        "income_annual must be > 0 - financial capacity check",              99.0, "ACCURACY"),
    # -- VALIDITY - boolean --
    (125, "RULE_21_BOOLEAN_FLAG",         "is_resident",          "is_resident must be 0 or 1 - residency status flag",               100.0, "VALIDITY"),
]
