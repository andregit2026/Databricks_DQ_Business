"""
Entity configuration for fund_trans_gold.
Validation request: DQ_VAL_REQUEST_2_fund_trans_gold (second entry, auto-assigned ID 2).
Rule mapping IDs: 201-218 (18 rules).
Two new generic rules: RULE_40, RULE_41.
"""

ENTITY               = "fund_trans_gold"
SOURCE_CATALOG       = "databricks_snippets_7405610928938750"
SOURCE_SCHEMA        = "dbdemos_fsi_credit"
DQ_VAL_REQUEST_SHORT = "DQ_VAL_REQUEST_2_fund_trans_gold"
OWNER_DEPT           = "Dept. Transaction Data"
OWNER_USER           = "Mr. Alex Smith"
SCHEDULE_CRON        = "0 0 7 * * ?"   # daily 07:00 UTC

# New generic rules introduced by this entity.
# These are MERGEd into dq_rules_generic (idempotent).
NEW_GENERIC_RULES = [
    {
        "generic_rule_desc_short": "RULE_40_TEMPORAL_WINDOW_CONSISTENCY",
        "rule_description":        "Temporal Window Consistency",
        "dq_category":             "CONSISTENCY",
        "rule_detail":             (
            "Checks that a 3-month metric does not exceed the 12-month metric for the same "
            "field (aggregation integrity). field_name is pipe-delimited: {field_3m}|{field_12m}."
        ),
        "rule_logic_template":     "CASE WHEN {field_3m} <= {field_12m} THEN 1 ELSE 0 END",
        "applicable_types":        "LONG,DOUBLE",
        "bcbs239_principle":       "P3",
        "solvencyii_pillar":       "Pillar3",
    },
    {
        "generic_rule_desc_short": "RULE_41_POSITIVE_IF_COUNT_NONZERO",
        "rule_description":        "Positive Amount if Count Non-Zero",
        "dq_category":             "ACCURACY",
        "rule_detail":             (
            "Checks that an amount is > 0 when the corresponding transaction count is > 0. "
            "A zero amount with a non-zero count is a data integrity failure. "
            "field_name is pipe-delimited: {count_field}|{amount_field}."
        ),
        "rule_logic_template":     "CASE WHEN {count_field} = 0 OR {amount_field} > 0 THEN 1 ELSE 0 END",
        "applicable_types":        "LONG,DOUBLE",
        "bcbs239_principle":       "P2",
        "solvencyii_pillar":       "Pillar1",
    },
]

# Rule mappings: (mapping_id, generic_rule_desc_short, field, description, threshold_pct, dq_category)
# Two-column rules use pipe-delimited field: "col_a|col_b"
MAPPINGS = [
    # -- UNIQUENESS --
    (201, "RULE_30_UNIQUE_IDENTIFIER",               "cust_id",
     "cust_id is the primary key - must be unique across all fund_trans_gold records",         100.0, "UNIQUENESS"),
    # -- COMPLETENESS - core 12m metrics --
    (202, "RULE_3_NUMERIC_NOT_NULL",                 "sent_txn_cnt_12m",
     "sent_txn_cnt_12m must not be NULL - core sent volume metric (12m)",                      100.0, "COMPLETENESS"),
    (203, "RULE_3_NUMERIC_NOT_NULL",                 "sent_txn_amt_12m",
     "sent_txn_amt_12m must not be NULL - core sent amount metric (12m)",                      100.0, "COMPLETENESS"),
    (204, "RULE_3_NUMERIC_NOT_NULL",                 "rcvd_txn_cnt_12m",
     "rcvd_txn_cnt_12m must not be NULL - core received volume metric (12m)",                  100.0, "COMPLETENESS"),
    (205, "RULE_3_NUMERIC_NOT_NULL",                 "rcvd_txn_amt_12m",
     "rcvd_txn_amt_12m must not be NULL - core received amount metric (12m)",                  100.0, "COMPLETENESS"),
    # -- ACCURACY - amounts must be >= 0 (all windows) --
    (206, "RULE_10_NUMERIC_NON_NEGATIVE",            "sent_txn_amt_12m",
     "sent_txn_amt_12m must be >= 0 - transaction amount cannot be negative",                  100.0, "ACCURACY"),
    (207, "RULE_10_NUMERIC_NON_NEGATIVE",            "rcvd_txn_amt_12m",
     "rcvd_txn_amt_12m must be >= 0 - transaction amount cannot be negative",                  100.0, "ACCURACY"),
    (208, "RULE_10_NUMERIC_NON_NEGATIVE",            "sent_txn_amt_6m",
     "sent_txn_amt_6m must be >= 0 - transaction amount cannot be negative",                   100.0, "ACCURACY"),
    (209, "RULE_10_NUMERIC_NON_NEGATIVE",            "rcvd_txn_amt_6m",
     "rcvd_txn_amt_6m must be >= 0 - transaction amount cannot be negative",                   100.0, "ACCURACY"),
    (210, "RULE_10_NUMERIC_NON_NEGATIVE",            "sent_txn_amt_3m",
     "sent_txn_amt_3m must be >= 0 - transaction amount cannot be negative",                   100.0, "ACCURACY"),
    (211, "RULE_10_NUMERIC_NON_NEGATIVE",            "rcvd_txn_amt_3m",
     "rcvd_txn_amt_3m must be >= 0 - transaction amount cannot be negative",                   100.0, "ACCURACY"),
    # -- CONSISTENCY - RULE_40: 3m must not exceed 12m --
    (212, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY",     "sent_txn_cnt_3m|sent_txn_cnt_12m",
     "sent_txn_cnt_3m must be <= sent_txn_cnt_12m - 3m window cannot exceed 12m window",       100.0, "CONSISTENCY"),
    (213, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY",     "rcvd_txn_cnt_3m|rcvd_txn_cnt_12m",
     "rcvd_txn_cnt_3m must be <= rcvd_txn_cnt_12m - 3m window cannot exceed 12m window",       100.0, "CONSISTENCY"),
    (214, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY",     "sent_txn_amt_3m|sent_txn_amt_12m",
     "sent_txn_amt_3m must be <= sent_txn_amt_12m - 3m window cannot exceed 12m window",       100.0, "CONSISTENCY"),
    (215, "RULE_40_TEMPORAL_WINDOW_CONSISTENCY",     "rcvd_txn_amt_3m|rcvd_txn_amt_12m",
     "rcvd_txn_amt_3m must be <= rcvd_txn_amt_12m - 3m window cannot exceed 12m window",       100.0, "CONSISTENCY"),
    # -- ACCURACY - RULE_41: amount > 0 when count > 0 --
    (216, "RULE_41_POSITIVE_IF_COUNT_NONZERO",       "sent_txn_cnt_12m|sent_txn_amt_12m",
     "sent_txn_amt_12m must be > 0 when sent_txn_cnt_12m > 0 - zero amount with non-zero count is a data integrity failure",
     100.0, "ACCURACY"),
    (217, "RULE_41_POSITIVE_IF_COUNT_NONZERO",       "rcvd_txn_cnt_12m|rcvd_txn_amt_12m",
     "rcvd_txn_amt_12m must be > 0 when rcvd_txn_cnt_12m > 0 - zero amount with non-zero count is a data integrity failure",
     100.0, "ACCURACY"),
    (218, "RULE_41_POSITIVE_IF_COUNT_NONZERO",       "sent_txn_cnt_3m|sent_txn_amt_3m",
     "sent_txn_amt_3m must be > 0 when sent_txn_cnt_3m > 0 - shortest window is most sensitive to integrity failures",
     100.0, "ACCURACY"),
]
