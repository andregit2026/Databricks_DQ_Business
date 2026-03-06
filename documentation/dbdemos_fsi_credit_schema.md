# Schema Summary: dbdemos_fsi_credit

**Catalog:** `databricks_snippets_7405610928938750`
**Schema:** `dbdemos_fsi_credit`
**Generated:** 2026-03-06

---

## Industry & Purpose

**Financial Services (FSI) - Credit Risk / Credit Decisioning**

This is a Databricks Demos (`dbdemos`) reference dataset demonstrating an end-to-end **credit risk ML pipeline** using the **Medallion architecture** (Bronze -> Silver -> Gold -> Feature Store -> ML Training). The target use case is predicting **customer loan default**.

---

## Table Count & Row Counts

| Layer | Table | Type | Rows | Key Purpose |
|---|---|---|---:|---|
| Bronze | `account_bronze` | STREAMING_TABLE | 152,000 | Raw account data (includes CDC ops) |
| Bronze | `credit_bureau_bronze` | STREAMING_TABLE | 49,440 | External credit bureau records |
| Bronze | `customer_bronze` | STREAMING_TABLE | 100,000 | Raw customer ingestion |
| Bronze | `fund_trans_bronze` | STREAMING_TABLE | 43,896 | Raw fund transactions |
| Bronze | `relationship_bronze` | STREAMING_TABLE | 100,000 | Customer-product relationship data |
| Bronze | `telco_bronze` | STREAMING_TABLE | 53,324 | Telecoms/phone billing data |
| Silver | `account_silver` | MATERIALIZED_VIEW | 100,000 | Deduplicated accounts |
| Silver | `customer_silver` | MATERIALIZED_VIEW | 100,000 | Cleansed customers |
| Silver | `fund_trans_silver` | MATERIALIZED_VIEW | 43,896 | Cleansed transactions |
| Gold | `credit_bureau_gold` | MATERIALIZED_VIEW | 49,440 | Aggregated bureau profile |
| Gold | `customer_gold` | MATERIALIZED_VIEW | 100,000 | Full Customer 360 (59 cols) |
| Gold | `fund_trans_gold` | MATERIALIZED_VIEW | 100,000 | Aggregated txn features (3/6/12m) |
| Gold | `telco_gold` | MATERIALIZED_VIEW | 53,326 | Aggregated phone billing behavior |
| ML | `credit_decisioning_features` | MANAGED | 100,000 | Feature Store table (62 features) |
| ML | `credit_risk_train_df` | MANAGED | 13,479 | Labeled training set (`defaulted` column) |
| ML | `feature_definitions` | MANAGED | 61 | Feature metadata registry |
| Ops | `dbdemos_credit_event_logs` | MANAGED | 407 | Pipeline event/audit logs |
| Internal | `__materialization_*` (x11) | MANAGED | - | DLT/SDP internal materialization tables |

**Total user-facing tables: 17** (+ 11 internal DLT tables)

---

## Data Domain Breakdown

| Domain | Source Tables | Key Signals |
|---|---|---|
| Customer demographics | `customer_gold` | age, gender, education, marital_status, is_resident, months_employment, address tenure |
| Financials / relationships | `relationship_bronze` -> `customer_gold` | income, total_assets, debt-to-income ratios, cards/loans/mortgages, overdrafts, deposits, equity, balances |
| Account behavior | `account_bronze` -> `account_silver` | number of accounts, avg_balance, balance_usd, available_balance |
| Credit bureau | `credit_bureau_gold` | credit utilization, overdue amounts, credit prolongations, overdue days |
| Transactions | `fund_trans_gold` | sent/received amounts and counts over 3m/6m/12m windows, distinct counterparty counts |
| Telecoms behavior | `telco_gold` | phone bill amounts, payment delays, pre-paid vs post-paid |
| ML target | `credit_risk_train_df` | `defaulted` (binary: 1 = defaulted, 0 = not defaulted) |

---

## Architecture Pattern

```
Kafka / Auto Loader
        |
    Bronze (Streaming Tables)        <-- 6 raw source domains
        |
    Silver (Materialized Views)      <-- dedup + cleanse
        |
    Gold  (Materialized Views)       <-- aggregated 360-view per customer
        |
    credit_decisioning_features      <-- Feature Store (join of all Gold)
        |
    credit_risk_train_df             <-- labeled subset for ML training
        |
    ML Model (Credit Default Prediction)
```

---

## Key Column Reference

### customer_gold (59 columns)

| Column | Type | Description |
|---|---|---|
| id, cust_id | INT | Customer identifiers |
| first_name, last_name, email | STRING | Personal identifiers |
| gender, education, marital_status | INT/STRING | Demographics |
| join_date, passport_expiry, visa_expiry | DATE | Key dates |
| status, is_resident | INT | Customer status flags |
| months_current_address, months_employment | INT | Stability indicators |
| income_monthly, income_annual | INT | Income metrics |
| tot_assets, tot_rel_bal | INT/DOUBLE | Asset & relationship balance |
| debt_income_rt_w_mortgage, debt_income_rt_wo_mortgage | DOUBLE | Debt-to-income ratios |
| total_cards, total_loans, total_mortgages | INT | Product counts |
| card_balance_amount, loan_balance_amount | DOUBLE | Outstanding balances |
| card_limit_amount, loan_limit_amount | DOUBLE | Credit limits |
| overdraft_balance_amount, overdraft_number | DOUBLE/INT | Overdraft exposure |
| total_deposits_number, total_deposits_amount | INT/DOUBLE | Deposit behavior |
| total_equity_amount, total_UT | DOUBLE | Investments |
| customer_revenue, revenue_12m, revenue_tot | DOUBLE | Revenue metrics |
| customer_score | INT | Internal scoring |
| avg_balance, balance_usd, available_balance_usd | DOUBLE | Account aggregates |
| num_accs | LONG | Number of accounts |

### credit_bureau_gold (16 columns)

| Column | Type | Description |
|---|---|---|
| CUST_ID, SK_ID_CURR | LONG | Customer linkage |
| SK_BUREAU_ID | LONG | Bureau record ID |
| CREDIT_ACTIVE | LONG | Active credit flag |
| CREDIT_CURRENCY | STRING | Currency |
| AMT_CREDIT_MAX_OVERDUE | LONG | Maximum overdue amount |
| AMT_CREDIT_SUM | LONG | Total credit sum |
| AMT_CREDIT_SUM_DEBT | LONG | Total debt |
| AMT_CREDIT_SUM_LIMIT | LONG | Credit limit |
| AMT_CREDIT_SUM_OVERDUE | LONG | Overdue credit sum |
| CNT_CREDIT_PROLONG | LONG | Credit prolongations count |
| CREDIT_DAY_OVERDUE | LONG | Days overdue |
| DAYS_CREDIT | LONG | Days since credit granted |
| DAYS_CREDIT_ENDDATE | LONG | Days until credit end |
| DAYS_ENDDATE_FACT | LONG | Actual end date (days) |

### fund_trans_gold (25 columns)

| Column | Type | Description |
|---|---|---|
| cust_id | INT | Customer ID |
| dist_payer_cnt_12m/6m/3m | LONG | Distinct payers count by window |
| sent_txn_cnt_12m/6m/3m | LONG | Sent transaction count by window |
| sent_txn_amt_12m/6m/3m | DOUBLE | Sent transaction amount by window |
| sent_amt_avg_12m/6m/3m | DOUBLE | Average sent amount by window |
| dist_payee_cnt_12m/6m/3m | LONG | Distinct payees count by window |
| rcvd_txn_cnt_12m/6m/3m | LONG | Received transaction count by window |
| rcvd_txn_amt_12m/6m/3m | DOUBLE | Received transaction amount by window |
| rcvd_amt_avg_12m/6m/3m | DOUBLE | Average received amount by window |

### credit_risk_train_df / credit_decisioning_features (62 columns)

These two tables share the same 62-column structure. `credit_risk_train_df` adds:

| Column | Type | Description |
|---|---|---|
| defaulted | INT | **ML target: 1 = defaulted, 0 = not defaulted** |

Feature categories covered: demographics, relationship/financials, account behavior, telco behavior, fund transfer aggregates (3m/6m/12m), derived ratios (`ratio_txn_amt_3m_12m`, `ratio_txn_amt_6m_12m`).

---

## Key Facts

| Fact | Value |
|---|---|
| Core entity | 100,000 unique customers |
| ML task | Binary classification - predict `defaulted` |
| Training set size | 13,479 labeled examples (~13.5% of 100K) |
| Feature count | 62 features per customer |
| Pipeline type | Lakeflow / DLT Spark Declarative Pipeline |
| Total user-facing rows | ~850K across all main tables |
