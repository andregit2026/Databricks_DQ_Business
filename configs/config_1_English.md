# Config 1 English — Naming Conventions & Coding Guidelines

Applies to: **all Databricks code, SQL, PySpark, and Python notebooks** created in this project.

---

## 1. Character Encoding

All identifiers (table names, column names, schema names, variable names, notebook names)
must use only ASCII characters `A-Z`, `a-z`, `0-9`, and `_`.

### Umlaut / Special Character Substitution Table

| Character | Replace with |
|-----------|-------------|
| `ä`       | `ae`        |
| `ö`       | `oe`        |
| `ü`       | `ue`        |
| `Ä`       | `Ae`        |
| `Ö`       | `Oe`        |
| `Ü`       | `Ue`        |
| `ß`       | `ss`        |
| `é`, `è`, `ê` | `e`    |
| `à`, `â`  | `a`         |
| `î`, `ï`  | `i`         |
| `ô`       | `o`         |
| `û`       | `u`         |
| `ç`       | `c`         |
| `ñ`       | `n`         |
| space ` ` | `_`         |
| `-`       | `_`         |

**Examples:**
- `Überprüfung` → `Ueberpruefung`
- `Straße` → `Strasse`
- `müller` → `mueller`
- `Geschäftsjahr` → `Geschaeftsjahr`

---

## 2. SQL Conventions

### 2a. Case Rules

| Element | Case | Example |
|---------|------|---------|
| Reserved words | `UPPER` | `SELECT`, `FROM`, `WHERE`, `JOIN`, `ON`, `GROUP BY`, `ORDER BY`, `HAVING`, `WITH`, `AS`, `CASE`, `WHEN`, `THEN`, `ELSE`, `END`, `AND`, `OR`, `NOT`, `IN`, `LIKE`, `IS`, `NULL`, `BETWEEN`, `EXISTS`, `UNION`, `ALL`, `DISTINCT`, `LIMIT`, `OFFSET`, `LEFT`, `RIGHT`, `INNER`, `OUTER`, `FULL`, `CROSS`, `INSERT`, `INTO`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `TABLE`, `VIEW`, `SET`, `VALUES` |
| Aggregate & scalar functions | `UPPER` | `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`, `COALESCE()`, `NULLIF()`, `CAST()`, `ROUND()`, `TRIM()`, `UPPER()`, `LOWER()`, `LENGTH()`, `SUBSTR()`, `CONCAT()`, `DATE_ADD()`, `DATE_SUB()`, `CURRENT_DATE()`, `CURRENT_TIMESTAMP()`, `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST()`, `LAST()`, `NVL()`, `IFF()`, `IFNULL()` |
| Window clause keywords | `UPPER` | `OVER`, `PARTITION BY`, `ORDER BY`, `ROWS`, `RANGE`, `UNBOUNDED`, `PRECEDING`, `FOLLOWING`, `CURRENT ROW` |
| Table names | `lower` | `dq_results`, `dq_rule_mappings` |
| Column / field names | `lower` | `rule_no`, `source_table`, `dq_category` |
| Schema names | `lower` | `dbdemos_dq_professional` |
| Catalog names | `lower` | `dbdemos` |
| Aliases | `lower` | `r`, `m`, `g`, `ride_cnt` |
| String literals | as-is | `'ACCURACY'`, `'PASS'` |

### 2b. Formatting Rules

- Each major clause (`SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`) starts on its own line.
- Indent continuation lines by 4 spaces.
- Place commas at the **start** of continuation lines in SELECT lists (leading comma style).
- Align `ON` conditions with the `JOIN` keyword.
- One blank line between separate SQL statements.

**Correct example:**
```sql
SELECT
    r.dq_rule_no
    , COALESCE(m.dq_category, g.dq_category)     AS dq_category
    , COUNT(DISTINCT r.dq_rule_no)                AS total_rules
    , SUM(r.dq_number_relevant_records)           AS relevant_records
    , ROUND(
        SUM(r.dq_number_pos_results)
        / NULLIF(SUM(r.dq_number_relevant_records), 0) * 100
      , 2)                                         AS pass_pct
FROM dbdemos.dbdemos_dq_professional.dq_results r
JOIN dbdemos.dbdemos_dq_professional.dq_rule_mappings m
    ON r.dq_rule_no = m.rule_no
JOIN dbdemos.dbdemos_dq_professional.dq_rules_generic g
    ON m.generic_rule_id = g.generic_rule_id
WHERE r.dq_number_relevant_records > 0
GROUP BY
    COALESCE(m.dq_category, g.dq_category)
ORDER BY dq_category
```

---

## 3. PySpark Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Variable names | `snake_case` | `enriched_df`, `rule_no`, `target_table` |
| DataFrame variables | `snake_case` + `_df` suffix | `dq_df`, `insert_df`, `enriched_df` |
| Constants / config values | `snake_case` (no ALL_CAPS) | `catalog`, `dq_schema` |
| Function names | `snake_case` | `apply_generic_rule()` |
| Column name strings | `lower` | `F.col("rule_no")`, `F.col("dq_category")` |
| Spark SQL f-strings | follow SQL conventions above | `spark.sql(f"SELECT ... FROM {catalog}.{dq_schema}.dq_results r")` |

---

## 4. Python / General Conventions

| Element | Convention |
|---------|-----------|
| Variable names | `snake_case` |
| File / notebook names | `snake_case`, prefixed with sequence number, e.g. `01_apply_dq_rules.py` |
| No special characters | apply substitution table from §1 |
| Widget parameter names | `snake_case` lowercase, e.g. `target_table`, `dq_schema` |
| Print labels in output | Sentence case, English only |

---

## 5. Code Comments

Every logical code block must have a single `#` comment line directly above it describing what the block does.

### Rules

- **Block = a group of related lines that accomplish one step** (e.g. load data, filter records, write a table, define a schema).
- Do not comment every individual line — describe the intent of the block, not each operation.
- Comments must be in **English**, sentence case, concise (one line max per block).
- SQL cells in notebooks: place a `-- comment` line above the first statement of each logical block.

### Example

```python
# Load widget parameters with fallback defaults
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "dev_catalog"

# Read raw taxi data from the Bronze layer
bronze_df = spark.table(f"{catalog}.{schema}.bronze_taxi_raw")

# Remove records with missing pickup or dropoff location
clean_df = bronze_df.filter(
    F.col("pickup_location_id").isNotNull()
    & F.col("dropoff_location_id").isNotNull()
)

# Write cleaned data to the Silver layer
clean_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_taxi_clean")
```
