# Databricks notebook source

# MAGIC %md

# MAGIC # DQ Rule Application - customer_gold

# MAGIC

# MAGIC Loads all active rule mappings for customer_gold, applies each generic rule

# MAGIC dynamically via a dispatcher, and writes the enriched customer_gold_dq table

# MAGIC with a single DQ_RESULT column encoding all rule outcomes.

# MAGIC

# MAGIC DQ_RESULT format: "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"

# MAGIC   1    = rule passed

# MAGIC   0    = rule failed

# MAGIC   NULL = rule not applicable (row status != 1)



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

    target_table = "customer_gold"



try:

    report_date = dbutils.widgets.get("report_date")

except:

    from datetime import date

    report_date = str(date.today())



print(f"DQ Enrichment Parameters:")

print(f"  Catalog:      {catalog}")

print(f"  DQ Schema:    {dq_schema}")

print(f"  Target table: {target_table}")

print(f"  Report date:  {report_date}")



# COMMAND ----------

from pyspark.sql import functions as F

from pyspark.sql.window import Window



# Load source table (copy already in DQ schema)

full_source = f"{catalog}.{dq_schema}.{target_table}"

df = spark.table(full_source)

total_rows = df.count()

print(f"\nLoaded {total_rows} rows from {full_source}")



# COMMAND ----------

# Load active rule mappings for this table

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

print(f"\nActive rule mappings: {len(mappings)}")

for m in mappings:
    print(f"  {m.dq_rule_no}: {m.generic_rule_desc_short} -> {m.source_field}")



# COMMAND ----------

# Generic Rule Dispatcher

# Adds temporary column _DQ_{rule_no} with result: 1=pass, 0=fail, NULL=not applicable



def compute_rule_result(df, rule_no, generic_rule_desc_short, field_name):

    tmp_col = f"_DQ_{rule_no}"

    if generic_rule_desc_short == "RULE_1_DATE_NOT_NULL":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull(), F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_2_STRING_NOT_NULL":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull() & (F.trim(F.col(field_name)) != ""),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_3_NUMERIC_NOT_NULL":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull(), F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_10_NUMERIC_NON_NEGATIVE":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull() & (F.col(field_name) >= 0),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_11_NUMERIC_POSITIVE":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull() & (F.col(field_name) > 0),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_12_DATE_NOT_FUTURE":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull() &

                   (F.col(field_name).cast("date") <= F.current_date()),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_13_DATE_NOT_PAST_LIMIT":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull() &

                   (F.col(field_name).cast("date") >= F.date_sub(F.current_date(), 3650)),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_14_DATE_NOT_FUTURE_LIMIT":
        return df.withColumn(tmp_col,
            F.when(F.col(field_name).isNotNull() &
                   (F.col(field_name).cast("date") <= F.date_add(F.current_date(), 3650)),
                   F.lit(1)).otherwise(F.lit(0)))

    elif generic_rule_desc_short == "RULE_20_ISO_COUNTRY_CODE":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).isNotNull() &

                   F.col(field_name).rlike("^[A-Z]{2}$"),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_21_BOOLEAN_FLAG":

        return df.withColumn(tmp_col,

            F.when(F.col(field_name).cast("integer").isin(0, 1),

                   F.lit(1)).otherwise(F.lit(0)))



    elif generic_rule_desc_short == "RULE_30_UNIQUE_IDENTIFIER":

        # Add result column while _rn_tmp is still in scope, then drop the helper

        w = Window.partitionBy(field_name).orderBy(F.monotonically_increasing_id())

        df = df.withColumn("_rn_tmp", F.row_number().over(w))

        df = df.withColumn(tmp_col,

                           F.when(F.col("_rn_tmp") == 1, F.lit(1))

                            .otherwise(F.lit(0))

                            .cast("integer"))

        return df.drop("_rn_tmp")  # early return, tmp_col already added



    else:

        print(f"  [WARN] Unknown rule: {generic_rule_desc_short} - result set to NULL")

        return df.withColumn(tmp_col, F.lit(None).cast("integer"))



# COMMAND ----------

# Apply all mapped rules (builds one temp column _DQ_{rule_no} per rule)

print("\nApplying rules...")

enriched_df = df



for m in mappings:

    tmp_col = f"_DQ_{m.rule_mapping_id}"

    enriched_df = compute_rule_result(

        df=enriched_df,

        rule_no=m.rule_mapping_id,

        generic_rule_desc_short=m.generic_rule_desc_short,
        field_name=m.source_field
    )

    # Row-level status filter: NULL for rows where status != 1 (inactive customers)

    enriched_df = enriched_df.withColumn(

        tmp_col,

        F.when(F.col("status").cast("integer") != 1, F.lit(None).cast("integer"))

         .otherwise(F.col(tmp_col))

    )
    print(f"  {m.dq_rule_no} ({m.generic_rule_desc_short} -> {m.source_field}): applied")



# COMMAND ----------

# Build single DQ_RESULT column: "RULE_101: 1 | RULE_102: 0 | RULE_103: NULL"

segments = []

for m in mappings:

    tmp_col = f"_DQ_{m.rule_mapping_id}"

    label   = f"RULE_{m.rule_mapping_id}"

    segments.append(

        F.when(F.col(tmp_col).isNull(), F.lit(f"{label}: NULL"))

         .otherwise(F.concat(F.lit(f"{label}: "), F.col(tmp_col).cast("string")))

    )



enriched_df = enriched_df.withColumn("DQ_RESULT", F.concat_ws(" | ", *segments))



# Drop all temporary rule columns

for m in mappings:

    enriched_df = enriched_df.drop(f"_DQ_{m.rule_mapping_id}")



print(f"\nDQ_RESULT column built. Final schema: {len(df.columns)} original + 1 DQ_RESULT")



# COMMAND ----------

# Write enriched DQ table

output_table = f"{catalog}.{dq_schema}.{target_table}_dq"



enriched_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)



out_count = spark.table(output_table).count()

print(f"\nWritten {out_count} rows to {output_table}")



# COMMAND ----------

# Quick overall DQ% — split DQ_RESULT by " | " and count applicable/passing segments

# Uses filter+split (avoids regexp_extract_all capturing-group requirement)

overall = spark.table(output_table).selectExpr(

    "COUNT(*) AS total_rows",

    "SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': [01]$'))) AS total_checks",

    "SUM(size(filter(split(DQ_RESULT, ' [|] '), x -> x rlike ': 1$')))    AS passed_checks",

    "ROUND(SUM(size(filter(split(DQ_RESULT,' [|] '),x->x rlike ': 1$')))*1.0 / "

    "NULLIF(SUM(size(filter(split(DQ_RESULT,' [|] '),x->x rlike ': [01]$'))),0)*100,2) AS overall_dq_pct"

).collect()[0]



print(f"\n{'='*60}")

print(f"DQ SUMMARY - {target_table} - {report_date}")

print(f"{'='*60}")

print(f"  Total rows:      {overall['total_rows']}")

print(f"  Total checks:    {overall['total_checks']}")

print(f"  Passed checks:   {overall['passed_checks']}")

print(f"  Overall DQ%:     {overall['overall_dq_pct']}%")

print(f"{'='*60}")



# COMMAND ----------

summary = (f"SUCCESS: {out_count} rows -> {output_table} | "

           f"Overall DQ%: {overall['overall_dq_pct']}% "

           f"({overall['passed_checks']}/{overall['total_checks']} checks)")

print(summary)

dbutils.notebook.exit(summary)

