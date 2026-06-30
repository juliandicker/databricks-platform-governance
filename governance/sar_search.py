# Databricks notebook source
# GDPR Subject Access Request (SAR) search.
# Scans every PII-tagged column in silver and gold for a given identifier
# (e.g. an email address or name) using the class.* governed tags to know
# which columns to search.
#
# Parameters — set when triggering the job manually:
#   identifier   required  the value to find (e.g. "jane.doe@example.com")
#   request_id   optional  unique ID for this SAR run; auto-generated if blank
#
# Results are appended to admin.shared.sar_results (SELECT restricted to
# sg-dbplat-data-stewards). Query after the run:
#   SELECT * FROM admin.shared.sar_results WHERE request_id = '<id>'

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

dbutils.widgets.text("identifier", "")
dbutils.widgets.text("request_id", "")

identifier = dbutils.widgets.get("identifier").strip()
request_id = dbutils.widgets.get("request_id").strip() or uuid.uuid4().hex[:8]

if not identifier:
    raise ValueError("identifier parameter is required — set it when triggering the job")

print(f"SAR search  identifier={identifier!r}  request_id={request_id}")

# COMMAND ----------

# Discover all PII-tagged columns across silver and gold via governed class.* tags

tagged_columns = spark.sql("""
    SELECT 'silver' AS table_catalog, schema_name AS table_schema,
           table_name, column_name, tag_name
    FROM   silver.information_schema.column_tags
    WHERE  tag_name LIKE 'class.%'
    UNION ALL
    SELECT 'gold' AS table_catalog, schema_name AS table_schema,
           table_name, column_name, tag_name
    FROM   gold.information_schema.column_tags
    WHERE  tag_name LIKE 'class.%'
""").collect()

print(f"Found {len(tagged_columns)} PII-tagged column(s) to search\n")

# COMMAND ----------

results = []
requested_at = datetime.now(timezone.utc)

for col in tagged_columns:
    full_table = f"{col.table_catalog}.{col.table_schema}.{col.table_name}"
    column     = col.column_name
    tag        = col.tag_name

    try:
        # Case-insensitive string match; double single-quotes to prevent SQL injection
        safe_id = identifier.replace("'", "''")
        count = spark.sql(
            f"SELECT COUNT(*) AS n FROM {full_table}"
            f" WHERE LOWER(CAST(`{column}` AS STRING)) = LOWER('{safe_id}')"
        ).collect()[0].n

        results.append((request_id, identifier, requested_at,
                        col.table_catalog, col.table_schema, col.table_name,
                        column, tag, int(count), None))

        label = "[MATCH]" if count > 0 else "[     ]"
        print(f"{label} {full_table}.{column} ({tag}): {count} row(s)")

    except Exception as exc:
        results.append((request_id, identifier, requested_at,
                        col.table_catalog, col.table_schema, col.table_name,
                        column, tag, 0, str(exc)))
        print(f"[FAIL]  {full_table}.{column}: {exc}")

# COMMAND ----------

schema = StructType([
    StructField("request_id",    StringType()),
    StructField("identifier",    StringType()),
    StructField("requested_at",  TimestampType()),
    StructField("table_catalog", StringType()),
    StructField("table_schema",  StringType()),
    StructField("table_name",    StringType()),
    StructField("column_name",   StringType()),
    StructField("tag_name",      StringType()),
    StructField("match_count",   LongType()),
    StructField("error",         StringType()),
])

(
    spark.createDataFrame(results, schema)
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("admin.shared.sar_results")
)

# Restrict read access to data stewards — not account users
spark.sql("GRANT SELECT ON TABLE admin.shared.sar_results TO `sg-dbplat-data-stewards`")

matches = [r for r in results if r[8] > 0]
print(f"\n{'='*60}")
print(f"SAR complete.")
print(f"  identifier  : {identifier!r}")
print(f"  request_id  : {request_id}")
print(f"  columns searched : {len(results)}")
print(f"  columns matched  : {len(matches)}")
print(f"\nQuery results:")
print(f"  SELECT * FROM admin.shared.sar_results")
print(f"  WHERE request_id = '{request_id}' AND match_count > 0")
