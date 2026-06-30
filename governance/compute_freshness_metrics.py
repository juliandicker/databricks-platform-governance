# Databricks notebook source
# Queries MAX(_updated_at) for every managed table in bronze, silver, and gold
# that has an _updated_at column and writes results to admin.shared.freshness_metrics.
# Idempotent — overwrites the metrics table on every run.
# The admin.shared.retention_compliance view joins this table for freshness data.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

tables = spark.sql("""
    SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
    FROM   system.information_schema.columns
    WHERE  table_catalog IN ('bronze', 'silver', 'gold')
      AND  column_name   = '_updated_at'
      AND  table_schema != 'information_schema'
      AND  NOT STARTSWITH(table_name, '_')
      AND  NOT ENDSWITH(table_name, '_drift_metrics')
      AND  NOT ENDSWITH(table_name, '_profile_metrics')
""").collect()

results = []
for row in tables:
    try:
        max_ts = spark.sql(f"SELECT MAX(_updated_at) AS ts FROM {row.full_name}").collect()[0].ts
        results.append((row.full_name, max_ts, None))
        print(f"[OK]   {row.full_name}: {max_ts}")
    except Exception as exc:
        results.append((row.full_name, None, str(exc)))
        print(f"[FAIL] {row.full_name}: {exc}")

# COMMAND ----------

schema = StructType([
    StructField("full_table_name", StringType()),
    StructField("max_updated_at",  TimestampType()),
    StructField("error",           StringType()),
])

(
    spark.createDataFrame(results, schema)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("admin.shared.freshness_metrics")
)

print(f"\nFreshness metrics written for {len(results)} table(s)")
