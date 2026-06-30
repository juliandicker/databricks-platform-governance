# Databricks notebook source
# Queries MAX(_updated_at) and the platform.freshness_sla table property for every
# managed table in bronze, silver, and gold that has an _updated_at column.
# Writes results to admin.shared.freshness_metrics.
# Idempotent — overwrites the metrics table on every run.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


def parse_sla_to_minutes(sla_str):
    """Parse a human-readable SLA string to minutes.

    Supported units: m (minutes), h (hours), d (days), y (years, 365.25d).
    Returns 1440 (24 hours) for unrecognised or missing values.
    Examples: '30m'->30, '4h'->240, '7d'->10080, '1y'->525960, '10y'->5259600
    """
    if not sla_str:
        return 1440
    sla_str = sla_str.strip().lower()
    multipliers = {"m": 1, "h": 60, "d": 1440, "y": 525960}
    unit = sla_str[-1] if sla_str else ""
    if unit not in multipliers:
        return 1440
    try:
        return int(sla_str[:-1]) * multipliers[unit]
    except ValueError:
        return 1440


# COMMAND ----------

tables = spark.sql("""
    SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
    FROM   system.information_schema.columns
    WHERE  table_catalog IN ('bronze', 'silver', 'gold')
      AND  column_name   = '_updated_at'
      AND  table_schema != 'information_schema'
""").collect()

results = []
for row in tables:
    try:
        max_ts = spark.sql(f"SELECT MAX(_updated_at) AS ts FROM {row.full_name}").collect()[0].ts

        # Read platform.freshness_sla table property; default to 1d if not set
        props = spark.sql(f"SHOW TBLPROPERTIES {row.full_name}").collect()
        sla_raw = next((r["value"] for r in props if r["key"] == "platform.freshness_sla"), None)
        sla_minutes = parse_sla_to_minutes(sla_raw)

        results.append((row.full_name, max_ts, sla_raw or "1d", sla_minutes, None))
        print(f"[OK]   {row.full_name}: {max_ts}  sla={sla_raw or '1d (default)'}")
    except Exception as exc:
        results.append((row.full_name, None, None, 1440, str(exc)))
        print(f"[FAIL] {row.full_name}: {exc}")

# COMMAND ----------

schema = StructType([
    StructField("full_table_name",       StringType()),
    StructField("max_updated_at",        TimestampType()),
    StructField("freshness_sla",         StringType()),
    StructField("freshness_sla_minutes", IntegerType()),
    StructField("error",                 StringType()),
])

(
    spark.createDataFrame(results, schema)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("admin.shared.freshness_metrics")
)

print(f"\nFreshness metrics written for {len(results)} table(s)")
