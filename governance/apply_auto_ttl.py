# Databricks notebook source
# Applies Auto TTL (DELETE ROWS 0 DAYS AFTER _delete_at) to every managed
# table in bronze, silver and gold that has a _delete_at column.
#
# Idempotent — re-applying to a table that already has Auto TTL configured
# updates the setting in place. Tables without _delete_at are skipped.
# Deletion is async; Databricks adds up to ~7 days buffer before permanent removal.

# COMMAND ----------

applied, skipped_error = [], []

tables = spark.sql("""
    SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
    FROM   system.information_schema.columns
    WHERE  table_catalog IN ('bronze', 'silver', 'gold')
      AND  column_name   = '_delete_at'
      AND  table_schema != 'information_schema'
      AND  NOT STARTSWITH(table_name, '_')
""").collect()

for row in tables:
    try:
        spark.sql(f"ALTER TABLE {row.full_name} DELETE ROWS 0 DAYS AFTER _delete_at")
        applied.append(row.full_name)
        print(f"[OK]   {row.full_name}")
    except Exception as exc:
        skipped_error.append((row.full_name, str(exc)))
        print(f"[FAIL] {row.full_name}: {exc}")

# COMMAND ----------

print(f"\nApplied: {len(applied)}  Errors: {len(skipped_error)}")
if skipped_error:
    raise RuntimeError(f"Auto TTL failed for {len(skipped_error)} table(s) — see output above")
