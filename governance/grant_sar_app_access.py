# Databricks notebook source
# Grants the SAR app's own service principal the catalog/schema access it
# needs. These used to be dynamic grants in the infra repo's terraform/catalogs.tf,
# keyed on var.sar_app_sp_id — that value only resolves after the app has
# already been deployed once and its SP id copied back into the infra repo's
# tfvars, a chicken-and-egg problem on every fresh workspace (the SAR app SP
# never exists yet on a brand-new metastore). Resolving the SP id here instead,
# via the Apps API, needs nothing handed back from infra at all.
# Idempotent — GRANT is safe to re-run.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
sp_id = w.apps.get(name="platform-sar-app").service_principal_client_id

# COMMAND ----------

# bronze/silver/gold: the app both finds upstream bronze PII via lineage
# tracing and executes the confirmed erasure delete there, same as
# silver/gold. (Once bronze-only: a real erasure request found and confirmed
# a bronze row for deletion, then failed with PERMISSION_DENIED on the actual
# DELETE because bronze granted SELECT only — the two-phase dry-run check
# only exercises SELECT, so it can't catch a missing MODIFY grant before the
# real delete is attempted.)
#
# admin.erasure / admin.access: SELECT+MODIFY — the app writes
# requests/request_items rows when a reviewer confirms an erasure or access
# report.
#
# admin.lineage_cache: SELECT only, not MODIFY — the app reads this cache at
# search time, but writes go through the lineage_cache_refresh job (see
# resources/jobs/lineage_cache_refresh.yml), which runs under its own job
# identity, not the app SP.
#
# admin.shared: EXECUTE only — to call the two hash UDFs
# (hash_subject_ref, hash_row_key) when writing the erasure audit trail.
grants = [
    ("CATALOG bronze", ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY"]),
    ("CATALOG silver", ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY"]),
    ("CATALOG gold", ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY"]),
    ("SCHEMA admin.erasure", ["SELECT", "MODIFY"]),
    ("SCHEMA admin.access", ["SELECT", "MODIFY"]),
    ("SCHEMA admin.lineage_cache", ["SELECT"]),
    ("SCHEMA admin.shared", ["EXECUTE"]),
]

for securable, privileges in grants:
    spark.sql(f"GRANT {', '.join(privileges)} ON {securable} TO `{sp_id}`")
    print(f"[OK] {securable}: {privileges}")

print(f"\nGranted SAR app SP ({sp_id}) access to {len(grants)} securable(s)")
