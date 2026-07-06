-- Incremental refresh for admin.lineage_cache — see create_lineage_cache_tables.sql
-- for the full rationale. Scans only a recent rolling window of
-- system.access.table_lineage/column_lineage and MERGEs into the cache
-- tables, rather than re-scanning full history on every run — this keeps the
-- refresh job's own cost roughly constant over time regardless of how much
-- raw history accumulates in the account-wide system tables.
--
-- :lineage_cache_lookback_days is bound from the governance_daily job's
-- lineage_cache_lookback_days parameter (default 30, see
-- resources/jobs/governance.yml) — safe for routine incremental refreshes;
-- comfortably covers a missed daily run or two. On first deploy against a
-- workspace with lineage history older than that, run this task once
-- manually with a wider value to backfill it — "Run now with different
-- parameters" in the Databricks Jobs UI, or:
--   databricks bundle run governance_daily --params lineage_cache_lookback_days=400
-- Idempotent MERGE — safe to re-run at any window width.
--
-- The in-app "refresh lineage cache now" button (apps/sar_app/lineage.py:
-- refresh_lineage_cache) runs the equivalent MERGE logic directly via the
-- SQL connector rather than this file — Databricks Apps and Jobs deploy to
-- separate filesystems, so the app can't read this file at runtime. Keep
-- the two in sync if the cache tables' schema changes.

MERGE INTO admin.lineage_cache.table_lineage_current AS tgt
USING (
  SELECT
    source_table_full_name,
    target_table_full_name,
    source_table_catalog,
    target_table_catalog,
    COALESCE(entity_type, 'unknown') AS entity_type,
    MAX(event_time) AS last_seen
  FROM system.access.table_lineage
  WHERE source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    AND source_table_full_name NOT RLIKE '_(drift|profile)_metrics$'
    AND target_table_full_name NOT RLIKE '_(drift|profile)_metrics$'
    AND event_date >= current_date() - CAST(:lineage_cache_lookback_days AS INT)
  GROUP BY ALL
) AS src
ON  tgt.source_table_full_name = src.source_table_full_name
AND tgt.target_table_full_name = src.target_table_full_name
WHEN MATCHED THEN UPDATE SET
  source_table_catalog = src.source_table_catalog,
  target_table_catalog = src.target_table_catalog,
  entity_type           = src.entity_type,
  last_seen             = src.last_seen
WHEN NOT MATCHED THEN INSERT (
  source_table_full_name, target_table_full_name,
  source_table_catalog, target_table_catalog, entity_type, last_seen
) VALUES (
  src.source_table_full_name, src.target_table_full_name,
  src.source_table_catalog, src.target_table_catalog, src.entity_type, src.last_seen
);

MERGE INTO admin.lineage_cache.column_lineage_current AS tgt
USING (
  SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name,
    MAX(event_time) AS last_seen
  FROM system.access.column_lineage
  WHERE source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    AND event_date >= current_date() - CAST(:lineage_cache_lookback_days AS INT)
  GROUP BY ALL
) AS src
ON  tgt.source_table_full_name = src.source_table_full_name
AND tgt.source_column_name     = src.source_column_name
AND tgt.target_table_full_name = src.target_table_full_name
AND tgt.target_column_name     = src.target_column_name
WHEN MATCHED THEN UPDATE SET last_seen = src.last_seen
WHEN NOT MATCHED THEN INSERT (
  source_table_full_name, source_column_name, target_table_full_name, target_column_name, last_seen
) VALUES (
  src.source_table_full_name, src.source_column_name, src.target_table_full_name, src.target_column_name, src.last_seen
);
