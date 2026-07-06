-- Deduplicated "latest edge" lineage cache — owned by the data_platform_admins
-- team (see terraform/data-product-teams.tf + terraform/catalogs.tf
-- databricks_grants.admin_lineage_cache).
--
-- system.access.table_lineage/column_lineage are account-wide, append-only
-- event logs: a single pipeline running daily for a year leaves ~365 rows for
-- the same structural edge. The SAR app used to re-aggregate those raw logs
-- from scratch on every search (scanning up to a year of history per query,
-- profiled at several seconds each even against a small demo workspace) —
-- that doesn't scale to a real deployment with many pipelines and a full
-- year of history. These two tables hold one row per edge that has ever been
-- observed, refreshed incrementally by governance/refresh_lineage_cache.sql
-- (governance_daily job, plus an in-app "refresh lineage cache now" button
-- for stewards who need fresher data before an urgent search) instead of
-- recomputed per search. Query these instead of system.access.* directly —
-- see apps/sar_app/lineage.py.
--
-- Staleness trade-off: a brand-new lineage edge won't appear here until the
-- next refresh (scheduled daily, or triggered manually). Lineage structure —
-- which tables feed which — changes on the order of days/weeks in practice,
-- not intraday, so this is an accepted trade-off against re-scanning a
-- year of account-wide event logs on every interactive search.

CREATE TABLE IF NOT EXISTS admin.lineage_cache.table_lineage_current (
  source_table_full_name STRING    COMMENT 'catalog.schema.table on the upstream side of the edge.',
  target_table_full_name STRING    COMMENT 'catalog.schema.table on the downstream side of the edge.',
  source_table_catalog   STRING,
  target_table_catalog   STRING,
  entity_type             STRING   COMMENT 'From system.access.table_lineage; UNKNOWN if not reported.',
  last_seen               TIMESTAMP COMMENT 'Most recent event_time this edge was observed in system.access.table_lineage.'
) COMMENT 'One row per distinct table-lineage edge ever observed — deduplicated, refreshed incrementally. Query both directions from here: WHERE target_table_full_name IN (...) for upstream, WHERE source_table_full_name IN (...) for downstream.';

CREATE TABLE IF NOT EXISTS admin.lineage_cache.column_lineage_current (
  source_table_full_name STRING,
  source_column_name     STRING,
  target_table_full_name STRING,
  target_column_name     STRING,
  last_seen               TIMESTAMP COMMENT 'Most recent event_time this edge was observed in system.access.column_lineage.'
) COMMENT 'One row per distinct column-lineage edge ever observed — deduplicated, refreshed incrementally. Query both directions from here: WHERE target_table_full_name/target_column_name IN (...) for upstream, WHERE source_table_full_name/source_column_name IN (...) for downstream.';
