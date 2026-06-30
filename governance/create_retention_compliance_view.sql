-- Governance compliance view — exposed via admin.shared for dashboard consumption.
-- Shows every managed table in bronze, silver, and gold with:
--   insertion_status      — COMPLIANT if _inserted_at column is present
--   freshness_status      — COMPLIANT if _updated_at column is present
--   retention_status      — COMPLIANT if _delete_at column is present
--   sla_status            — FRESH / STALE / NEVER_UPDATED / NO_COLUMN / ERROR
--   freshness_sla         — raw SLA string from TBLPROPERTIES e.g. '1h', '7d'
--   freshness_sla_minutes — SLA in minutes; default 1440 (24h) if not set
--   max_updated_at        — actual MAX(_updated_at) from admin.shared.freshness_metrics
--
-- Teams set the SLA per table via:
--   ALTER TABLE <catalog>.<schema>.<table>
--   SET TBLPROPERTIES ('platform.freshness_sla' = '1h');
--
-- Supported units: m (minutes), h (hours), d (days), y (years).
-- Examples: '30m', '4h', '1d', '7d', '30d', '1y', '10y'. Default: '1d'.
-- Table properties are visible in the Unity Catalog Explorer — Details tab.

CREATE OR REPLACE VIEW admin.shared.retention_compliance AS

WITH compliance AS (
  SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS full_table_name,
    MAX(CASE WHEN c.column_name = '_inserted_at' THEN TRUE ELSE FALSE END) AS has_inserted_at,
    MAX(CASE WHEN c.column_name = '_updated_at'  THEN TRUE ELSE FALSE END) AS has_updated_at,
    MAX(CASE WHEN c.column_name = '_delete_at'   THEN TRUE ELSE FALSE END) AS has_delete_at
  FROM system.information_schema.tables t
  LEFT JOIN system.information_schema.columns c
    ON  t.table_catalog = c.table_catalog
    AND t.table_schema  = c.table_schema
    AND t.table_name    = c.table_name
    AND c.column_name   IN ('_inserted_at', '_updated_at', '_delete_at')
  WHERE t.table_catalog IN ('bronze', 'silver', 'gold')
    AND t.table_schema NOT IN ('information_schema')
    AND t.table_type = 'MANAGED'
    AND t.table_name NOT LIKE '\_%'
    AND t.table_name NOT LIKE '%\_drift\_metrics'
    AND t.table_name NOT LIKE '%\_profile\_metrics'
    AND t.table_name NOT LIKE 'event_log_%'
  GROUP BY t.table_catalog, t.table_schema, t.table_name
)

SELECT
  c.table_catalog,
  c.table_schema,
  c.table_name,
  c.full_table_name,
  CASE WHEN c.has_inserted_at THEN 'COMPLIANT' ELSE 'NON-COMPLIANT' END AS insertion_status,
  CASE WHEN c.has_updated_at  THEN 'COMPLIANT' ELSE 'NON-COMPLIANT' END AS freshness_status,
  CASE WHEN c.has_delete_at   THEN 'COMPLIANT' ELSE 'NON-COMPLIANT' END AS retention_status,
  CASE
    WHEN NOT c.has_updated_at                    THEN 'NO_COLUMN'
    WHEN f.error IS NOT NULL                     THEN 'ERROR'
    WHEN f.max_updated_at IS NULL                THEN 'NEVER_UPDATED'
    WHEN TIMESTAMPDIFF(MINUTE, f.max_updated_at, CURRENT_TIMESTAMP())
         > COALESCE(f.freshness_sla_minutes, 1440) THEN 'STALE'
    ELSE                                              'FRESH'
  END AS sla_status,
  c.has_inserted_at,
  c.has_updated_at,
  c.has_delete_at,
  COALESCE(f.freshness_sla, '1d')            AS freshness_sla,
  COALESCE(f.freshness_sla_minutes, 1440)    AS freshness_sla_minutes,
  f.max_updated_at,
  f.error AS freshness_error
FROM compliance c
LEFT JOIN admin.shared.freshness_metrics f ON c.full_table_name = f.full_table_name
ORDER BY
  CASE
    WHEN NOT c.has_inserted_at OR NOT c.has_updated_at OR NOT c.has_delete_at THEN 0
    WHEN CASE
           WHEN NOT c.has_updated_at         THEN 'NO_COLUMN'
           WHEN f.error IS NOT NULL          THEN 'ERROR'
           WHEN f.max_updated_at IS NULL     THEN 'NEVER_UPDATED'
           WHEN TIMESTAMPDIFF(MINUTE, f.max_updated_at, CURRENT_TIMESTAMP())
                > COALESCE(f.freshness_sla_minutes, 1440) THEN 'STALE'
           ELSE 'FRESH'
         END IN ('STALE', 'NEVER_UPDATED', 'ERROR') THEN 0
    ELSE 1
  END,
  c.table_catalog, c.table_schema, c.table_name;

-- Grant read access so the dashboard and all workspace users can query this view.
GRANT SELECT ON VIEW admin.shared.retention_compliance TO `account users`;
