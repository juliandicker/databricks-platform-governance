-- Governance compliance view — exposed via admin.shared for dashboard consumption.
-- Shows every managed table in bronze, silver, and gold with:
--   retention_status  — COMPLIANT if _delete_at column is present
--   freshness_status  — COMPLIANT if _updated_at column is present
--   max_updated_at    — actual MAX(_updated_at) from admin.shared.freshness_metrics
--
-- Teams are responsible for populating _delete_at and _updated_at on every row.
-- Structural compliance (column presence) is checked via information_schema.
-- Actual freshness (max_updated_at) is computed by the compute_freshness_metrics job task.

CREATE OR REPLACE VIEW admin.shared.retention_compliance AS

WITH compliance AS (
  SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS full_table_name,
    MAX(CASE WHEN c.column_name = '_delete_at'  THEN TRUE ELSE FALSE END) AS has_delete_at,
    MAX(CASE WHEN c.column_name = '_updated_at' THEN TRUE ELSE FALSE END) AS has_updated_at
  FROM system.information_schema.tables t
  LEFT JOIN system.information_schema.columns c
    ON  t.table_catalog = c.table_catalog
    AND t.table_schema  = c.table_schema
    AND t.table_name    = c.table_name
    AND c.column_name   IN ('_delete_at', '_updated_at')
  WHERE t.table_catalog IN ('bronze', 'silver', 'gold')
    AND t.table_schema NOT IN ('information_schema')
    AND t.table_type = 'MANAGED'
    AND NOT STARTSWITH(t.table_name, '_')
    AND NOT ENDSWITH(t.table_name, '_drift_metrics')
    AND NOT ENDSWITH(t.table_name, '_profile_metrics')
  GROUP BY t.table_catalog, t.table_schema, t.table_name
)

SELECT
  c.table_catalog,
  c.table_schema,
  c.table_name,
  c.full_table_name,
  CASE WHEN c.has_delete_at  THEN 'COMPLIANT' ELSE 'NON-COMPLIANT' END AS retention_status,
  CASE WHEN c.has_updated_at THEN 'COMPLIANT' ELSE 'NON-COMPLIANT' END AS freshness_status,
  c.has_delete_at,
  c.has_updated_at,
  f.max_updated_at,
  f.error AS freshness_error
FROM compliance c
LEFT JOIN admin.shared.freshness_metrics f ON c.full_table_name = f.full_table_name
ORDER BY
  CASE WHEN NOT c.has_delete_at OR NOT c.has_updated_at THEN 0 ELSE 1 END,
  c.table_catalog, c.table_schema, c.table_name;

-- Grant read access so the dashboard and all workspace users can query this view.
GRANT SELECT ON VIEW admin.shared.retention_compliance TO `account users`;
