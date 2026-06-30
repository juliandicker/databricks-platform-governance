-- Retention compliance view — exposed via admin.shared for dashboard consumption.
-- Shows every managed table in bronze, silver, and gold with its compliance status:
--   COMPLIANT     = _delete_at column present
--   NON-COMPLIANT = _delete_at column missing
--
-- Teams are responsible for populating _delete_at on every row. Structural
-- compliance (column presence) is checked here; data compliance (NULL rate)
-- requires a separate per-table quality check.

CREATE OR REPLACE VIEW admin.shared.retention_compliance AS

WITH all_tables AS (
  SELECT table_catalog, table_schema, table_name, table_type FROM bronze.information_schema.tables
  UNION ALL
  SELECT table_catalog, table_schema, table_name, table_type FROM silver.information_schema.tables
  UNION ALL
  SELECT table_catalog, table_schema, table_name, table_type FROM gold.information_schema.tables
),

all_columns AS (
  SELECT table_catalog, table_schema, table_name, column_name FROM bronze.information_schema.columns
  UNION ALL
  SELECT table_catalog, table_schema, table_name, column_name FROM silver.information_schema.columns
  UNION ALL
  SELECT table_catalog, table_schema, table_name, column_name FROM gold.information_schema.columns
)

SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS full_table_name,
  CASE WHEN c.column_name IS NOT NULL THEN 'COMPLIANT' ELSE 'NON-COMPLIANT' END AS retention_status,
  c.column_name IS NOT NULL AS has_delete_at
FROM all_tables t
LEFT JOIN all_columns c
  ON  t.table_catalog = c.table_catalog
  AND t.table_schema  = c.table_schema
  AND t.table_name    = c.table_name
  AND c.column_name   = '_delete_at'
WHERE t.table_schema NOT IN ('information_schema')
  AND t.table_type = 'MANAGED'
ORDER BY retention_status DESC, t.table_catalog, t.table_schema, t.table_name;

-- Grant read access so the dashboard and all workspace users can query this view.
GRANT SELECT ON VIEW admin.shared.retention_compliance TO `account users`;
