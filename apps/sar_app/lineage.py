"""Lineage traversal and display-table construction.

``LineageClient`` performs iterative BFS over the deduplicated lineage-edge
cache in ``admin.lineage_cache`` (``table_lineage_current`` /
``column_lineage_current``), not the raw ``system.access.table_lineage`` /
``column_lineage`` event logs directly. Those account-wide system tables are
append-only — a single pipeline running daily for a year leaves ~365 rows
for the same structural edge — so re-aggregating them from scratch on every
interactive search doesn't scale to a real deployment with many pipelines
and a full year of history (profiled at several seconds per query even
against a small demo workspace). ``admin.lineage_cache`` holds one row per
distinct edge ever observed, refreshed incrementally by
``governance/refresh_lineage_cache.sql`` (scheduled) and
``refresh_lineage_cache`` below (on-demand, same logic) — see
``governance/create_lineage_cache_tables.sql`` for the full rationale and
the staleness trade-off this accepts.

``_relative_time`` is a module-private helper used exclusively by
``build_display_table``.
"""

from __future__ import annotations

import pandas as pd

from database import DatabricksClient


def _relative_time(ts) -> str:
    """Convert *ts* to a human-readable relative string (e.g. 'yesterday')."""
    if ts is None or pd.isna(ts):
        return "unknown"
    try:
        now = pd.Timestamp.utcnow()
        ts_utc = (
            pd.Timestamp(ts).tz_convert("UTC")
            if pd.Timestamp(ts).tzinfo is not None
            else pd.Timestamp(ts).tz_localize("UTC")
        )
        days = (now - ts_utc).days
        if days == 0:
            return "today"
        if days == 1:
            return "yesterday"
        if days < 7:
            return f"{days} days ago"
        if days < 14:
            return "last week"
        if days < 30:
            return f"{days // 7} weeks ago"
        if days < 60:
            return "last month"
        return f"{days // 30} months ago"
    except Exception:  # noqa: BLE001
        return str(ts)


def _is_table_full_name(name: object) -> bool:
    """Return True if *name* looks like a ``catalog.schema.table`` name.

    ``system.access.column_lineage`` can point at non-table entities
    (dashboards, notebook variables, temp views) where the full-name column
    is ``NULL`` or lacks the usual three dot-separated parts.
    """
    return name is not None and str(name).count(".") == 2


def refresh_lineage_cache(client: DatabricksClient, lookback_days: int = 30) -> None:
    """Incrementally MERGE recent system.access.* lineage events into admin.lineage_cache.

    Same logic as ``governance/refresh_lineage_cache.sql`` (the scheduled
    governance_daily task) — duplicated here rather than shared, since
    Databricks Apps and Jobs deploy to separate filesystems and this app
    can't read that file at runtime. Keep the two in sync if the cache
    tables' schema changes. *lookback_days* defaults to 30 for the same
    "safe for routine refreshes, widen for an initial backfill" reasoning
    documented there; *client* should be the app's own service-principal
    client, since data stewards only get read access to admin.lineage_cache
    (see terraform/catalogs.tf: databricks_grants.admin_lineage_cache).
    """
    client.execute(f"""
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
              AND event_date >= current_date() - {int(lookback_days)}
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
        )
    """)

    client.execute(f"""
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
              AND event_date >= current_date() - {int(lookback_days)}
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
        )
    """)


class LineageClient:
    """Traverses ``admin.lineage_cache`` for upstream/downstream tables.

    Uses iterative BFS with a visited set to avoid cycles. Drift and profile
    metrics tables (auto-generated by Lakehouse Monitoring) are excluded at
    cache-refresh time (see ``governance/refresh_lineage_cache.sql``), so
    reads here don't need to filter them out again.
    """

    def __init__(self, client: DatabricksClient) -> None:
        self._client = client

    def upstream(
        self,
        source_tables: list[str],
        # Safety cap on BFS depth, not an expected real-world bound — actual
        # pipelines here are 2-4 hops deep. Guards against runaway traversal
        # over pathologically long or cyclic lineage graphs.
        max_depth: int = 20,
    ) -> pd.DataFrame:
        """Return transitive upstream lineage for *source_tables*.

        Columns: ``matched_table, upstream_table, source_catalog,
        entity_type, last_seen, depth``.
        """
        return self._traverse(source_tables, direction="upstream", max_depth=max_depth)

    def downstream(
        self,
        source_tables: list[str],
        max_depth: int = 20,  # see upstream() for rationale
    ) -> pd.DataFrame:
        """Return transitive downstream lineage for *source_tables*.

        Columns: ``matched_table, downstream_table, target_catalog,
        entity_type, last_seen, depth``.
        """
        return self._traverse(source_tables, direction="downstream", max_depth=max_depth)

    def column_lineage_upstream(
        self,
        initial_conditions: dict[str, list[tuple[str, str, str]]],
        max_depth: int = 10,
    ) -> pd.DataFrame:
        """Trace column lineage upstream, collecting every hop up to bronze.

        Performs a BFS over ``system.access.column_lineage``. Every
        intermediate hop is collected, not just the terminal bronze source —
        e.g. if gold was searched directly, the silver table gold was built
        from is a real copy of the subject's data and must be searched too,
        not skipped as a mere pass-through on the way to bronze. Carries the
        original tag and clean search value through each hop so callers can
        build search conditions without knowing the intermediate path.
        Traversal stops once a hop lands in bronze — nothing upstream of
        bronze is tracked in this platform.

        Args:
            initial_conditions: Maps each matched silver/gold table full name
                to a list of ``(column_name, tag, clean_val)`` tuples.
            max_depth: Safety cap on BFS iterations.

        Returns:
            DataFrame with columns ``source_table_full_name``,
            ``source_column_name``, ``tag``, ``clean_val``. Empty if no
            upstream lineage found.
        """
        if not initial_conditions:
            return pd.DataFrame()

        # frontier: (table_full_name, column_name) -> (tag, clean_val)
        frontier: dict[tuple[str, str], tuple[str, str]] = {
            (table, col): (tag, clean_val)
            for table, cols_info in initial_conditions.items()
            for col, tag, clean_val in cols_info
        }
        visited: set[tuple[str, str]] = set(frontier)
        upstream_rows: list[dict[str, str]] = []

        for _ in range(max_depth):
            if not frontier:
                break

            table_to_cols: dict[str, set[str]] = {}
            for table, col in frontier:
                table_to_cols.setdefault(table, set()).add(col)

            table_in = ", ".join(f"'{t}'" for t in table_to_cols)
            col_in = ", ".join(
                f"'{c}'" for c in {c for cols in table_to_cols.values() for c in cols}
            )

            df = self._client.query(f"""
                SELECT source_table_full_name,
                       source_column_name,
                       target_table_full_name,
                       target_column_name
                FROM   admin.lineage_cache.column_lineage_current
                WHERE  target_table_full_name IN ({table_in})
                  AND  target_column_name     IN ({col_in})
            """)

            if df.empty:
                break

            next_frontier: dict[tuple[str, str], tuple[str, str]] = {}

            for _, row in df.iterrows():
                tgt_key = (row.target_table_full_name, row.target_column_name)
                if tgt_key not in frontier:
                    continue  # cross-table false match from independent IN predicates

                if not _is_table_full_name(row.source_table_full_name):
                    continue

                tag, clean_val = frontier[tgt_key]
                src_key = (row.source_table_full_name, row.source_column_name)

                if src_key in visited:
                    continue
                visited.add(src_key)

                upstream_rows.append({
                    "source_table_full_name": row.source_table_full_name,
                    "source_column_name":     row.source_column_name,
                    "tag":                    tag,
                    "clean_val":              clean_val,
                })

                if not str(row.source_table_full_name).startswith("bronze."):
                    next_frontier[src_key] = (tag, clean_val)

            frontier = next_frontier

        if not upstream_rows:
            return pd.DataFrame()
        return pd.DataFrame(upstream_rows)

    def column_lineage_downstream(
        self,
        initial_conditions: dict[str, list[tuple[str, str, str]]],
        max_depth: int = 10,
    ) -> pd.DataFrame:
        """Trace column lineage downstream from matched tables.

        Performs a BFS over ``system.access.column_lineage`` in the forward
        direction, collecting every hop (same principle as
        ``column_lineage_upstream``): a SAR-relevant copy of a subject's data
        may exist in any derived table, not only at a fixed terminal layer,
        and downstream copies often don't inherit the originating
        ``class.*`` tag. Carries the original tag and clean search value
        through intermediate hops so callers can build search conditions
        without knowing the derived table's schema.

        Args:
            initial_conditions: Maps each matched silver/gold table full name
                to a list of ``(column_name, tag, clean_val)`` tuples.
            max_depth: Safety cap on BFS iterations.

        Returns:
            DataFrame with columns ``target_table_full_name``,
            ``target_column_name``, ``tag``, ``clean_val``. Empty if no
            downstream lineage found.
        """
        if not initial_conditions:
            return pd.DataFrame()

        # frontier: (table_full_name, column_name) -> (tag, clean_val)
        frontier: dict[tuple[str, str], tuple[str, str]] = {
            (table, col): (tag, clean_val)
            for table, cols_info in initial_conditions.items()
            for col, tag, clean_val in cols_info
        }
        visited: set[tuple[str, str]] = set(frontier)
        downstream_rows: list[dict[str, str]] = []

        for _ in range(max_depth):
            if not frontier:
                break

            table_to_cols: dict[str, set[str]] = {}
            for table, col in frontier:
                table_to_cols.setdefault(table, set()).add(col)

            table_in = ", ".join(f"'{t}'" for t in table_to_cols)
            col_in = ", ".join(
                f"'{c}'" for c in {c for cols in table_to_cols.values() for c in cols}
            )

            df = self._client.query(f"""
                SELECT source_table_full_name,
                       source_column_name,
                       target_table_full_name,
                       target_column_name
                FROM   admin.lineage_cache.column_lineage_current
                WHERE  source_table_full_name IN ({table_in})
                  AND  source_column_name     IN ({col_in})
            """)

            if df.empty:
                break

            next_frontier: dict[tuple[str, str], tuple[str, str]] = {}

            for _, row in df.iterrows():
                src_key = (row.source_table_full_name, row.source_column_name)
                if src_key not in frontier:
                    continue  # cross-table false match from independent IN predicates

                if not _is_table_full_name(row.target_table_full_name):
                    continue

                tag, clean_val = frontier[src_key]
                tgt_key = (row.target_table_full_name, row.target_column_name)

                if tgt_key in visited:
                    continue
                visited.add(tgt_key)

                downstream_rows.append({
                    "target_table_full_name": row.target_table_full_name,
                    "target_column_name":     row.target_column_name,
                    "tag":                    tag,
                    "clean_val":              clean_val,
                })
                next_frontier[tgt_key] = (tag, clean_val)

            frontier = next_frontier

        if not downstream_rows:
            return pd.DataFrame()
        return pd.DataFrame(downstream_rows)

    def build_display_table(
        self,
        upstream_df: pd.DataFrame,
        downstream_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge upstream/downstream results into a single display table.

        Columns returned: ``Name, Direction, Type, Last activity``.
        Deduplicated on ``(Name, Direction)`` keeping the shortest path;
        most-recent ``last_seen`` breaks ties.
        """
        parts = []

        if not upstream_df.empty:
            up = upstream_df[["upstream_table", "entity_type", "last_seen", "depth"]].copy()
            up = up.rename(columns={"upstream_table": "Name", "entity_type": "Type"})
            up["Direction"] = "↑ Upstream"
            parts.append(up)

        if not downstream_df.empty:
            dn = downstream_df[
                ["downstream_table", "entity_type", "last_seen", "depth"]
            ].copy()
            dn = dn.rename(columns={"downstream_table": "Name", "entity_type": "Type"})
            dn["Direction"] = "↓ Downstream"
            parts.append(dn)

        if not parts:
            return pd.DataFrame()

        combined = pd.concat(parts, ignore_index=True)
        combined["Last activity"] = combined["last_seen"].apply(_relative_time)
        combined = combined.sort_values(["depth", "last_seen"], ascending=[True, False])
        combined = combined.drop_duplicates(subset=["Name", "Direction"], keep="first")
        combined = combined.sort_values(["depth", "Direction"], ascending=[True, True])
        return combined[["Name", "Direction", "Type", "Last activity"]].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _traverse(
        self,
        source_tables: list[str],
        direction: str,
        max_depth: int,
    ) -> pd.DataFrame:
        if not source_tables:
            return pd.DataFrame()

        all_rows: list[pd.DataFrame] = []
        frontier = list(source_tables)
        visited: set[str] = set(source_tables)
        depth = 1

        while frontier and depth <= max_depth:
            in_list = ", ".join(f"'{t}'" for t in frontier)
            df = self._client.query(self._build_lineage_sql(in_list, direction))
            if df.empty:
                break

            df["depth"] = depth
            all_rows.append(df)

            next_col = "upstream_table" if direction == "upstream" else "downstream_table"
            new = set(df[next_col].tolist()) - visited
            visited.update(new)
            frontier = list(new)
            depth += 1

        return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

    def _build_lineage_sql(self, in_list: str, direction: str) -> str:
        base = f"""
            SELECT
                {{filter_col}}                       AS matched_table,
                {{result_col}}                       AS {{result_alias}},
                {{catalog_col}}                      AS {{catalog_alias}},
                entity_type,
                last_seen
            FROM admin.lineage_cache.table_lineage_current
            WHERE {{filter_col}} IN ({in_list})
            ORDER BY last_seen DESC
        """
        if direction == "upstream":
            return base.format(
                filter_col="target_table_full_name",
                result_col="source_table_full_name",
                result_alias="upstream_table",
                catalog_col="source_table_catalog",
                catalog_alias="source_catalog",
            )
        return base.format(
            filter_col="source_table_full_name",
            result_col="target_table_full_name",
            result_alias="downstream_table",
            catalog_col="target_table_catalog",
            catalog_alias="target_catalog",
        )
