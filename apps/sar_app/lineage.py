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
distinct edge ever observed, refreshed incrementally by the
``lineage_cache_refresh`` Databricks Job (``governance/refresh_lineage_cache.sql``
— see ``governance/create_lineage_cache_tables.sql`` for the full rationale
and the staleness trade-off this accepts). ``trigger_lineage_cache_refresh``
below triggers that same job on demand rather than re-implementing its SQL —
the MERGE logic lives in exactly one place.

``_relative_time`` is a module-private helper used exclusively by
``build_display_table``.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.jobs import RunResultState

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


def trigger_lineage_cache_refresh(lookback_days: int = 30, timeout_seconds: int = 180) -> tuple[bool, str]:
    """Trigger the ``lineage_cache_refresh`` job now and wait for it to finish.

    The MERGE logic that actually refreshes admin.lineage_cache lives in
    exactly one place — governance/refresh_lineage_cache.sql, owned by the
    standalone ``lineage_cache_refresh`` job (resources/jobs/lineage_cache_refresh.yml)
    that governance_daily's schedule also triggers via a run_job_task. This
    function triggers the same job on demand rather than re-implementing its
    SQL in Python, so there is never a second copy to keep in sync.

    Requires the ``LINEAGE_CACHE_REFRESH_JOB_ID`` env var, resolved via
    app.yaml's ``valueFrom: 'lineage-cache-refresh-job'`` binding to the job
    resource declared in resources/apps/sar.yml — like
    ``DATABRICKS_WAREHOUSE_ID``, this only resolves inside a deployed bundle,
    not under local ``apps run-local`` (see docs/sar-app.md).

    Runs as the app's own identity (``WorkspaceClient()``'s ambient auth,
    same as ``get_service_principal_token()`` elsewhere in this app) —
    the app resource declaration grants it CAN_MANAGE_RUN on this specific
    job, so it never needs direct read/write access to admin.lineage_cache
    or system.access.* itself; the job's own configured identity does the
    actual work.

    Returns ``(success, message)`` for the caller to show the reviewer.
    """
    job_id = os.getenv("LINEAGE_CACHE_REFRESH_JOB_ID")
    if not job_id:
        return False, (
            "LINEAGE_CACHE_REFRESH_JOB_ID is not set — this only resolves inside "
            "a deployed app, not under local `apps run-local`."
        )

    w = WorkspaceClient()
    run = w.jobs.run_now(
        job_id=int(job_id),
        job_parameters={"lineage_cache_lookback_days": str(int(lookback_days))},
    )
    try:
        result = run.result(timeout=timedelta(seconds=timeout_seconds))
    except TimeoutError:
        return False, (
            f"Refresh job triggered (run_id={run.run_id}) but didn't finish within "
            f"{timeout_seconds}s — check the Jobs UI for its current status."
        )
    except OperationFailed as exc:
        return False, f"Refresh job (run_id={run.run_id}) failed to reach a terminal state: {exc}"

    if result.state.result_state == RunResultState.SUCCESS:
        return True, f"Lineage cache refreshed ({result.run_page_url})."
    return False, (
        f"Lineage cache refresh job finished with status "
        f"{result.state.result_state}: {result.state.state_message} ({result.run_page_url})"
    )


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
