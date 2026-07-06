"""SAR Search Databricks App — entry point and Streamlit UI.

This module is intentionally thin: it owns only the page layout, sidebar
widgets, and the top-level search/lineage/erasure orchestration.  All
business logic lives in the supporting modules:

    normalise  — SearchNormaliser  (input cleaning per identifier type)
    matching   — NameMatcher       (column grouping, SQL clauses, fuzzy scoring)
    database   — DatabricksClient  (connection lifecycle, cached tag scan)
    search     — SARSearcher       (WHERE-clause orchestration, post-filter)
    lineage    — LineageClient     (BFS lineage traversal, display table, graph)
    erasure    — ErasureExecutor   (confirmed-delete execution, audit trail)

The search pipeline only re-runs on the Search button click; its results are
cached in ``st.session_state`` so that later reruns (checkbox edits in the
review tables, opening the confirm dialog, ...) redraw from cached data
instead of re-querying Databricks.
"""

from __future__ import annotations

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_option_menu import option_menu

import access_review
import erasure_review
from access_report import (
    TableAccessTarget,
    build_report,
    get_all_tagged_columns,
    get_retention_info,
    get_table_comments,
    write_access_request,
)
from database import DatabricksClient, get_service_principal_token, get_tagged_columns
from erasure import ErasureExecutor, TableErasureTarget, format_delete_sql_pretty, get_vacuum_retention
from idle_watchdog import ensure_started as _ensure_watchdog_started
from idle_watchdog import seconds_remaining as _watchdog_seconds_remaining
from idle_watchdog import stop_app_now as _stop_app_now
from idle_watchdog import touch as _touch_watchdog
from lineage import LineageClient, refresh_lineage_cache
from lineage_view import STATUS_STYLE as _STATUS_STYLE
from lineage_view import LineageEdge, LineageNode
from lineage_view import card_container_key
from lineage_view import render as render_lineage_view
from matching import NameMatcher
from normalise import SearchNormaliser
from search import SARSearcher

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

TAG_MAP: dict[str, str] = {
    "Name":                "class.name",
    "Email":               "class.email_address",
    "Date of Birth":       "class.date_of_birth",
    "Phone":               "class.phone_number",
    "Postcode / Location": "class.location",
}

SEARCHABLE_LAYERS: list[str] = ["silver", "gold"]

#: Fuzzy name matches still show down to the match threshold slider, but
#: only ones at/above this score are pre-checked for erasure — anything
#: between the two is a borderline match that needs a human look before
#: being included.
SAFE_AUTO_SELECT_CONFIDENCE = 90

SYSTEM_TABLE_REMINDER = (
    "Databricks system tables (`system.query.history`, `system.access.*`) retain "
    "the executed query text, including search identifiers, for their own "
    "retention window — this is outside this app's control."
)

LEGAL_BASES = [
    "Art. 17(1)(a) — data no longer necessary for its original purpose",
    "Art. 17(1)(b) — consent withdrawn, no other legal basis applies",
    "Art. 17(1)(c) — data subject objects, no overriding legitimate grounds",
    "Art. 17(1)(d) — data was unlawfully processed",
    "Art. 17(1)(e) — erasure required to comply with a legal obligation",
]

PROVENANCE_SECTIONS = [
    ("direct", "Direct Tag Matches", "Found via governed `class.*` tags."),
    (
        "upstream",
        "Upstream Sources — via column lineage",
        "Traced backward via `system.access.column_lineage`, including any "
        "intermediate table the searched data was built from — not just the "
        "ultimate bronze source. Bronze reads run as the app service "
        "principal, which holds SELECT there; other layers run as your "
        "user identity.",
    ),
    (
        "downstream",
        "Downstream Copies — via column lineage",
        "Traced forward via `system.access.column_lineage`, catching derived "
        "copies that don't carry the original `class.*` tags.",
    ),
]

EXECUTION_ICON = {"SUCCEEDED": "✅", "FAILED": "❌", "SKIPPED": "⚠️", "ABORTED": "⚠️"}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _get_token() -> str:
    """Return the caller's Databricks PAT from the request header or env."""
    return (
        st.context.headers.get("x-forwarded-access-token")
        or os.getenv("DATABRICKS_TOKEN", "")
    )


def _get_requester_identity() -> str:
    """Best-effort identity of the calling user, for the erasure audit trail."""
    return (
        st.context.headers.get("x-forwarded-email")
        or st.context.headers.get("x-forwarded-user")
        or "unknown"
    )


def _build_lineage_plan(
    lineage_df: pd.DataFrame,
    table_col: str,
    column_col: str,
) -> dict[tuple[str, str, str], list[tuple[list[str], str, str]]]:
    """Group column lineage results into per-table search conditions.

    Works for both the upstream-to-bronze result (``source_table_full_name``
    / ``source_column_name``) and the downstream result
    (``target_table_full_name`` / ``target_column_name``).
    """
    groups: dict[tuple[str, str, str], dict[str, tuple[str, set[str]]]] = {}
    for _, row in lineage_df.iterrows():
        parts = str(row[table_col]).split(".", 2)
        key = (parts[0], parts[1], parts[2])
        tag, clean_val = str(row.tag), str(row.clean_val)
        tag_map = groups.setdefault(key, {})
        if tag not in tag_map:
            tag_map[tag] = (clean_val, set())
        tag_map[tag][1].add(str(row[column_col]))
    return {
        key: [
            (sorted(col_set), clean_val, tag)
            for tag, (clean_val, col_set) in tag_map.items()
        ]
        for key, tag_map in groups.items()
    }


def _prep_card(
    full_name: str,
    catalog: str,
    schema: str,
    table: str,
    provenance: str,
    matched_column_or_tag: str,
    result_df: pd.DataFrame,
    vacuum_retention: str,
) -> dict:
    """Build a review card: the real table columns (for erasure targeting)
    plus a display copy with a leading ``Erase`` checkbox column."""
    original = result_df.drop(columns=["_match_score"], errors="ignore").copy()
    display = result_df.copy()
    if "_match_score" in display.columns:
        default_selected = display["_match_score"] >= SAFE_AUTO_SELECT_CONFIDENCE
        display = display.rename(columns={"_match_score": "Match Score %"})
    else:
        default_selected = pd.Series(True, index=display.index)
    display.insert(0, "Erase", default_selected)

    return {
        "full_name": full_name,
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "provenance": provenance,
        "matched_column_or_tag": matched_column_or_tag,
        "vacuum_retention": vacuum_retention,
        "original_df": original,
        "df": display,
    }


def _render_case_bar(
    subject_name: str,
    matched_on: str,
    tables: int,
    catalogs: int,
    records_found: int,
    selected: int,
    total: int,
) -> None:
    """Subject summary + running erasure-selection count, styled like a case file
    header rather than a bare st.metric — native Streamlit has no stat-row/pill
    primitive, so this is a small inline HTML block rather than a new component
    (unlike the lineage map, nothing here needs live DOM measurement)."""
    def stat(value: int, label: str) -> str:
        return f'''<div style="display:flex;flex-direction:column;gap:2px;">
          <span style="font-size:1.25rem;font-weight:700;font-variant-numeric:tabular-nums;color:inherit;">{value}</span>
          <span style="font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;color:#8b93a7;">{label}</span>
        </div>'''

    st.markdown(
        f'''<div style="display:flex;align-items:center;gap:28px;flex-wrap:wrap;
                    background:rgba(127,140,160,0.08);border:1px solid rgba(127,140,160,0.18);
                    border-radius:10px;padding:16px 22px;margin:4px 0 14px;color:inherit;">
          <div style="display:flex;flex-direction:column;gap:2px;">
            <span style="font-size:1.125rem;font-weight:700;">{subject_name}</span>
            <span style="font-size:0.75rem;color:#8b93a7;">{matched_on}</span>
          </div>
          {stat(tables, "Tables")}
          {stat(catalogs, "Catalogs")}
          {stat(records_found, "Records Found")}
          <div style="flex:1;"></div>
          <div style="display:flex;align-items:center;gap:8px;background:rgba(127,140,160,0.12);
                      border:1px solid rgba(127,140,160,0.22);border-radius:999px;padding:10px 18px;
                      font-size:0.875rem;white-space:nowrap;">
            Selected for erasure
            <strong style="color:#ff4d5e;font-size:1rem;font-variant-numeric:tabular-nums;">{selected} / {total}</strong>
          </div>
        </div>''',
        unsafe_allow_html=True,
    )


def _render_timing_breakdown(timings: list[dict], wall_seconds: float) -> None:
    """Collapsed-by-default breakdown of where a search spent its time.

    Populated by ``_mark`` calls throughout ``_run_search_pipeline`` — phase
    totals (tag catalogue scan, table lineage, column lineage traces) plus
    one entry per individual table query, so a single slow table doesn't
    just get averaged away into an opaque phase total. ``wall_seconds`` is
    the pipeline's actual elapsed time; the phase totals below can sum to
    more than that, since the table/column lineage traces now run
    concurrently rather than one after another.
    """
    if not timings:
        return

    df = pd.DataFrame(timings)
    query_seconds = df["seconds"].sum()

    with st.expander(f"⏱️ Search performance breakdown — {wall_seconds:.1f}s wall time", expanded=False):
        if query_seconds > wall_seconds * 1.1:
            st.caption(
                f"Phases below sum to {query_seconds:.1f}s of query time — more than the "
                f"{wall_seconds:.1f}s wall time because the lineage queries run concurrently."
            )
        summary = (
            df.groupby("phase", as_index=False)
            .agg(total_seconds=("seconds", "sum"), queries=("seconds", "count"))
            .sort_values("total_seconds", ascending=False)
        )
        summary["total_seconds"] = summary["total_seconds"].round(2)
        st.dataframe(summary, hide_index=True, use_container_width=True)

        per_table = df[df["table"].notna()].sort_values("seconds", ascending=False).head(15)
        if not per_table.empty:
            st.caption("Slowest individual table queries:")
            per_table = per_table[["phase", "table", "seconds"]].copy()
            per_table["seconds"] = per_table["seconds"].round(2)
            st.dataframe(per_table, hide_index=True, use_container_width=True)


@st.fragment(run_every=1)
def _render_watchdog_controls() -> None:
    """Sidebar countdown to the idle auto-stop, plus a manual stop button."""
    remaining = _watchdog_seconds_remaining()
    minutes, seconds = divmod(remaining, 60)
    st.caption(f"Auto-stop in {minutes:02d}:{seconds:02d} of inactivity")
    if st.button("Stop app now", use_container_width=True):
        _stop_app_now()
        st.success("Stopping the app…")


def _render_lineage_cache_refresh() -> None:
    """Sidebar 'refresh now' escape hatch for admin.lineage_cache.

    The cache normally refreshes on governance_daily's schedule (see
    governance/refresh_lineage_cache.sql); this is for a steward who knows a
    relevant pipeline ran very recently and doesn't want to wait for the next
    scheduled refresh before an urgent search.
    """
    if st.button("🔄 Refresh lineage cache now", use_container_width=True):
        try:
            sp_token = get_service_principal_token()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Could not acquire service-principal token: {exc}")
            return
        sp_client = DatabricksClient(sp_token)
        try:
            with st.spinner("Refreshing lineage cache…"):
                refresh_lineage_cache(sp_client)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Lineage cache refresh failed: {exc}")
        else:
            st.success("Lineage cache refreshed.")
        finally:
            sp_client.close()


# ---------------------------------------------------------------------------
# Search pipeline — runs once per Search click, cached in session_state
# ---------------------------------------------------------------------------

def _mark(timings: list[dict], phase: str, start: float, table: str | None = None) -> None:
    """Record one timing entry (phase total, or a single table's query) for the
    performance breakdown expander — see ``_render_timing_breakdown``."""
    timings.append({"phase": phase, "table": table, "seconds": time.perf_counter() - start})


def _run_search_pipeline(
    token: str,
    selected: dict[str, str],
    catalog: str,
    fuzzy_threshold: int,
) -> dict:
    """Search, trace lineage, and build review cards. Only called on Search click."""
    pipeline_start = time.perf_counter()
    db_client = DatabricksClient(token)
    cards: list[dict] = []
    matched_tables: list[dict] = []
    lineage_nodes: dict[str, LineageNode] = {}
    lineage_edges: dict[tuple[str, str], LineageEdge] = {}
    timings: list[dict] = []

    try:
        name_matcher = NameMatcher()
        searcher = SARSearcher(db_client, name_matcher)

        # 1. Discover tagged columns
        t0 = time.perf_counter()
        tagged_df = get_tagged_columns(token, catalog)
        _mark(timings, "Tag catalogue scan", t0)
        if tagged_df.empty:
            return {
                "cards": cards, "lineage_nodes": [], "lineage_edges": [],
                "matched_tables": matched_tables, "timings": timings,
                "wall_seconds": time.perf_counter() - pipeline_start,
            }

        # 2. Search each table (direct tag matches)
        table_list = list(tagged_df.groupby(["table_schema", "table_name"]))
        progress = st.progress(0, text=f"Searching {catalog} tables…")

        for i, ((schema, table), group) in enumerate(table_list):
            progress.progress((i + 1) / len(table_list), text=f"Searching `{catalog}.{schema}.{table}`…")

            available_tags: dict[str, list[str]] = {}
            for _, row in group.iterrows():
                available_tags.setdefault(row.tag_name, []).append(row.column_name)

            conditions: list[tuple[list[str], str, str]] = []
            for tag, val in selected.items():
                if tag not in available_tags:
                    continue
                clean_val = SearchNormaliser.for_tag(tag, val)
                conditions.append((available_tags[tag], clean_val, tag))

            if not conditions:
                continue

            full_name_probe = f"{catalog}.{schema}.{table}"
            t_table = time.perf_counter()
            try:
                result_df = searcher.search(catalog, schema, table, conditions, fuzzy_threshold)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"`{catalog}.{schema}.{table}` — query failed: {exc}")
                _mark(timings, "Direct search", t_table, full_name_probe)
                continue
            _mark(timings, "Direct search", t_table, full_name_probe)

            if not result_df.empty:
                full_name = f"{catalog}.{schema}.{table}"
                matched_tables.append({"full_name": full_name, "conditions": conditions})
                tag_labels = ", ".join(
                    next(k for k, v in TAG_MAP.items() if v == tag)
                    for tag in selected if tag in available_tags
                )
                vacuum_raw, _ = get_vacuum_retention(db_client, full_name)
                cards.append(_prep_card(
                    full_name, catalog, schema, table, "direct", tag_labels, result_df, vacuum_raw
                ))
                lineage_nodes[full_name] = LineageNode(
                    full_name, catalog, "direct", row_count=len(result_df),
                    caption=f"{len(result_df)} row(s)",
                )

        progress.empty()
        if not matched_tables:
            return {
                "cards": cards, "lineage_nodes": [], "lineage_edges": [],
                "matched_tables": matched_tables, "timings": timings,
                "wall_seconds": time.perf_counter() - pipeline_start,
            }

        matched_full_names = [e["full_name"] for e in matched_tables]

        initial_conditions: dict[str, list[tuple[str, str, str]]] = {}
        for entry in matched_tables:
            for cols, clean_val, tag in entry["conditions"]:
                for col in cols:
                    initial_conditions.setdefault(entry["full_name"], []).append((col, tag, clean_val))

        # SP token used both for the lineage-cache reads below (data stewards
        # only get read access to admin.lineage_cache, not account users —
        # the admin catalog deliberately has no blanket account-users grant,
        # see terraform/catalogs.tf) and for the upstream bronze search loop
        # further down. Fetched once, reused for both.
        try:
            sp_token = get_service_principal_token()
        except Exception as exc:  # noqa: BLE001
            st.warning(f"Could not acquire service-principal token: {exc}")
            sp_token = None

        upstream_df = downstream_df = col_lineage_df = downstream_lineage_df = pd.DataFrame()

        if sp_token is None:
            st.warning("Skipping lineage tracing — no service-principal token available.")
        else:
            # 3+4a+5a. Table-level lineage (for the graph) and the two column-lineage
            # traces (for finding more copies) are independent of each other — the
            # table-level pair only needs matched_full_names, the column-lineage
            # pair only needs initial_conditions, both already known here. Each of
            # these admin.lineage_cache scans can cost some time alone, so running
            # all four concurrently turns their *sum* into roughly their *max*
            # instead. Each needs its own connection — a single DatabricksClient's
            # SQL connection isn't safe to share across threads.
            def _fetch_lineage(fn):
                start = time.perf_counter()
                client = DatabricksClient(sp_token)
                try:
                    result = fn(LineageClient(client))
                finally:
                    client.close()
                return result, time.perf_counter() - start

            with st.spinner("Traversing lineage graph…"):
                with ThreadPoolExecutor(max_workers=4) as pool:
                    fut_table_up = pool.submit(_fetch_lineage, lambda lc: lc.upstream(matched_full_names))
                    fut_table_down = pool.submit(_fetch_lineage, lambda lc: lc.downstream(matched_full_names))
                    fut_col_up = pool.submit(_fetch_lineage, lambda lc: lc.column_lineage_upstream(initial_conditions))
                    fut_col_down = pool.submit(_fetch_lineage, lambda lc: lc.column_lineage_downstream(initial_conditions))

                    try:
                        upstream_df, elapsed = fut_table_up.result()
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"Upstream lineage query failed: {exc}")
                        upstream_df, elapsed = pd.DataFrame(), 0.0
                    timings.append({"phase": "Table lineage (upstream)", "table": None, "seconds": elapsed})

                    try:
                        downstream_df, elapsed = fut_table_down.result()
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"Downstream lineage query failed: {exc}")
                        downstream_df, elapsed = pd.DataFrame(), 0.0
                    timings.append({"phase": "Table lineage (downstream)", "table": None, "seconds": elapsed})

                    try:
                        col_lineage_df, elapsed = fut_col_up.result()
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"Column lineage query failed: {exc}")
                        col_lineage_df, elapsed = pd.DataFrame(), 0.0
                    timings.append({"phase": "Column lineage trace (upstream)", "table": None, "seconds": elapsed})

                    try:
                        downstream_lineage_df, elapsed = fut_col_down.result()
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"Downstream column lineage query failed: {exc}")
                        downstream_lineage_df, elapsed = pd.DataFrame(), 0.0
                    timings.append({"phase": "Column lineage trace (downstream)", "table": None, "seconds": elapsed})

        traversed_color = _STATUS_STYLE["traversed"]["color"]
        for _, row in upstream_df.iterrows():
            lineage_nodes.setdefault(
                row.upstream_table,
                LineageNode(row.upstream_table, row.upstream_table.split(".")[0], "traversed",
                            caption="traversed · no PII match"),
            )
            lineage_edges[(row.upstream_table, row.matched_table)] = LineageEdge(
                row.upstream_table, row.matched_table, color=traversed_color
            )
        for _, row in downstream_df.iterrows():
            lineage_nodes.setdefault(
                row.downstream_table,
                LineageNode(row.downstream_table, row.downstream_table.split(".")[0], "traversed",
                            caption="traversed · no PII match"),
            )
            lineage_edges[(row.matched_table, row.downstream_table)] = LineageEdge(
                row.matched_table, row.downstream_table, color=traversed_color
            )

        # 4b. Upstream search via column lineage — every hop back to bronze,
        # not just the terminal bronze table (an intermediate silver/gold
        # table the searched data was built from is a real copy too).
        # Bronze rows need the app's own SP (SELECT-restricted for users);
        # any other layer can be searched as the calling user, same as a
        # direct match. Reuses the sp_token already fetched above for the
        # lineage-cache reads.
        sp_client: DatabricksClient | None = None
        sp_searcher: SARSearcher | None = None
        try:
            if sp_token:
                sp_client = DatabricksClient(sp_token)
                sp_searcher = SARSearcher(sp_client, name_matcher)

            if not col_lineage_df.empty:
                upstream_plan = _build_lineage_plan(col_lineage_df, "source_table_full_name", "source_column_name")
                u_progress = st.progress(0, text="Searching upstream tables…")
                upstream_items = list(upstream_plan.items())
                for u_i, ((u_cat, u_sch, u_tbl), u_conditions) in enumerate(upstream_items):
                    u_progress.progress((u_i + 1) / len(upstream_items), text=f"Searching `{u_cat}.{u_sch}.{u_tbl}`…")

                    if u_cat == "bronze":
                        if sp_searcher is None:
                            st.warning(f"`{u_cat}.{u_sch}.{u_tbl}` — skipped, no service-principal token for bronze access.")
                            continue
                        active_searcher, active_client = sp_searcher, sp_client
                    else:
                        active_searcher, active_client = searcher, db_client

                    full_name_probe = f"{u_cat}.{u_sch}.{u_tbl}"
                    t_table = time.perf_counter()
                    try:
                        u_df = active_searcher.search(u_cat, u_sch, u_tbl, u_conditions, fuzzy_threshold)
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"`{u_cat}.{u_sch}.{u_tbl}` — upstream query failed: {exc}")
                        _mark(timings, "Upstream table search", t_table, full_name_probe)
                        continue
                    _mark(timings, "Upstream table search", t_table, full_name_probe)
                    if not u_df.empty:
                        full_name = f"{u_cat}.{u_sch}.{u_tbl}"
                        tag_labels = ", ".join(tag for _, _, tag in u_conditions)
                        vacuum_raw, _ = get_vacuum_retention(active_client, full_name)
                        cards.append(_prep_card(
                            full_name, u_cat, u_sch, u_tbl, "upstream", tag_labels, u_df, vacuum_raw
                        ))
                        lineage_nodes[full_name] = LineageNode(
                            full_name, u_cat, "upstream", row_count=len(u_df),
                            caption=f"{len(u_df)} row(s)",
                        )
                        cols_used = ", ".join(sorted({c for cols, _, _ in u_conditions for c in cols}))
                        for matched in matched_full_names:
                            lineage_edges[(full_name, matched)] = LineageEdge(
                                full_name, matched, color=_STATUS_STYLE["upstream"]["color"], label=cols_used
                            )
                u_progress.empty()
        finally:
            if sp_client is not None:
                sp_client.close()

        # 5b. Downstream copies search via column lineage (as the user)
        if not downstream_lineage_df.empty:
            downstream_plan = _build_lineage_plan(downstream_lineage_df, "target_table_full_name", "target_column_name")
            d_progress = st.progress(0, text="Searching downstream tables…")
            downstream_items = list(downstream_plan.items())
            for d_i, ((d_cat, d_sch, d_tbl), d_conditions) in enumerate(downstream_items):
                d_progress.progress((d_i + 1) / len(downstream_items), text=f"Searching `{d_cat}.{d_sch}.{d_tbl}`…")
                full_name_probe = f"{d_cat}.{d_sch}.{d_tbl}"
                t_table = time.perf_counter()
                try:
                    d_df = searcher.search(d_cat, d_sch, d_tbl, d_conditions, fuzzy_threshold)
                except Exception as exc:  # noqa: BLE001
                    st.warning(f"`{d_cat}.{d_sch}.{d_tbl}` — downstream query failed: {exc}")
                    _mark(timings, "Downstream table search", t_table, full_name_probe)
                    continue
                _mark(timings, "Downstream table search", t_table, full_name_probe)
                if not d_df.empty:
                    full_name = f"{d_cat}.{d_sch}.{d_tbl}"
                    tag_labels = ", ".join(tag for _, _, tag in d_conditions)
                    vacuum_raw, _ = get_vacuum_retention(db_client, full_name)
                    cards.append(_prep_card(
                        full_name, d_cat, d_sch, d_tbl, "downstream", tag_labels, d_df, vacuum_raw
                    ))
                    lineage_nodes[full_name] = LineageNode(
                        full_name, d_cat, "downstream", row_count=len(d_df),
                        caption=f"{len(d_df)} row(s)",
                    )
                    cols_used = ", ".join(sorted({c for cols, _, _ in d_conditions for c in cols}))
                    for matched in matched_full_names:
                        lineage_edges[(matched, full_name)] = LineageEdge(
                            matched, full_name, color=_STATUS_STYLE["downstream"]["color"], label=cols_used
                        )
            d_progress.empty()

        return {
            "cards": cards,
            "lineage_nodes": list(lineage_nodes.values()),
            "lineage_edges": list(lineage_edges.values()),
            "matched_tables": matched_tables,
            "timings": timings,
            "wall_seconds": time.perf_counter() - pipeline_start,
        }
    finally:
        db_client.close()


# ---------------------------------------------------------------------------
# Confirm dialog
# ---------------------------------------------------------------------------

@st.dialog("Confirm erasure request", width="large")
def _render_confirm_dialog() -> None:
    targets: list[TableErasureTarget] = st.session_state.get("sar_pending_targets", [])
    previews: list[tuple[TableErasureTarget, str, str]] = st.session_state.get("sar_pending_previews", [])
    total_rows = sum(len(t.selected_rows) for t in targets)

    st.write(f"This will submit **{total_rows}** row(s) across **{len(targets)}** table(s) for erasure.")
    st.caption(SYSTEM_TABLE_REMINDER)

    for target, method, sql_pretty in previews:
        with st.expander(f"View SQL to be executed — `{target.full_name}`", expanded=False):
            st.caption(f"Row targeting method: {method.replace('_', ' ')}")
            st.code(sql_pretty, language="sql", wrap_lines=True)

    legal_basis = st.selectbox("Legal basis (GDPR Art. 17(1))", LEGAL_BASES)
    confirm_text = st.text_input("Type DELETE to confirm", placeholder="DELETE")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.session_state.pop("sar_pending_targets", None)
            st.session_state.pop("sar_pending_previews", None)
            st.rerun()
    with col2:
        if st.button(
            "Submit erasure request",
            type="primary",
            use_container_width=True,
            disabled=confirm_text.strip().upper() != "DELETE",
        ):
            _touch_watchdog()
            sp_token = get_service_principal_token()
            exec_client = DatabricksClient(sp_token)
            try:
                executor = ErasureExecutor(exec_client)
                with st.spinner("Executing erasure…"):
                    request_id, results = executor.run(
                        subject_ref=st.session_state.get("sar_subject_ref", ""),
                        requested_by=_get_requester_identity(),
                        legal_basis=legal_basis,
                        targets=targets,
                    )
            finally:
                exec_client.close()
            st.session_state.sar_last_result = (request_id, results)
            st.session_state.pop("sar_pending_targets", None)
            st.session_state.pop("sar_pending_previews", None)
            st.rerun()


# ---------------------------------------------------------------------------
# Access report dialog (GDPR Art. 15)
# ---------------------------------------------------------------------------

@st.dialog("Generate Art. 15 access report", width="large")
def _render_access_report_dialog() -> None:
    review_tables: list[dict] = st.session_state.get("sar_access_review_tables", [])
    retention_df: pd.DataFrame = st.session_state.get("sar_access_retention_df", pd.DataFrame())
    comments: dict = st.session_state.get("sar_access_comments", {})

    st.write(
        f"This will generate a subject-facing HTML report covering "
        f"**{len(review_tables)}** table(s). Review which columns to include "
        f"before generating — a column outside the searched identifier(s) may "
        f"belong to a different person (e.g. a shared record)."
    )
    st.caption(SYSTEM_TABLE_REMINDER)

    search_id = st.session_state.get("sar_search_id", "")
    targets: list[TableAccessTarget] = []
    for rt in review_tables:
        full_name = rt["full_name"]
        with st.expander(f"`{full_name}` — {len(rt['rows'])} row(s)", expanded=True):
            comment_info = comments.get(full_name, {})
            if comment_info.get("table_comment"):
                st.caption(f"Table description: {comment_info['table_comment']}")

            checklist_df = pd.DataFrame({
                "Column": rt["all_columns"],
                "Tag": [rt["tag_by_col"].get(c, "—") for c in rt["all_columns"]],
                "Include": [rt["default_include"][c] for c in rt["all_columns"]],
            })
            edited = st.data_editor(
                checklist_df,
                key=f"access_cols_{search_id}_{full_name}",
                hide_index=True,
                use_container_width=True,
                disabled=["Column", "Tag"],
                column_config={
                    "Include": st.column_config.CheckboxColumn(
                        "Include",
                        help="Unchecked columns are governed personal-data columns outside "
                        "this request's searched identifier(s) — verify before including.",
                    )
                },
            )
            included = list(edited.loc[edited["Include"], "Column"])
            redacted = [c for c in rt["tag_by_col"] if c not in included]

            targets.append(TableAccessTarget(
                full_name=full_name,
                provenance=rt["provenance"],
                matched_column_or_tag=rt["matched_column_or_tag"],
                rows=rt["rows"],
                included_columns=included,
                redacted_columns=redacted,
            ))

    purpose = st.text_area(
        "Purpose of processing (required — included in the report, not stored in the audit trail)",
        placeholder="e.g. Providing and administering travel booking services for the data subject.",
    )
    can_confirm = bool(purpose.strip())

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True, key="access_cancel"):
            for key in ("sar_access_review_tables", "sar_access_retention_df", "sar_access_comments"):
                st.session_state.pop(key, None)
            st.rerun()
    with col2:
        if st.button(
            "Generate report",
            type="primary",
            use_container_width=True,
            disabled=not can_confirm,
        ):
            _touch_watchdog()
            sp_token = get_service_principal_token()
            exec_client = DatabricksClient(sp_token)
            try:
                with st.spinner("Generating report…"):
                    request_id = write_access_request(
                        exec_client,
                        subject_ref=st.session_state.get("sar_subject_ref", ""),
                        requested_by=_get_requester_identity(),
                        targets=targets,
                    )
                    html_report = build_report(
                        request_id=request_id,
                        subject_display=st.session_state.get("sar_subject_name", ""),
                        requested_by=_get_requester_identity(),
                        purpose=purpose.strip(),
                        generated_at=datetime.now(timezone.utc),
                        targets=targets,
                        retention_df=retention_df,
                        comments=comments,
                    )
            finally:
                exec_client.close()
            st.session_state.sar_access_last_result = (request_id, html_report)
            for key in ("sar_access_review_tables", "sar_access_retention_df", "sar_access_comments"):
                st.session_state.pop(key, None)
            st.rerun()


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SAR Search", page_icon="🔍", layout="wide")
_ensure_watchdog_started()

# Streamlit's dataframe/data_editor toolbar has no public parameter to disable
# just the "Download as CSV" button (streamlit/streamlit#8402, still unreleased
# as of this writing) — these tables hold PII, so exporting to a local CSV
# bypasses the governed access/masking model entirely. This CSS hides it via
# its accessible name rather than position, since toolbar button order isn't
# guaranteed. Verified against Streamlit 1.58.0: the aria-label lives on the
# nested button[data-testid="stBaseButton-elementToolbar"], not on the
# button[data-testid="stElementToolbarButton"] wrapper (that testid is on a
# div, not the button) — this targets internal DOM structure, not a public
# API, so it may need re-verifying on a future Streamlit upgrade.
st.markdown(
    """<style>
    button[data-testid="stBaseButton-elementToolbar"][aria-label="Download as CSV"] { display: none; }
    </style>""",
    unsafe_allow_html=True,
)

# Tighter overall spacing — Streamlit's defaults (measured on this app: 96px/160px
# top/bottom page padding, 16px gap between every stacked element and between
# columns, 32px margin around every st.divider()) read as sparse for a page meant
# to be scanned quickly rather than read top-to-bottom. Values below are direct
# overrides of those measured defaults, not guesses.
st.markdown(
    """<style>
    .block-container { padding-top: 2rem !important; padding-bottom: 2.5rem !important; }
    [data-testid="stVerticalBlock"] { gap: 0.5rem !important; }
    [data-testid="stHorizontalBlock"] { gap: 0.75rem !important; }
    hr { margin: 0.75rem 0 !important; }
    h1 { font-size: 2rem !important; }
    h3 { font-size: 1.25rem !important; }
    </style>""",
    unsafe_allow_html=True,
)

# A sidebar option_menu rather than st.navigation/st.Page — this keeps each
# extra page a minimal, isolated addition (call the new module, st.stop())
# instead of restructuring the existing single-script search flow below
# into multiple page-callables.
with st.sidebar:
    page = option_menu(
        None,
        ["Search & Erase", "Review Erasure Requests", "Review Access Requests"],
        icons=["search", "clock-history", "file-earmark-text"],
        default_index=0,
        key="sar_page",
    )
if page == "Review Erasure Requests":
    token = _get_token()
    if not token:
        st.error("No auth token available. Ensure the app is running inside Databricks.")
        st.stop()
    review_client = DatabricksClient(token)
    try:
        erasure_review.render(review_client)
    finally:
        review_client.close()
    st.stop()
if page == "Review Access Requests":
    token = _get_token()
    if not token:
        st.error("No auth token available. Ensure the app is running inside Databricks.")
        st.stop()
    review_client = DatabricksClient(token)
    try:
        access_review.render(review_client)
    finally:
        review_client.close()
    st.stop()

st.title("GDPR Subject Access Request Search")
st.caption(
    "Searches tables using governed `class.*` tags. "
    "Queries run as your user identity — data steward ABAC exemptions apply. "
    "Everything found is pre-selected for erasure — deselect anything that "
    "looks like a false positive before confirming."
)

with st.sidebar:
    _render_watchdog_controls()
    _render_lineage_cache_refresh()

# ---------------------------------------------------------------------------
# Search options — laid out across the main page's width rather than the
# sidebar, so all identifier types are visible at once instead of requiring
# scrolling through a long single-column list.
# ---------------------------------------------------------------------------

st.subheader(
    "Search Identifiers",
    help=(
        "Enter one or more identifier values below, then click **Search**.\n\n"
        "**Name matching** strips honorifics (Mr/Mrs/Dr…), expands common "
        "nicknames (Tony → Anthony, Ant…), and ranks results by fuzzy similarity. "
        "Works across split-name tables (first_name + last_name) as well as "
        "full-name columns.\n\n"
        "When multiple identifiers are provided, a row must satisfy **all** of the "
        "identifiers tagged on its own table — a table without one of the selected "
        "identifiers is still searched on the ones it does have, since PII fields "
        "are often split across tables."
    ),
)
st.caption("Enter one or more identifier values to search — leave any you don't need blank.")

selected: dict[str, str] = {}
fuzzy_threshold = 75  # overridden by the slider below

identifier_cols = st.columns(len(TAG_MAP))
for col, (label, tag) in zip(identifier_cols, TAG_MAP.items()):
    with col:
        if label == "Date of Birth":
            today = date.today()
            try:
                min_dob = today.replace(year=today.year - 100)
            except ValueError:
                # today is 29 Feb and year-100 isn't a leap year
                min_dob = today.replace(year=today.year - 100, day=28)
            dob_val = st.date_input(
                label,
                value=None,
                min_value=min_dob,
                max_value=today,
                key=f"input_{label}",
                format="DD/MM/YYYY",
                help=(
                    "A date picker avoids ambiguous formats (e.g. 01/02/2000 "
                    "meaning different dates in different locales)."
                ),
            )
            if dob_val is not None:
                selected[tag] = dob_val.isoformat()
        else:
            val = st.text_input(label, key=f"input_{label}", placeholder=label)
            if val.strip():
                selected[tag] = val.strip()
        if label == "Name":
            fuzzy_threshold = st.slider(
                "Match threshold",
                min_value=50,
                max_value=100,
                value=75,
                step=5,
                help=(
                    "Minimum WRatio score (0–100) for a name to be considered a match. "
                    "Lower values catch more variants; higher values require closer "
                    "spelling. Nickname expansion runs regardless of this setting."
                ),
            )

# Layer + Search reuse two of the same identifier columns (re-entering a
# column after its first use just appends further down it) rather than a
# new row — every column but Name's is otherwise empty below its input,
# since only Name grows a second widget (the threshold slider above).
with identifier_cols[3]:
    catalog = st.radio(
        "Layer to search",
        options=SEARCHABLE_LAYERS,
        index=SEARCHABLE_LAYERS.index("silver"),
        horizontal=True,
        help="Only tables with class.* tags are included.",
    )
with identifier_cols[4]:
    st.write("")  # vertical spacer so the button aligns with the radio, not its label
    search_clicked = st.button("Search", type="primary", use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Main area — run the search pipeline only on Search click; everything else (checkbox
# edits, opening the confirm dialog) reruns this script but redraws from the
# cached session_state results instead of re-querying Databricks.
# ---------------------------------------------------------------------------

if search_clicked:
    if not selected:
        st.warning("Enter at least one identifier value before searching.")
        st.stop()

    token = _get_token()
    if not token:
        st.error("No auth token available. Ensure the app is running inside Databricks.")
        st.stop()

    _touch_watchdog()

    with st.spinner(f"Loading tagged column catalogue from {catalog}…"):
        result = _run_search_pipeline(token, selected, catalog, fuzzy_threshold)
    friendly_selected = {
        next(k for k, v in TAG_MAP.items() if v == tag): val for tag, val in selected.items()
    }
    matched_on = ", ".join(friendly_selected)
    if "Name" in friendly_selected:
        matched_on += f" · fuzzy ≥{fuzzy_threshold}"

    st.session_state.sar_search_id = str(uuid.uuid4())
    st.session_state.sar_cards = result["cards"]
    st.session_state.sar_lineage_nodes = result["lineage_nodes"]
    st.session_state.sar_lineage_edges = result["lineage_edges"]
    st.session_state.sar_search_timings = result["timings"]
    st.session_state.sar_search_wall_seconds = result["wall_seconds"]
    st.session_state.sar_subject_ref = "; ".join(f"{k}={v}" for k, v in selected.items())
    st.session_state.sar_subject_name = friendly_selected.get("Name") or next(iter(friendly_selected.values()))
    st.session_state.sar_matched_on = matched_on
    st.session_state.sar_any_matched = bool(result["matched_tables"])
    # Which class.* tags this search actually looked for — used by the access-
    # report dialog to default-include columns tagged with a searched
    # identifier and default-exclude other governed columns for review.
    st.session_state.sar_selected_tags = list(selected.keys())
    st.session_state.pop("sar_last_result", None)
    st.session_state.pop("sar_pending_targets", None)
    st.session_state.pop("sar_pending_previews", None)
    st.session_state.pop("sar_access_last_result", None)
    st.session_state.pop("sar_access_review_tables", None)
    st.session_state.pop("sar_access_retention_df", None)
    st.session_state.pop("sar_access_comments", None)

if "sar_cards" not in st.session_state:
    st.stop()

_touch_watchdog()

cards: list[dict] = st.session_state.sar_cards
search_id: str = st.session_state.sar_search_id

_render_timing_breakdown(
    st.session_state.get("sar_search_timings", []),
    st.session_state.get("sar_search_wall_seconds", 0.0),
)

if not st.session_state.sar_any_matched:
    st.success("No records found for the provided identifier(s).")
    st.stop()

# ---------------------------------------------------------------------------
# Lineage map
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Data Lineage")
st.caption(
    "Bronze → silver → gold traversal via `system.access.table_lineage` and "
    "`system.access.column_lineage` (1-year rolling window). Solid nodes matched "
    "the subject; dashed nodes were traversed but held no PII match."
)
if st.session_state.sar_lineage_nodes:
    lineage_html, lineage_height = render_lineage_view(
        st.session_state.sar_lineage_nodes, st.session_state.sar_lineage_edges
    )
    components.html(lineage_html, height=lineage_height, scrolling=False)
else:
    st.info("No lineage found for the matched tables.")
st.caption(SYSTEM_TABLE_REMINDER)

# ---------------------------------------------------------------------------
# Review cards, grouped by provenance
# ---------------------------------------------------------------------------

edited_cards: list[tuple[dict, pd.DataFrame]] = []

for provenance, heading, caption in PROVENANCE_SECTIONS:
    section_cards = [c for c in cards if c["provenance"] == provenance]
    if not section_cards:
        continue

    st.divider()
    st.subheader(heading)
    st.caption(caption)

    for card in section_cards:
        style = _STATUS_STYLE[card["provenance"]]
        container_key = card_container_key(provenance, card["full_name"])

        # Colors match the lineage map (same STATUS_STYLE), so a card's left
        # accent is a visual pointer back to the same-colored node/edges there.
        # Requires streamlit==1.58.0 (pinned in requirements.txt) — st.container's
        # key parameter, which produces the .st-key-<key> class this targets,
        # isn't available on older versions (confirmed via a prior TypeError
        # against this app's previously-deployed 1.38.0).
        st.markdown(
            f'<style>.st-key-{container_key} {{ '
            f'border-left: 4px solid {style["color"]} !important; border-radius: 8px; }}</style>',
            unsafe_allow_html=True,
        )

        with st.container(key=container_key, border=True):
            header = st.empty()

            editor_key = f"editor_{search_id}_{provenance}_{card['full_name']}"
            edited = st.data_editor(
                card["df"],
                key=editor_key,
                hide_index=True,
                use_container_width=True,
                disabled=[c for c in card["df"].columns if c != "Erase"],
                column_config={
                    "Erase": st.column_config.CheckboxColumn(
                        "Erase", help="Selected rows are included in the erasure request."
                    )
                },
            )
            edited_cards.append((card, edited))
            n_selected = int(edited["Erase"].sum())

            badge_html = (
                f'<span style="display:inline-flex;align-items:center;gap:6px;font-size:0.6875rem;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:0.03em;padding:4px 10px;border-radius:999px;'
                f'color:{style["color"]};background:{style["bg"]};">'
                f'<span style="width:6px;height:6px;border-radius:50%;background:{style["color"]};"></span>'
                f'{style["badge"]}</span>'
                if style["badge"] else ""
            )
            chip_html = (
                f'<span style="font-size:0.75rem;color:#8b93a7;background:rgba(127,140,160,0.12);'
                f'border:1px solid rgba(127,140,160,0.22);border-radius:5px;padding:3px 9px;'
                f'font-family:ui-monospace,monospace;">matched on: {card["matched_column_or_tag"]}</span>'
            )
            vacuum_chip_html = (
                f'<span style="font-size:0.75rem;color:#8b93a7;background:rgba(127,140,160,0.12);'
                f'border:1px solid rgba(127,140,160,0.22);border-radius:5px;padding:3px 9px;'
                f'font-family:ui-monospace,monospace;">VACUUM: {card["vacuum_retention"]}</span>'
            )
            header.markdown(
                f'''<div style="display:flex;align-items:center;justify-content:space-between;
                            flex-wrap:wrap;gap:10px;margin-bottom:10px;">
                  <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
                    <span style="font-family:ui-monospace,monospace;font-size:0.9375rem;font-weight:600;">
                      {card["full_name"]}
                    </span>
                    {badge_html}
                    {chip_html}
                    {vacuum_chip_html}
                  </div>
                  <span style="font-size:0.8125rem;color:#8b93a7;white-space:nowrap;">
                    {n_selected} of {len(edited)} selected
                  </span>
                </div>''',
                unsafe_allow_html=True,
            )

# ---------------------------------------------------------------------------
# Erasure review / confirm
# ---------------------------------------------------------------------------

st.divider()

total_selected = sum(int(edited["Erase"].sum()) for _, edited in edited_cards)
total_rows = sum(len(edited) for _, edited in edited_cards)
tables_count = len(cards)
catalogs_count = len({c["catalog"] for c in cards})

_render_case_bar(
    st.session_state.sar_subject_name,
    st.session_state.sar_matched_on,
    tables_count,
    catalogs_count,
    total_rows,
    total_selected,
    total_rows,
)

ACTION_ACCESS_REPORT = "Generate an Art. 15 access report"
ACTION_ERASURE = "Process an Art. 17 erasure request"

st.write("**What would you like to do with these results?**")
action_choice = st.radio(
    "Action",
    options=[ACTION_ACCESS_REPORT, ACTION_ERASURE],
    label_visibility="collapsed",
    horizontal=True,
)

continue_disabled = action_choice == ACTION_ERASURE and total_selected == 0
if action_choice == ACTION_ERASURE:
    st.caption(
        "Select at least one row above before continuing."
        if continue_disabled
        else "Covers only the rows selected above."
    )
else:
    st.caption(
        "Covers every row found above, regardless of erasure selection — "
        "a subject's right of access isn't conditioned on what's marked for erasure."
    )

if st.button("Continue", type="primary", use_container_width=True, disabled=continue_disabled):
    if action_choice == ACTION_ERASURE:
        targets = [
            TableErasureTarget(
                full_name=card["full_name"],
                provenance=card["provenance"],
                matched_column_or_tag=card["matched_column_or_tag"],
                selected_rows=card["original_df"].loc[edited.index[edited["Erase"]]],
            )
            for card, edited in edited_cards
            if edited["Erase"].sum() > 0
        ]

        sp_token = get_service_principal_token()
        preview_client = DatabricksClient(sp_token)
        previews = []
        try:
            executor = ErasureExecutor(preview_client)
            for target in targets:
                try:
                    method, _sql, clauses = executor.build_delete_sql(target)
                    sql_pretty = format_delete_sql_pretty(target.full_name, clauses)
                except Exception as exc:  # noqa: BLE001
                    method, sql_pretty = "unknown", f"-- failed to build preview: {exc}"
                previews.append((target, method, sql_pretty))
        finally:
            preview_client.close()

        st.session_state.sar_pending_targets = targets
        st.session_state.sar_pending_previews = previews
        _render_confirm_dialog()
    else:
        sp_token = get_service_principal_token()
        meta_client = DatabricksClient(sp_token)
        full_names = [card["full_name"] for card in cards]
        searched_tags = set(st.session_state.get("sar_selected_tags", []))
        try:
            with st.spinner("Loading column tags, retention, and table descriptions…"):
                retention_df = get_retention_info(meta_client, full_names)
                comments = get_table_comments(meta_client, full_names)

                review_tables = []
                for card in cards:
                    tagged = get_all_tagged_columns(meta_client, card["full_name"])
                    tag_by_col = {col: tag for tag, cols in tagged.items() for col in cols}
                    all_columns = list(card["original_df"].columns)
                    default_include = {
                        col: (tag_by_col.get(col) in searched_tags) or (col not in tag_by_col)
                        for col in all_columns
                    }
                    review_tables.append({
                        "full_name": card["full_name"],
                        "provenance": card["provenance"],
                        "matched_column_or_tag": card["matched_column_or_tag"],
                        "rows": card["original_df"],
                        "all_columns": all_columns,
                        "tag_by_col": tag_by_col,
                        "default_include": default_include,
                    })
        finally:
            meta_client.close()

        st.session_state.sar_access_review_tables = review_tables
        st.session_state.sar_access_retention_df = retention_df
        st.session_state.sar_access_comments = comments
        _render_access_report_dialog()

# ---------------------------------------------------------------------------
# Last erasure result
# ---------------------------------------------------------------------------

if "sar_last_result" in st.session_state:
    request_id, results = st.session_state.sar_last_result
    st.divider()
    st.subheader("Erasure Request Result")
    st.success(f"Erasure request `{request_id}` processed — recorded in `admin.erasure`.")
    for r in results:
        icon = EXECUTION_ICON.get(r.execution_status, "•")
        st.write(
            f"{icon} **{r.full_name}** — {r.rows_deleted}/{r.rows_selected} row(s) deleted. "
            f"Physical purge expected by ~{r.estimated_purge_by.strftime('%Y-%m-%d')} "
            f"(VACUUM retention: {r.vacuum_retention_raw})."
        )
        if r.error_message:
            st.caption(f"⚠ {r.error_message}")

# ---------------------------------------------------------------------------
# Last access report result
# ---------------------------------------------------------------------------

if "sar_access_last_result" in st.session_state:
    request_id, html_report = st.session_state.sar_access_last_result
    st.divider()
    st.subheader("Access Report Result")
    st.success(f"Access report `{request_id}` generated — recorded in `admin.access`.")
    st.caption(
        "Open the downloaded file in a browser tab and use Print → Save as PDF "
        "for a handoff-ready document."
    )
    st.download_button(
        "Download report (.html)",
        data=html_report,
        file_name=f"access-report-{request_id}.html",
        mime="text/html",
    )
