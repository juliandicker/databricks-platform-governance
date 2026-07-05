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
import uuid
from datetime import date

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from database import DatabricksClient, get_service_principal_token, get_tagged_columns
from erasure import ErasureExecutor, TableErasureTarget, format_delete_sql_pretty, get_vacuum_retention
from idle_watchdog import ensure_started as _ensure_watchdog_started
from idle_watchdog import seconds_remaining as _watchdog_seconds_remaining
from idle_watchdog import stop_app_now as _stop_app_now
from idle_watchdog import touch as _touch_watchdog
from lineage import LineageClient
from lineage_view import STATUS_STYLE as _STATUS_STYLE
from lineage_view import LineageEdge, LineageNode
from lineage_view import render as render_lineage_view
from matching import NameMatcher
from normalise import SearchNormaliser
from search import SARSearcher

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

TAG_MAP: dict[str, str] = {
    "Email":               "class.email_address",
    "Name":                "class.name",
    "Date of Birth":       "class.date_of_birth",
    "Phone":               "class.phone_number",
    "Postcode / Location": "class.location",
}

SEARCHABLE_LAYERS: list[str] = ["silver", "gold"]

#: Fuzzy name matches still show down to the sidebar's own threshold, but
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

EXECUTION_ICON = {"SUCCEEDED": "✅", "FAILED": "❌", "SKIPPED": "⚠️"}


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
        "original_df": original,
        "df": display,
    }


def _node_caption(client: DatabricksClient, full_name: str, row_count: int) -> str:
    """Row count plus the table's actual VACUUM retention, for the lineage map."""
    vacuum_raw, _ = get_vacuum_retention(client, full_name)
    return f"{row_count} row(s) · VACUUM: {vacuum_raw}"


@st.fragment(run_every=1)
def _render_watchdog_controls() -> None:
    """Sidebar countdown to the idle auto-stop, plus a manual stop button."""
    remaining = _watchdog_seconds_remaining()
    minutes, seconds = divmod(remaining, 60)
    st.caption(f"Auto-stop in {minutes:02d}:{seconds:02d} of inactivity")
    if st.button("Stop app now", use_container_width=True):
        _stop_app_now()
        st.success("Stopping the app…")


# ---------------------------------------------------------------------------
# Search pipeline — runs once per Search click, cached in session_state
# ---------------------------------------------------------------------------

def _run_search_pipeline(
    token: str,
    selected: dict[str, str],
    catalog: str,
    fuzzy_threshold: int,
) -> dict:
    """Search, trace lineage, and build review cards. Only called on Search click."""
    db_client = DatabricksClient(token)
    cards: list[dict] = []
    matched_tables: list[dict] = []
    lineage_nodes: dict[str, LineageNode] = {}
    lineage_edges: dict[tuple[str, str], LineageEdge] = {}

    try:
        name_matcher = NameMatcher()
        searcher = SARSearcher(db_client, name_matcher)
        lineage = LineageClient(db_client)

        # 1. Discover tagged columns
        tagged_df = get_tagged_columns(token, catalog)
        if tagged_df.empty:
            return {"cards": cards, "lineage_nodes": [], "lineage_edges": [], "matched_tables": matched_tables}

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

            try:
                result_df = searcher.search(catalog, schema, table, conditions, fuzzy_threshold)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"`{catalog}.{schema}.{table}` — query failed: {exc}")
                continue

            if not result_df.empty:
                full_name = f"{catalog}.{schema}.{table}"
                matched_tables.append({"full_name": full_name, "conditions": conditions})
                tag_labels = ", ".join(
                    next(k for k, v in TAG_MAP.items() if v == tag)
                    for tag in selected if tag in available_tags
                )
                cards.append(_prep_card(full_name, catalog, schema, table, "direct", tag_labels, result_df))
                lineage_nodes[full_name] = LineageNode(
                    full_name, catalog, "direct", row_count=len(result_df),
                    caption=_node_caption(db_client, full_name, len(result_df)),
                )

        progress.empty()
        if not matched_tables:
            return {"cards": cards, "lineage_nodes": [], "lineage_edges": [], "matched_tables": matched_tables}

        matched_full_names = [e["full_name"] for e in matched_tables]

        # 3. Table-level lineage (for the graph)
        with st.spinner("Traversing lineage graph…"):
            try:
                upstream_df = lineage.upstream(matched_full_names)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Upstream lineage query failed: {exc}")
                upstream_df = pd.DataFrame()
            try:
                downstream_df = lineage.downstream(matched_full_names)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Downstream lineage query failed: {exc}")
                downstream_df = pd.DataFrame()

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

        initial_conditions: dict[str, list[tuple[str, str, str]]] = {}
        for entry in matched_tables:
            for cols, clean_val, tag in entry["conditions"]:
                for col in cols:
                    initial_conditions.setdefault(entry["full_name"], []).append((col, tag, clean_val))

        # 4. Upstream search via column lineage — every hop back to bronze,
        # not just the terminal bronze table (an intermediate silver/gold
        # table the searched data was built from is a real copy too).
        # Bronze rows need the app's own SP (SELECT-restricted for users);
        # any other layer can be searched as the calling user, same as a
        # direct match.
        sp_client: DatabricksClient | None = None
        sp_searcher: SARSearcher | None = None
        try:
            try:
                sp_token = get_service_principal_token()
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Could not acquire service-principal token for bronze access: {exc}")
                sp_token = None
            if sp_token:
                sp_client = DatabricksClient(sp_token)
                sp_searcher = SARSearcher(sp_client, name_matcher)

            with st.spinner("Tracing column lineage upstream…"):
                try:
                    col_lineage_df = lineage.column_lineage_upstream(initial_conditions)
                except Exception as exc:  # noqa: BLE001
                    st.warning(f"Column lineage query failed: {exc}")
                    col_lineage_df = pd.DataFrame()

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

                    try:
                        u_df = active_searcher.search(u_cat, u_sch, u_tbl, u_conditions, fuzzy_threshold)
                    except Exception as exc:  # noqa: BLE001
                        st.warning(f"`{u_cat}.{u_sch}.{u_tbl}` — upstream query failed: {exc}")
                        continue
                    if not u_df.empty:
                        full_name = f"{u_cat}.{u_sch}.{u_tbl}"
                        tag_labels = ", ".join(tag for _, _, tag in u_conditions)
                        cards.append(_prep_card(full_name, u_cat, u_sch, u_tbl, "upstream", tag_labels, u_df))
                        lineage_nodes[full_name] = LineageNode(
                            full_name, u_cat, "upstream", row_count=len(u_df),
                            caption=_node_caption(active_client, full_name, len(u_df)),
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

        # 5. Downstream copies search via column lineage (as the user)
        with st.spinner("Tracing column lineage downstream…"):
            try:
                downstream_lineage_df = lineage.column_lineage_downstream(initial_conditions)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Downstream column lineage query failed: {exc}")
                downstream_lineage_df = pd.DataFrame()

        if not downstream_lineage_df.empty:
            downstream_plan = _build_lineage_plan(downstream_lineage_df, "target_table_full_name", "target_column_name")
            d_progress = st.progress(0, text="Searching downstream tables…")
            downstream_items = list(downstream_plan.items())
            for d_i, ((d_cat, d_sch, d_tbl), d_conditions) in enumerate(downstream_items):
                d_progress.progress((d_i + 1) / len(downstream_items), text=f"Searching `{d_cat}.{d_sch}.{d_tbl}`…")
                try:
                    d_df = searcher.search(d_cat, d_sch, d_tbl, d_conditions, fuzzy_threshold)
                except Exception as exc:  # noqa: BLE001
                    st.warning(f"`{d_cat}.{d_sch}.{d_tbl}` — downstream query failed: {exc}")
                    continue
                if not d_df.empty:
                    full_name = f"{d_cat}.{d_sch}.{d_tbl}"
                    tag_labels = ", ".join(tag for _, _, tag in d_conditions)
                    cards.append(_prep_card(full_name, d_cat, d_sch, d_tbl, "downstream", tag_labels, d_df))
                    lineage_nodes[full_name] = LineageNode(
                        full_name, d_cat, "downstream", row_count=len(d_df),
                        caption=_node_caption(db_client, full_name, len(d_df)),
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
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SAR Search", page_icon="🔍", layout="wide")
_ensure_watchdog_started()

# Streamlit's dataframe/data_editor toolbar has no public parameter to disable
# just the "Download as CSV" button (streamlit/streamlit#8402, still unreleased
# as of this writing) — these tables hold PII, so exporting to a local CSV
# bypasses the governed access/masking model entirely. This CSS hides it via
# its accessible name rather than position, since toolbar button order isn't
# guaranteed; it targets internal Streamlit DOM structure, not a public API,
# so it may need updating on a future Streamlit upgrade.
st.markdown(
    """<style>
    button[data-testid="stElementToolbarButton"][aria-label*="Download"] { display: none; }
    </style>""",
    unsafe_allow_html=True,
)

st.title("GDPR Subject Access Request Search")
st.caption(
    "Searches tables using governed `class.*` tags. "
    "Queries run as your user identity — data steward ABAC exemptions apply. "
    "Everything found is pre-selected for erasure — deselect anything that "
    "looks like a false positive before confirming."
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Search Identifiers")
    st.markdown("Select one or more identifier types and enter the values to find.")
    st.divider()

    selected: dict[str, str] = {}
    fuzzy_threshold = 75  # overridden by the slider when Name is enabled

    for label, tag in TAG_MAP.items():
        if st.checkbox(label, value=(label == "Email")):
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
                    label_visibility="collapsed",
                    format="DD/MM/YYYY",
                    help=(
                        "A date picker avoids ambiguous formats (e.g. 01/02/2000 "
                        "meaning different dates in different locales)."
                    ),
                )
                if dob_val is not None:
                    selected[tag] = dob_val.isoformat()
            else:
                val = st.text_input(
                    label,
                    key=f"input_{label}",
                    label_visibility="collapsed",
                    placeholder=label,
                )
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

    st.divider()

    catalog = st.radio(
        "Layer to search",
        options=SEARCHABLE_LAYERS,
        index=SEARCHABLE_LAYERS.index("silver"),
        horizontal=True,
        help="Only tables with class.* tags are included.",
    )

    st.divider()
    search_clicked = st.button("Search", type="primary", use_container_width=True)

    st.divider()
    _render_watchdog_controls()

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
    st.session_state.sar_search_id = str(uuid.uuid4())
    st.session_state.sar_cards = result["cards"]
    st.session_state.sar_lineage_nodes = result["lineage_nodes"]
    st.session_state.sar_lineage_edges = result["lineage_edges"]
    st.session_state.sar_subject_ref = "; ".join(f"{k}={v}" for k, v in selected.items())
    st.session_state.sar_any_matched = bool(result["matched_tables"])
    st.session_state.pop("sar_last_result", None)
    st.session_state.pop("sar_pending_targets", None)
    st.session_state.pop("sar_pending_previews", None)

if "sar_cards" not in st.session_state:
    st.info(
        "Select one or more identifier types in the sidebar, enter values, "
        "then click **Search**.\n\n"
        "**Name matching** strips honorifics (Mr/Mrs/Dr…), expands common "
        "nicknames (Tony → Anthony, Ant…), and ranks results by fuzzy similarity. "
        "Works across split-name tables (first_name + last_name) as well as "
        "full-name columns.\n\n"
        "When multiple identifiers are provided, a row must satisfy **all** of the "
        "identifiers tagged on its own table — a table without one of the selected "
        "identifiers is still searched on the ones it does have, since PII fields "
        "are often split across tables."
    )
    st.stop()

_touch_watchdog()

cards: list[dict] = st.session_state.sar_cards
search_id: str = st.session_state.sar_search_id

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
        editor_key = f"editor_{search_id}_{provenance}_{card['full_name']}"
        st.markdown(f"**{card['full_name']}** — matched on: {card['matched_column_or_tag']}")
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
        st.caption(f"{n_selected} of {len(edited)} row(s) selected for erasure.")

# ---------------------------------------------------------------------------
# Erasure review / confirm
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Erasure Review")

total_selected = sum(int(edited["Erase"].sum()) for _, edited in edited_cards)
total_rows = sum(len(edited) for _, edited in edited_cards)
tables_with_selection = sum(1 for _, edited in edited_cards if edited["Erase"].sum() > 0)

col1, col2 = st.columns([3, 1])
with col1:
    st.metric(
        "Selected for erasure",
        f"{total_selected} / {total_rows}",
        help=f"Across {tables_with_selection} table(s)",
    )
with col2:
    if st.button(
        "Review & confirm erasure",
        type="primary",
        use_container_width=True,
        disabled=total_selected == 0,
    ):
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
