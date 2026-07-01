"""SAR Search Databricks App — entry point and Streamlit UI.

This module is intentionally thin: it owns only the page layout, sidebar
widgets, and the top-level search/lineage orchestration loop.  All business
logic lives in the supporting modules:

    normalise  — SearchNormaliser  (input cleaning per identifier type)
    matching   — NameMatcher       (column grouping, SQL clauses, fuzzy scoring)
    database   — DatabricksClient  (connection lifecycle, cached tag scan)
    search     — SARSearcher       (WHERE-clause orchestration, post-filter)
    lineage    — LineageClient     (BFS lineage traversal, display table)
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import streamlit as st

from database import DatabricksClient, get_tagged_columns
from idle_watchdog import ensure_started as _ensure_watchdog_started
from idle_watchdog import seconds_remaining as _watchdog_seconds_remaining
from idle_watchdog import stop_app_now as _stop_app_now
from idle_watchdog import touch as _touch_watchdog
from lineage import LineageClient
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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _get_token() -> str:
    """Return the caller's Databricks PAT from the request header or env."""
    return (
        st.context.headers.get("x-forwarded-access-token")
        or os.getenv("DATABRICKS_TOKEN", "")
    )


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
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SAR Search", page_icon="🔍", layout="wide")
_ensure_watchdog_started()

st.title("GDPR Subject Access Request Search")
st.caption(
    "Searches tables using governed `class.*` tags. "
    "Queries run as your user identity — data steward ABAC exemptions apply."
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
# Main area — guard clauses
# ---------------------------------------------------------------------------

if not search_clicked:
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

if not selected:
    st.warning("Enter at least one identifier value before searching.")
    st.stop()

token = _get_token()
if not token:
    st.error("No auth token available. Ensure the app is running inside Databricks.")
    st.stop()

_touch_watchdog()

# ---------------------------------------------------------------------------
# Initialise service objects (once per search run)
# ---------------------------------------------------------------------------

_db_client = DatabricksClient(token)

try:
    _name_matcher = NameMatcher()
    _searcher = SARSearcher(_db_client, _name_matcher)
    _lineage = LineageClient(_db_client)

    # -----------------------------------------------------------------------
    # 1. Discover tagged columns
    # -----------------------------------------------------------------------

    with st.spinner(f"Loading tagged column catalogue from {catalog}…"):
        try:
            tagged_df = get_tagged_columns(token, catalog)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to load column tags: {exc}")
            st.stop()

    if tagged_df.empty:
        st.warning(f"No class.*-tagged columns found in {catalog}. Apply governed tags first.")
        st.stop()

    # -----------------------------------------------------------------------
    # 2. Search each table
    # -----------------------------------------------------------------------

    st.subheader(f"{catalog.capitalize()} Layer Results")

    table_list = list(tagged_df.groupby(["table_schema", "table_name"]))
    matched_tables: list[str] = []
    any_results = False

    progress = st.progress(0, text=f"Searching {catalog} tables…")

    for i, ((schema, table), group) in enumerate(table_list):
        progress.progress(
            (i + 1) / len(table_list),
            text=f"Searching `{catalog}.{schema}.{table}`…",
        )

        available_tags: dict[str, list[str]] = {}
        for _, row in group.iterrows():
            available_tags.setdefault(row.tag_name, []).append(row.column_name)

        conditions: list[tuple[list[str], str, str]] = []
        for tag, val in selected.items():
            if tag not in available_tags:
                continue
            cols = available_tags[tag]
            clean_val = SearchNormaliser.for_tag(tag, val)
            conditions.append((cols, clean_val, tag))

        if not conditions:
            continue

        try:
            result_df = _searcher.search(catalog, schema, table, conditions, fuzzy_threshold)
        except Exception as exc:  # noqa: BLE001
            st.warning(f"`{catalog}.{schema}.{table}` — query failed: {exc}")
            continue

        if not result_df.empty:
            any_results = True
            matched_tables.append(f"{catalog}.{schema}.{table}")

            tag_labels = ", ".join(
                next(k for k, v in TAG_MAP.items() if v == tag)
                for tag in selected
                if tag in available_tags
            )
            header = (
                f"**{catalog}.{schema}.{table}** "
                f"— {len(result_df)} row(s) matched on: {tag_labels}"
            )
            with st.expander(header, expanded=True):
                display_df = (
                    result_df.rename(columns={"_match_score": "Match Score %"})
                    if "_match_score" in result_df
                    else result_df
                )
                st.dataframe(display_df, use_container_width=True)

    progress.empty()

    if not any_results:
        st.success("No records found for the provided identifier(s).")

    # -----------------------------------------------------------------------
    # 3. Lineage
    # -----------------------------------------------------------------------

    if matched_tables:
        st.divider()
        st.subheader("Data Lineage")
        st.caption(
            "Full transitive lineage via `system.access.table_lineage` "
            "(1-year rolling window)."
        )

        with st.spinner("Traversing lineage graph…"):
            try:
                upstream_df = _lineage.upstream(matched_tables)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Upstream lineage query failed: {exc}")
                upstream_df = pd.DataFrame()

            try:
                downstream_df = _lineage.downstream(matched_tables)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Downstream lineage query failed: {exc}")
                downstream_df = pd.DataFrame()

        lineage_table = _lineage.build_display_table(upstream_df, downstream_df)

        if lineage_table.empty:
            st.info("No lineage found for the matched tables.")
        else:
            st.dataframe(lineage_table, use_container_width=True, hide_index=True)
            st.caption(
                "These tables **may** contain data derived from or contributing to the "
                "subject's records. Verify manually before including in a SAR response."
            )
finally:
    _db_client.close()
