"""
GDPR Subject Access Request (SAR) Search — Databricks App.

Searches all class.*-tagged columns in silver for a given identifier.
Queries run as the calling user (user-authorization mode), so data steward
ABAC exemptions apply automatically — unmasked values are returned.

Lineage section uses system.access.table_lineage to show which gold/bronze
tables were derived from any silver table that contained a match.
"""

import os
import streamlit as st
import pandas as pd
from databricks import sql as dbsql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TAG_MAP = {
    "Email":               "class.email_address",
    "Name":                "class.name",
    "Date of Birth":       "class.date_of_birth",
    "Phone":               "class.phone_number",
    "Postcode / Location": "class.location",
}

# Tags that use LIKE containment search instead of exact equality.
# Names may be stored without honorifics, in different order, or split
# across first_name/last_name columns — partial matching is more reliable.
LIKE_TAGS = {"class.name", "class.location"}

# Honorifics to strip before a name search so "Mr Tom Hill" → "Tom Hill"
_HONORIFICS = {"mr", "mrs", "ms", "miss", "dr", "prof", "sir", "mx", "rev", "lord", "lady"}


def clean_search_value(tag: str, val: str) -> str:
    """Strip honorifics from name searches; leave other values unchanged."""
    if tag != "class.name":
        return val
    parts = val.strip().split()
    if parts and parts[0].lower().rstrip(".") in _HONORIFICS:
        parts = parts[1:]
    return " ".join(parts)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_token() -> str:
    """User-auth token injected by Databricks App runtime; falls back to env for local dev."""
    return (
        st.context.headers.get("x-forwarded-access-token")
        or os.getenv("DATABRICKS_TOKEN", "")
    )


def _connect(token: str):
    host = os.getenv("DATABRICKS_HOST", "").removeprefix("https://")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
    )


def _query(token: str, sql: str) -> pd.DataFrame:
    with _connect(token) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall_arrow().to_pandas()


@st.cache_data(ttl=300, show_spinner=False)
def get_tagged_columns(_token: str) -> pd.DataFrame:
    """Return all class.*-tagged columns in silver. Cached for 5 minutes."""
    return _query(_token, """
        SELECT schema_name  AS table_schema,
               table_name,
               column_name,
               tag_name
        FROM   silver.information_schema.column_tags
        WHERE  tag_name LIKE 'class.%'
    """)


def search_silver_table(
    token: str,
    schema: str,
    table: str,
    conditions: list[tuple[str, str, str]],  # (column, value, tag)
) -> pd.DataFrame:
    """
    Run a SELECT on silver.<schema>.<table> ANDing all (column, value, tag) conditions.
    Name and location tags use LIKE containment; all others use exact equality.
    Returns up to 100 matching rows.
    """
    clauses = []
    for col, val, tag in conditions:
        safe = val.replace("'", "''")
        if tag in LIKE_TAGS:
            clauses.append(f"LOWER(CAST(`{col}` AS STRING)) LIKE LOWER('%{safe}%')")
        else:
            clauses.append(f"LOWER(CAST(`{col}` AS STRING)) = LOWER('{safe}')")
    return _query(
        token,
        f"SELECT * FROM silver.`{schema}`.`{table}` WHERE {' AND '.join(clauses)} LIMIT 100",
    )


def get_downstream_tables(token: str, source_tables: list[str]) -> pd.DataFrame:
    """
    Query system.access.table_lineage for all tables downstream of the matched silver tables.
    Uses event_date filter for partition pruning (1-year retention window).
    """
    if not source_tables:
        return pd.DataFrame()
    in_list = ", ".join(f"'{t}'" for t in source_tables)
    return _query(token, f"""
        SELECT
            source_table_full_name                      AS silver_table,
            target_table_full_name                      AS downstream_table,
            target_table_catalog                        AS target_catalog,
            COALESCE(entity_type, 'unknown')            AS entity_type,
            MAX(event_time)                             AS last_seen
        FROM system.access.table_lineage
        WHERE source_table_full_name IN ({in_list})
          AND target_table_full_name IS NOT NULL
          AND event_date >= current_date() - 365
        GROUP BY ALL
        ORDER BY last_seen DESC
    """)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="SAR Search", page_icon="🔍", layout="wide")

st.title("GDPR Subject Access Request Search")
st.caption(
    "Searches silver layer tables using governed `class.*` tags. "
    "Queries run as your user identity — data steward ABAC exemptions apply."
)

# ---------------------------------------------------------------------------
# Sidebar — identifier selection
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Search Identifiers")
    st.markdown("Select one or more identifier types and enter the values to find.")
    st.divider()

    selected: dict[str, str] = {}
    for label, tag in TAG_MAP.items():
        if st.checkbox(label, value=(label == "Email")):
            val = st.text_input(label, key=f"input_{label}", label_visibility="collapsed",
                                placeholder=label)
            if val.strip():
                selected[tag] = val.strip()

    st.divider()
    search_clicked = st.button("Search", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

if not search_clicked:
    st.info(
        "Select one or more identifier types in the sidebar, enter values, "
        "then click **Search**. \n\n"
        "When multiple identifiers are provided, a row must match **all** of them "
        "(AND logic within each table). Tables that only carry a subset of the "
        "provided tags are still searched using the columns they have."
    )
    st.stop()

if not selected:
    st.warning("Enter at least one identifier value before searching.")
    st.stop()

token = _get_token()
if not token:
    st.error("No auth token available. Ensure the app is running inside Databricks.")
    st.stop()

# ---------------------------------------------------------------------------
# 1. Discover tagged columns
# ---------------------------------------------------------------------------

with st.spinner("Loading tagged column catalogue…"):
    try:
        tagged_df = get_tagged_columns(token)
    except Exception as exc:
        st.error(f"Failed to load column tags: {exc}")
        st.stop()

if tagged_df.empty:
    st.warning("No class.*-tagged columns found in silver. Apply governed tags first.")
    st.stop()

# ---------------------------------------------------------------------------
# 2. Search each silver table
# ---------------------------------------------------------------------------

st.subheader("Silver Layer Results")

# Group tagged columns by (schema, table)
tables = tagged_df.groupby(["table_schema", "table_name"])

matched_tables: list[str] = []
any_results = False

progress = st.progress(0, text="Searching silver tables…")
table_list = list(tables)

for i, ((schema, table), group) in enumerate(table_list):
    progress.progress((i + 1) / len(table_list), text=f"Searching `silver.{schema}.{table}`…")

    # Find which of the user-selected tags this table actually has
    available = {row.tag_name: row.column_name for _, row in group.iterrows()}
    conditions = [
        (available[tag], clean_search_value(tag, val), tag)
        for tag, val in selected.items()
        if tag in available
    ]

    if not conditions:
        continue  # table has none of the searched tag types

    try:
        result_df = search_silver_table(token, schema, table, conditions)
    except Exception as exc:
        st.warning(f"`silver.{schema}.{table}` — query failed: {exc}")
        continue

    if not result_df.empty:
        any_results = True
        full_name = f"silver.{schema}.{table}"
        matched_tables.append(full_name)

        tag_labels = ", ".join(
            next(k for k, v in TAG_MAP.items() if v == tag)
            for tag in selected
            if tag in available
        )
        with st.expander(
            f"**silver.{schema}.{table}** — {len(result_df)} row(s) matched on: {tag_labels}",
            expanded=True,
        ):
            st.dataframe(result_df, use_container_width=True)

progress.empty()

if not any_results:
    st.success("No records found in silver for the provided identifier(s).")

# ---------------------------------------------------------------------------
# 3. Lineage — downstream tables
# ---------------------------------------------------------------------------

if matched_tables:
    st.divider()
    st.subheader("Downstream Lineage")
    st.caption(
        "Tables that have received data from the matched silver tables, "
        "according to `system.access.table_lineage` (1-year rolling window)."
    )

    with st.spinner("Querying lineage…"):
        try:
            lineage_df = get_downstream_tables(token, matched_tables)
        except Exception as exc:
            st.warning(f"Lineage query failed: {exc}")
            lineage_df = pd.DataFrame()

    if lineage_df.empty:
        st.info(
            "No downstream lineage found for the matched tables. "
            "This may mean no pipelines have run since lineage tracking was enabled, "
            "or the data has not propagated downstream."
        )
    else:
        st.dataframe(
            lineage_df.rename(columns={
                "silver_table":      "Source (Silver)",
                "downstream_table":  "Downstream Table",
                "target_catalog":    "Catalog",
                "entity_type":       "Written By",
                "last_seen":         "Last Seen",
            }),
            use_container_width=True,
        )
        st.caption(
            "These downstream tables **may** contain data derived from the subject's records. "
            "Check them manually or run the formal SAR job (`platform-sar-search`) "
            "for an auditable record."
        )
