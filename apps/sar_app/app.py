"""
GDPR Subject Access Request (SAR) Search — Databricks App.

Searches all class.*-tagged columns in silver for a given identifier.
Queries run as the calling user (user-authorization mode), so data steward
ABAC exemptions apply automatically — unmasked values are returned.

Name search uses two-stage fuzzy matching:
  1. SQL pre-filter: expanded OR LIKE across nickname variants per name token
     so "Tony" also fetches rows containing "Anthony", "Ant", etc.
  2. Python post-filter: rapidfuzz token_sort_ratio scores the candidates
     and drops anything below the configurable threshold.

Lineage section uses system.access.table_lineage to show which gold/bronze
tables were derived from any silver table that contained a match.
"""

import os
import streamlit as st
import pandas as pd
from rapidfuzz import fuzz
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

# class.location uses LIKE (partial postcodes); class.name uses nickname expansion + fuzzy
LIKE_TAGS = {"class.location"}

_HONORIFICS = {"mr", "mrs", "ms", "miss", "dr", "prof", "sir", "mx", "rev", "lord", "lady"}

# Common English name nicknames / short forms.
# Each key maps to a list of variants that should be searched alongside it.
NICKNAMES: dict[str, list[str]] = {
    "anthony":    ["tony", "ant", "antony", "anton"],
    "tony":       ["anthony", "ant", "antony"],
    "ant":        ["anthony", "antony"],
    "antony":     ["anthony", "tony", "ant"],
    "robert":     ["bob", "rob", "bobby", "robbie", "bert"],
    "bob":        ["robert", "rob", "bobby"],
    "rob":        ["robert", "bob", "robbie"],
    "william":    ["bill", "will", "billy", "willy", "liam"],
    "bill":       ["william", "will", "billy"],
    "will":       ["william", "bill", "billy"],
    "liam":       ["william"],
    "james":      ["jim", "jimmy", "jamie", "jay"],
    "jim":        ["james", "jimmy", "jamie"],
    "jamie":      ["james", "jim"],
    "michael":    ["mike", "mick", "mickey", "micky"],
    "mike":       ["michael", "mick", "mickey"],
    "mick":       ["michael", "mike"],
    "thomas":     ["tom", "tommy"],
    "tom":        ["thomas", "tommy"],
    "tommy":      ["thomas", "tom"],
    "christopher": ["chris", "christy", "kit"],
    "chris":      ["christopher", "christy"],
    "daniel":     ["dan", "danny"],
    "dan":        ["daniel", "danny"],
    "benjamin":   ["ben", "benny", "benji"],
    "ben":        ["benjamin", "benny"],
    "alexander":  ["alex", "al", "sandy", "lex"],
    "alex":       ["alexander", "alexandra", "alexis"],
    "samuel":     ["sam", "sammy"],
    "sam":        ["samuel", "samantha", "sammy"],
    "samantha":   ["sam", "sammy"],
    "nicholas":   ["nick", "nico", "nicky"],
    "nick":       ["nicholas", "nico"],
    "stephen":    ["steve", "steven", "stevie"],
    "steven":     ["steve", "stephen", "stevie"],
    "steve":      ["stephen", "steven", "stevie"],
    "matthew":    ["matt", "matty"],
    "matt":       ["matthew", "matty"],
    "joseph":     ["joe", "joey"],
    "joe":        ["joseph", "joey"],
    "john":       ["johnny", "jon", "jack"],
    "jon":        ["john", "johnny"],
    "johnny":     ["john", "jon"],
    "jack":       ["john", "jake"],
    "jacob":      ["jake", "jay"],
    "jake":       ["jacob", "jack"],
    "henry":      ["harry", "hal"],
    "harry":      ["henry", "harold", "hal"],
    "harold":     ["harry", "hal"],
    "charles":    ["charlie", "chuck", "chas"],
    "charlie":    ["charles", "chuck"],
    "edward":     ["ed", "eddie", "ned", "ted"],
    "ed":         ["edward", "eddie", "ted"],
    "ted":        ["edward", "ed"],
    "andrew":     ["andy", "drew"],
    "andy":       ["andrew"],
    "drew":       ["andrew"],
    "richard":    ["rick", "ricky", "dick", "rich"],
    "rick":       ["richard", "ricky"],
    "peter":      ["pete", "petey"],
    "pete":       ["peter"],
    "frederick":  ["fred", "freddie", "freddy"],
    "fred":       ["frederick", "freddie"],
    "david":      ["dave", "davey"],
    "dave":       ["david"],
    "mark":       ["marcus"],
    "marcus":     ["mark"],
    "patrick":    ["pat", "paddy"],
    "pat":        ["patrick", "patricia"],
    "katherine":  ["kate", "katy", "katie", "kathy", "kat", "cath", "catherine"],
    "catherine":  ["kate", "cath", "cat", "katherine", "kath"],
    "kate":       ["katherine", "catherine", "katy", "katie"],
    "elizabeth":  ["liz", "beth", "betty", "eliza", "lisa", "ellie", "bess"],
    "liz":        ["elizabeth", "beth", "betty"],
    "beth":       ["elizabeth", "liz"],
    "jennifer":   ["jenny", "jen"],
    "jenny":      ["jennifer", "jen"],
    "jen":        ["jennifer", "jenny"],
    "patricia":   ["pat", "trish", "tricia"],
    "trish":      ["patricia", "pat"],
    "susan":      ["sue", "susie"],
    "sue":        ["susan", "susie"],
    "victoria":   ["vicky", "vic", "vikki"],
    "vicky":      ["victoria", "vic"],
    "amelia":     ["amy", "milly", "millie", "mel"],
    "amy":        ["amelia"],
    "millie":     ["amelia", "milly", "millicent"],
    "sarah":      ["sara"],
    "sara":       ["sarah"],
    "claire":     ["clara", "clare"],
    "helen":      ["eleanor", "nell", "elena"],
    "emily":      ["em", "emma"],
    "emma":       ["emily", "em"],
    "margaret":   ["maggie", "meg", "peggy", "marge"],
    "maggie":     ["margaret", "meg"],
    "dorothy":    ["dot", "dottie", "dolly"],
    "ian":        ["iain"],
    "iain":       ["ian"],
    "neil":       ["nigel"],
    "nigel":      ["neil"],
}


def clean_search_value(tag: str, val: str) -> str:
    """Strip honorifics from name searches; leave other values unchanged."""
    if tag != "class.name":
        return val
    parts = val.strip().split()
    if parts and parts[0].lower().rstrip(".") in _HONORIFICS:
        parts = parts[1:]
    return " ".join(parts)


def _name_sql_clause(col: str, clean_val: str) -> str:
    """
    Build a WHERE clause for a name column using expanded OR LIKE per token.
    Each token in the search term is expanded with its known nickname variants,
    and all tokens must be present (AND of per-token OR-LIKE groups).
    Example: 'Tony Hill' → (LIKE '%tony%' OR LIKE '%anthony%' OR ...) AND LIKE '%hill%'
    """
    token_groups = []
    for token in clean_val.lower().split():
        variants = [token] + NICKNAMES.get(token, [])
        likes = " OR ".join(
            f"LOWER(CAST(`{col}` AS STRING)) LIKE '%{v.replace(chr(39), chr(39)*2)}%'"
            for v in variants
        )
        token_groups.append(f"({likes})")
    return " AND ".join(token_groups)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_token() -> str:
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
    conditions: list[tuple[str, str, str]],  # (column, clean_value, tag)
    fuzzy_threshold: int = 75,
) -> pd.DataFrame:
    """
    Search silver.<schema>.<table> with the given conditions.

    - class.name:     expanded OR LIKE pre-filter (nicknames) + rapidfuzz post-filter
    - class.location: LIKE containment
    - everything else: exact equality

    Returns matched rows with a _match_score column (100 = exact) when any
    name condition was applied; rows sorted by score descending.
    """
    clauses = []
    name_conditions: list[tuple[str, str]] = []  # (column, clean_value) for scoring

    for col, val, tag in conditions:
        safe = val.replace("'", "''")
        if tag == "class.name":
            clauses.append(_name_sql_clause(col, val))
            name_conditions.append((col, val))
        elif tag in LIKE_TAGS:
            clauses.append(f"LOWER(CAST(`{col}` AS STRING)) LIKE LOWER('%{safe}%')")
        else:
            clauses.append(f"LOWER(CAST(`{col}` AS STRING)) = LOWER('{safe}')")

    sql = (
        f"SELECT * FROM silver.`{schema}`.`{table}`"
        f" WHERE {' AND '.join(clauses)}"
        f" LIMIT 500"
    )
    df = _query(token, sql)

    if df.empty or not name_conditions:
        return df

    # Rapidfuzz post-filter: score each row against the clean search values
    def _row_score(row: pd.Series) -> int:
        scores = [
            fuzz.token_sort_ratio(val.lower(), str(row.get(col, "")).lower())
            for col, val in name_conditions
        ]
        return min(scores)  # row must pass threshold for ALL name conditions

    df["_match_score"] = df.apply(_row_score, axis=1)
    df = df[df["_match_score"] >= fuzzy_threshold].copy()
    df = df.sort_values("_match_score", ascending=False).reset_index(drop=True)
    return df


def get_downstream_tables(token: str, source_tables: list[str]) -> pd.DataFrame:
    if not source_tables:
        return pd.DataFrame()
    in_list = ", ".join(f"'{t}'" for t in source_tables)
    return _query(token, f"""
        SELECT
            source_table_full_name               AS silver_table,
            target_table_full_name               AS downstream_table,
            target_table_catalog                 AS target_catalog,
            COALESCE(entity_type, 'unknown')     AS entity_type,
            MAX(event_time)                      AS last_seen
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
# Sidebar
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

    fuzzy_threshold = st.slider(
        "Name match threshold",
        min_value=50, max_value=100, value=75, step=5,
        help=(
            "Minimum rapidfuzz token_sort_ratio score (0–100) for a name to be "
            "considered a match. Lower values catch more variants; higher values "
            "require closer spelling. Nickname expansion (Tony/Anthony/Ant) runs "
            "regardless of this threshold."
        ),
    )

    st.divider()
    search_clicked = st.button("Search", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

if not search_clicked:
    st.info(
        "Select one or more identifier types in the sidebar, enter values, "
        "then click **Search**.\n\n"
        "**Name matching** strips honorifics (Mr/Mrs/Dr…), expands common "
        "nicknames (Tony → Anthony, Ant…), and ranks results by fuzzy similarity. "
        "Adjust the threshold slider if you want stricter or broader matches.\n\n"
        "When multiple identifiers are provided, a row must satisfy **all** of them."
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

tables = tagged_df.groupby(["table_schema", "table_name"])
matched_tables: list[str] = []
any_results = False

progress = st.progress(0, text="Searching silver tables…")
table_list = list(tables)

for i, ((schema, table), group) in enumerate(table_list):
    progress.progress((i + 1) / len(table_list), text=f"Searching `silver.{schema}.{table}`…")

    available = {row.tag_name: row.column_name for _, row in group.iterrows()}
    conditions = [
        (available[tag], clean_search_value(tag, val), tag)
        for tag, val in selected.items()
        if tag in available
    ]

    if not conditions:
        continue

    try:
        result_df = search_silver_table(token, schema, table, conditions, fuzzy_threshold)
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
        score_col = "_match_score" in result_df.columns
        header = (
            f"**silver.{schema}.{table}** — {len(result_df)} row(s) matched on: {tag_labels}"
        )
        with st.expander(header, expanded=True):
            display_df = result_df.copy()
            if score_col:
                display_df = display_df.rename(columns={"_match_score": "Match Score %"})
            st.dataframe(display_df, use_container_width=True)

progress.empty()

if not any_results:
    st.success("No records found in silver for the provided identifier(s).")

# ---------------------------------------------------------------------------
# 3. Lineage
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
            "No downstream lineage found. This may mean no pipelines have run "
            "since lineage tracking was enabled, or data has not propagated downstream."
        )
    else:
        st.dataframe(
            lineage_df.rename(columns={
                "silver_table":     "Source (Silver)",
                "downstream_table": "Downstream Table",
                "target_catalog":   "Catalog",
                "entity_type":      "Written By",
                "last_seen":        "Last Seen",
            }),
            use_container_width=True,
        )
        st.caption(
            "These downstream tables **may** contain data derived from the subject's records. "
            "Check them manually or run the formal SAR job (`platform-sar-search`) "
            "for an auditable record."
        )
