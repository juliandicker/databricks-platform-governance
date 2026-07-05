"""Databricks SQL connection wrapper and cached metadata queries.

``DatabricksClient`` owns the connection lifecycle and exposes a single
``query`` method.  ``get_tagged_columns`` is a module-level cached function
(``@st.cache_data``) so the Unity Catalog tag scan is not repeated on every
Streamlit re-run.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient


class DatabricksClient:
    """Thin wrapper around the Databricks SQL connector.

    Reads ``DATABRICKS_HOST`` and ``DATABRICKS_WAREHOUSE_ID`` from the
    environment. The auth token is injected at construction time so it can
    be scoped to the calling user's session.

    The underlying connection is opened lazily on first ``query()`` and
    reused for the lifetime of the client — a search run issues one query per
    matched table plus several lineage BFS hops, and re-connecting to the SQL
    warehouse for each of those adds real latency. Call ``close()`` once the
    client is no longer needed.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self._host = os.getenv("DATABRICKS_HOST", "").removeprefix("https://")
        self._warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self._conn = None

    def query(self, sql: str) -> pd.DataFrame:
        """Execute *sql* and return all results as a ``DataFrame``."""
        if self._conn is None:
            self._conn = dbsql.connect(
                server_hostname=self._host,
                http_path=f"/sql/1.0/warehouses/{self._warehouse_id}",
                access_token=self._token,
            )
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall_arrow().to_pandas()

    def execute(self, sql: str) -> int:
        """Execute a non-SELECT statement (DELETE, INSERT, ...) and return the affected row count.

        Unlike ``query()``, this never calls ``fetchall_arrow()`` — DML
        statements have no result set to fetch.
        """
        if self._conn is None:
            self._conn = dbsql.connect(
                server_hostname=self._host,
                http_path=f"/sql/1.0/warehouses/{self._warehouse_id}",
                access_token=self._token,
            )
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount

    def close(self) -> None:
        """Close the underlying connection, if one was opened."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def get_service_principal_token() -> str:
    """Return a bearer token for the app's own service principal identity.

    Used to escalate beyond the calling user's own privileges for actions
    the app's SP has explicit grants for that the user doesn't necessarily
    have themselves — upstream bronze search, and erasure execution across
    tables owned by teams other than the caller's own (see the SAR-app-SP
    grants in ``terraform/catalogs.tf``).
    """
    return WorkspaceClient().config.authenticate()["Authorization"].removeprefix("Bearer ")


@st.cache_data(ttl=300, show_spinner=False)
def get_tagged_columns(token: str, catalog: str) -> pd.DataFrame:
    """Return all ``class.*`` column tags from *catalog*.information_schema.

    Results are cached for 5 minutes (``ttl=300``) to avoid re-scanning the
    information schema on every Streamlit re-run. ``token`` is included in
    the cache key (unlike a leading-underscore param) so the tag catalogue is
    scoped per-caller rather than shared across sessions — otherwise the
    first user to populate the cache for a given catalog would silently
    determine what every other user sees for up to 5 minutes.
    """
    return DatabricksClient(token).query(f"""
        SELECT schema_name  AS table_schema,
               table_name,
               column_name,
               tag_name
        FROM   {catalog}.information_schema.column_tags
        WHERE  tag_name LIKE 'class.%'
    """)
