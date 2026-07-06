"""SAR Article 15 access-report review page.

Read-only visibility into ``admin.access.requests``/``request_items`` — the
evidence trail proving a disclosure happened, without ever showing what was
disclosed (the report document itself is never persisted; see
``access_report.py``). Same UI/backend split as ``erasure_review.py``: this
module only renders, ``access_report.py`` owns the query/audit logic. There
is no restore-equivalent action here — a disclosure has nothing to undo.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from access_report import list_access_request_items, list_access_requests
from database import DatabricksClient

#: Art. 12(3) — one month to respond to an access request, extendable by up
#: to two further months for complex requests. This badge flags the base
#: one-month deadline only; a genuinely extended request should be tracked
#: by the DPO outside this tool.
SLA_DAYS = 30


def _to_utc(ts: object) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _sla_badge(requested_at: object, completed_at: object) -> str:
    deadline = _to_utc(requested_at) + pd.Timedelta(days=SLA_DAYS)
    if completed_at is not None and not pd.isna(completed_at):
        completed = _to_utc(completed_at)
        return (
            f"✅ Completed on time (by {deadline.date()})" if completed <= deadline
            else f"⚠️ Completed late — deadline was {deadline.date()}"
        )
    now = pd.Timestamp.now(tz="UTC")
    return (
        f"⏳ Open — due {deadline.date()}" if now <= deadline
        else f"❌ Overdue — was due {deadline.date()}"
    )


def render(client: DatabricksClient) -> None:
    st.title("Access Requests")
    st.caption(
        "Review past GDPR Article 15 access reports. This page shows evidence "
        "that a disclosure happened — which tables, how many rows, and which "
        "columns were included or redacted — never the disclosed data itself, "
        "which only ever exists as a one-time download at request time."
    )

    requests_df = list_access_requests(client)
    if requests_df.empty:
        st.info("No access requests recorded yet.")
        return

    def _label(row: pd.Series) -> str:
        return f"{row['request_id']} — {row['requested_at']} — {row['status']} — {row['requested_by']}"

    labels = [_label(row) for _, row in requests_df.iterrows()]
    selected_label = st.selectbox("Access request", labels)
    selected_row = requests_df.iloc[labels.index(selected_label)]
    request_id = selected_row["request_id"]

    st.subheader(f"Request `{request_id}`")
    st.write(
        f"Requested by **{selected_row['requested_by']}** · "
        f"Status: **{selected_row['status']}** · "
        f"{_sla_badge(selected_row['requested_at'], selected_row['completed_at'])}"
    )

    items_df = list_access_request_items(client, request_id)
    if items_df.empty:
        st.info("No items recorded for this request.")
        return

    for _, item in items_df.iterrows():
        with st.container(border=True):
            st.write(
                f"**{item['table_full_name']}** ({item['provenance']}) — "
                f"{item['rows_disclosed']} row(s) disclosed · "
                f"matched on: {item['matched_column_or_tag']}"
            )
            included = list(item["columns_included"]) if item["columns_included"] is not None else []
            redacted = list(item["columns_redacted"]) if item["columns_redacted"] is not None else []
            st.caption(f"Columns included: {', '.join(included) or '—'}")
            if redacted:
                st.caption(f"⚠ Columns redacted (not disclosed): {', '.join(redacted)}")
