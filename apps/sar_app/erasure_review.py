"""SAR erasure requests review page.

Read-only visibility into ``admin.erasure.requests``/``request_items``,
plus a time-travel-based "restore" for a successfully-executed table
delete, while its VACUUM retention window still holds the physical files.
All backend logic (finding the pre-delete version, hashing, reinserting
rows, writing the restorations audit trail) lives in ``erasure.py`` — this
module only renders it, the same division ``app.py`` already keeps between
UI and the supporting modules.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from database import DatabricksClient, get_service_principal_token
from erasure import (
    RestorationResult,
    find_pre_delete_version,
    list_request_items,
    list_requests,
    list_restorations,
    restore_request_item,
    write_restoration,
)

EXECUTION_ICON = {"SUCCEEDED": "✅", "FAILED": "❌", "SKIPPED": "⚠️"}

RESTORE_REASONS = [
    "Erasure executed in error",
    "Legal hold applied after erasure",
    "Other (see notes)",
]


def _to_utc(ts: object) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _restored_table_names(client: DatabricksClient, request_id: str) -> set[str]:
    """table_full_name values that already have a SUCCEEDED restoration for this request."""
    restorations = list_restorations(client, request_id)
    if restorations.empty:
        return set()
    succeeded = restorations[restorations["execution_status"] == "SUCCEEDED"]
    return set(succeeded["table_full_name"])


@st.dialog("Confirm restore", width="large")
def _render_restore_dialog(client: DatabricksClient, request_id: str, item: pd.Series) -> None:
    table_full_name = item["table_full_name"]
    st.write(f"Restoring rows into **{table_full_name}** for request `{request_id}`.")

    with st.spinner("Locating the pre-delete version…"):
        version_info = find_pre_delete_version(client, table_full_name, item["executed_at"])

    if version_info is None:
        st.error("Could not find a matching DELETE operation in this table's history.")
        if st.button("Close"):
            st.rerun()
        return

    st.caption(
        f"Detected Delta version **{version_info['matched_version']}** as the delete "
        f"(committed {version_info['matched_timestamp']}, "
        f"{version_info['time_diff_seconds']:.1f}s from this item's recorded execution time) "
        f"— will restore from version **{version_info['pre_delete_version']}**, just before it."
    )
    if version_info.get("operation_metrics"):
        st.caption(f"Delta operation metrics: {version_info['operation_metrics']}")

    reason = st.selectbox("Restoration reason", RESTORE_REASONS)
    notes = st.text_input("Notes (required if 'Other')", "")
    confirm_text = st.text_input("Type RESTORE to confirm", placeholder="RESTORE")

    final_reason = notes.strip() if reason.startswith("Other") else reason
    can_confirm = confirm_text.strip().upper() == "RESTORE" and bool(final_reason)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cancel", use_container_width=True):
            st.rerun()
    with col2:
        if st.button(
            "Restore rows", type="primary", use_container_width=True, disabled=not can_confirm
        ):
            sp_token = get_service_principal_token()
            exec_client = DatabricksClient(sp_token)
            try:
                with st.spinner("Restoring…"):
                    result = restore_request_item(exec_client, item)
                    write_restoration(
                        exec_client,
                        request_id,
                        restored_by=st.context.headers.get("x-forwarded-email") or "unknown",
                        reason=final_reason,
                        result=result,
                    )
            finally:
                exec_client.close()
            st.session_state["sar_review_last_result"] = result
            st.rerun()


def render(client: DatabricksClient) -> None:
    st.title("Erasure Requests")
    st.caption(
        "Review past SAR erasure requests and, while a table's VACUUM retention "
        "window still holds the deleted files, restore a specific table's rows "
        "via Delta time travel — a surgical reinsert of just the erased rows, "
        "not a full-table rollback that would also undo anyone else's writes "
        "made since the erasure."
    )

    if "sar_review_last_result" in st.session_state:
        result: RestorationResult = st.session_state.pop("sar_review_last_result")
        icon = "✅" if result.execution_status == "SUCCEEDED" else "❌"
        st.write(f"{icon} **{result.table_full_name}** — {result.rows_restored} row(s) restored.")
        if result.error_message:
            st.caption(f"⚠ {result.error_message}")
        st.divider()

    requests_df = list_requests(client)
    if requests_df.empty:
        st.info("No erasure requests recorded yet.")
        return

    def _label(row: pd.Series) -> str:
        return f"{row['request_id']} — {row['requested_at']} — {row['status']} — {row['requested_by']}"

    labels = [_label(row) for _, row in requests_df.iterrows()]
    selected_label = st.selectbox("Erasure request", labels)
    selected_row = requests_df.iloc[labels.index(selected_label)]
    request_id = selected_row["request_id"]

    st.subheader(f"Request `{request_id}`")
    st.write(
        f"Legal basis: **{selected_row['legal_basis']}** · "
        f"Requested by **{selected_row['requested_by']}** · "
        f"Status: **{selected_row['status']}**"
    )

    items_df = list_request_items(client, request_id)
    if items_df.empty:
        st.info("No items recorded for this request.")
        return

    restored_tables = _restored_table_names(client, request_id)
    now = pd.Timestamp.now(tz="UTC")

    for _, item in items_df.iterrows():
        icon = EXECUTION_ICON.get(item["execution_status"], "•")
        table_full_name = item["table_full_name"]
        with st.container(border=True):
            st.write(
                f"{icon} **{table_full_name}** ({item['provenance']}) — "
                f"{item['rows_deleted']}/{item['rows_selected']} row(s) deleted · "
                f"targeting: {item['row_targeting_method']} · "
                f"VACUUM: {item['vacuum_retention_raw']} · "
                f"estimated purge by {item['estimated_purge_by']}"
            )
            if item["error_message"]:
                st.caption(f"⚠ {item['error_message']}")

            if table_full_name in restored_tables:
                st.caption("✅ Already restored — see `admin.erasure.restorations` for details.")
                continue

            was_deleted = item["execution_status"] == "SUCCEEDED" and item["rows_deleted"] > 0
            if not was_deleted:
                continue

            within_window = now < _to_utc(item["estimated_purge_by"])
            if not within_window:
                st.caption(
                    "VACUUM retention window has likely passed — a restore may no "
                    "longer be possible, but you can still try."
                )

            if st.button(
                f"Restore rows in {table_full_name}",
                key=f"restore_{request_id}_{table_full_name}",
            ):
                _render_restore_dialog(client, request_id, item)
