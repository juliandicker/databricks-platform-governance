"""GDPR Article 15 access-report backend.

Builds a subject-facing HTML disclosure document from the *same* search
results the Search & Erase page already found (``st.session_state.sar_cards``
/ ``card["original_df"]``) and writes the audit trail to ``admin.access``
(owned by the data_platform_admins team — see
``terraform/data-product-teams.tf`` and
``terraform/catalogs.tf: databricks_grants.admin_access``).

Hard constraints this module exists to satisfy (agreed DPO/GDPR review, see
the erasure-feature design notes this mirrors):
  - The audit trail is evidence that a disclosure happened, never a copy of
    the disclosed data — subject and row identifiers are always hashed via
    the ``admin.shared.hash_access_subject_ref``/``hash_access_row_key``
    UDFs before being persisted, never stored as plaintext. The generated
    report document itself is never persisted server-side either — it
    exists only as the reviewer's one-time download.
  - A row matching the subject's search identifiers can still carry a
    *different* subject's personal data in another column (e.g. a shared
    booking row) — Art. 15(4) means this tool must not blindly disclose
    every ``class.*``-tagged column on a matched table. Column inclusion is
    therefore always an explicit, reviewer-confirmed decision
    (``TableAccessTarget.included_columns`` / ``redacted_columns``), never
    an automatic default.
  - This module never re-queries bronze/silver/gold for row data — it only
    ever renders rows already fetched by the search pipeline. No new
    lakehouse read grants are needed for this feature.
"""

from __future__ import annotations

import html
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from database import DatabricksClient
from erasure import _sql_string, _sql_timestamp

#: Fixed Art. 15(1)(e)/(f) rights notice — genuinely static, no per-request
#: variation, so it costs nothing to include in full every time.
RIGHTS_BOILERPLATE = (
    "In addition to this right of access, you have the right to request "
    "rectification of inaccurate data (Art. 16), erasure (Art. 17), "
    "restriction of processing (Art. 18), to object to processing (Art. 21), "
    "and to receive your data in a portable format (Art. 20), and the right "
    "to lodge a complaint with your national data protection supervisory "
    "authority (Art. 77)."
)

#: Truthful only for what this platform actually models — no external
#: processors are configured anywhere in this repo. Flagged in docs/sar-app.md
#: as something to verify against real-world data flows outside this platform.
RECIPIENTS_BOILERPLATE = (
    "Your data is processed internally by the data platform team and the "
    "data product team that owns each table listed below. This platform has "
    "no external data recipients or processors configured."
)

AUTOMATED_DECISION_BOILERPLATE = (
    "No automated decision-making producing legal or similarly significant "
    "effects (Art. 22) has been identified for the tables in this report."
)


@dataclass
class TableAccessTarget:
    """One matched table's worth of reviewer-confirmed disclosure scope."""

    full_name: str              # catalog.schema.table
    provenance: str              # "direct" | "upstream" | "downstream"
    matched_column_or_tag: str
    rows: pd.DataFrame           # every row this search matched for this table
    included_columns: list[str]  # reviewer-confirmed columns to disclose
    redacted_columns: list[str]  # class.*-tagged columns present but excluded


def get_all_tagged_columns(client: DatabricksClient, full_name: str) -> dict[str, list[str]]:
    """Return ``{tag_name: [column_name, ...]}`` for every governed column on *full_name*.

    Unlike ``database.get_tagged_columns`` (scoped to a whole catalog for the
    initial search sweep), this looks at one specific matched table so the
    redaction-review step can see *every* ``class.*`` column present on it —
    including ones the search didn't happen to match against — not just the
    tag(s) that found this table.
    """
    catalog, schema, table = full_name.split(".", 2)
    df = client.query(f"""
        SELECT column_name, tag_name
        FROM   {catalog}.information_schema.column_tags
        WHERE  schema_name = '{schema}' AND table_name = '{table}' AND tag_name LIKE 'class.%'
    """)
    tags: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        tags.setdefault(row.tag_name, []).append(row.column_name)
    return tags


def get_retention_info(client: DatabricksClient, full_names: list[str]) -> pd.DataFrame:
    """Return the ``admin.shared.retention_compliance`` rows for *full_names*.

    Deliberately not ``erasure.get_vacuum_retention`` — VACUUM retention is
    how long a *deleted* row's physical files survive for restore purposes,
    a different concept from a live row's retention policy. Conflating the
    two would put the wrong fact in a legal disclosure document.
    """
    if not full_names:
        return pd.DataFrame()
    in_list = ", ".join(_sql_string(f) for f in full_names)
    return client.query(f"""
        SELECT full_table_name, retention_status, has_delete_at, freshness_sla
        FROM   admin.shared.retention_compliance
        WHERE  full_table_name IN ({in_list})
    """)


def get_table_comments(client: DatabricksClient, full_names: list[str]) -> dict[str, dict]:
    """Return ``{full_name: {"table_comment": str | None, "column_comments": {col: str}}}``.

    A drafting aid only, never a source of truth — pipeline authors may not
    have set ``COMMENT``s, or may have stale ones. Shown to the reviewer
    alongside the required purpose-of-processing field, never substituted
    for it.
    """
    comments: dict[str, dict] = {}
    for full_name in full_names:
        catalog, schema, table = full_name.split(".", 2)
        try:
            t_df = client.query(f"""
                SELECT comment FROM {catalog}.information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{table}'
            """)
            table_comment = t_df.iloc[0]["comment"] if not t_df.empty else None
        except Exception:  # noqa: BLE001
            table_comment = None
        try:
            c_df = client.query(f"""
                SELECT column_name, comment FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{table}' AND comment IS NOT NULL
            """)
            column_comments = dict(zip(c_df["column_name"], c_df["comment"])) if not c_df.empty else {}
        except Exception:  # noqa: BLE001
            column_comments = {}
        comments[full_name] = {"table_comment": table_comment, "column_comments": column_comments}
    return comments


def hash_value(client: DatabricksClient, udf_name: str, val: str) -> str:
    """Call an ``admin.shared`` hashing UDF on a single value.

    Same shape as ``erasure.ErasureExecutor._hash_value`` but module-level
    and duplicated rather than imported — this feature has no execution
    state to bind it to, and keeping the two features' hashing code
    independent means changes here can't regress the already-shipped
    erasure path.
    """
    df = client.query(f"SELECT admin.shared.{udf_name}({_sql_string(val)}) AS h")
    return str(df.iloc[0]["h"])


def hash_row_keys(client: DatabricksClient, df: pd.DataFrame, columns: list[str], udf_name: str) -> list[str]:
    """Hash a canonical per-row key (all *columns* values joined) for each row in *df*.

    Same shape as ``erasure.hash_row_keys``, parameterised by *udf_name* so
    it can call ``hash_access_row_key`` instead of erasure's ``hash_row_key``
    — evidence of exactly which rows were disclosed, never the row content.
    """
    keys = ["|".join(str(row[col]) for col in columns) for _, row in df.iterrows()]
    if not keys:
        return []
    values_clause = ", ".join(f"({_sql_string(k)})" for k in keys)
    result = client.query(f"""
        SELECT admin.shared.{udf_name}(val) AS h
        FROM   (VALUES {values_clause}) AS t(val)
    """)
    return list(result["h"])


def _esc(val: object) -> str:
    return html.escape(str(val)) if val is not None and not pd.isna(val) else ""


def _retention_line(retention_df: pd.DataFrame, full_name: str) -> str:
    if retention_df.empty:
        return "Not tracked by the platform's retention-compliance view."
    match = retention_df[retention_df["full_table_name"] == full_name]
    if match.empty:
        return "Not tracked by the platform's retention-compliance view."
    row = match.iloc[0]
    if not row["has_delete_at"]:
        return "No automatic retention policy configured for this table."
    return f"Automatic retention configured (freshness SLA: {_esc(row['freshness_sla'])})."


def build_report(
    request_id: str,
    subject_display: str,
    requested_by: str,
    purpose: str,
    generated_at: datetime,
    targets: list[TableAccessTarget],
    retention_df: pd.DataFrame,
    comments: dict[str, dict],
) -> str:
    """Assemble the final self-contained HTML disclosure document.

    A single string with inline CSS and no external resources — the
    reviewer downloads it and can open it standalone in a browser and use
    Print -> Save as PDF. Redacted columns are dropped entirely from the
    per-table tables (Art. 15(4) is about *not disclosing* another
    person's data, not about showing a masked placeholder).
    """
    categories = sorted({t.matched_column_or_tag for t in targets if t.matched_column_or_tag})

    table_sections = []
    for target in targets:
        comment_info = comments.get(target.full_name, {})
        table_comment = comment_info.get("table_comment")
        column_comments = comment_info.get("column_comments", {})

        redacted_note = (
            f"<p class='redacted-note'>Columns not disclosed (present on this table but "
            f"outside the scope of this request): {_esc(', '.join(target.redacted_columns))}.</p>"
            if target.redacted_columns else ""
        )
        comment_note = (
            f"<p class='comment-note'><em>Table description: {_esc(table_comment)}</em></p>"
            if table_comment else ""
        )

        header_cells = "".join(
            f"<th>{_esc(col)}"
            + (f"<br><span class='col-comment'>{_esc(column_comments[col])}</span>" if col in column_comments else "")
            + "</th>"
            for col in target.included_columns
        )
        body_rows = "".join(
            "<tr>" + "".join(f"<td>{_esc(row[col])}</td>" for col in target.included_columns) + "</tr>"
            for _, row in target.rows.iterrows()
        )

        table_sections.append(f"""
        <section class="table-section">
          <h3>{_esc(target.full_name)}</h3>
          <p class="meta">Found via: {_esc(target.provenance)} match on {_esc(target.matched_column_or_tag)}
             &middot; {len(target.rows)} row(s) &middot; {_retention_line(retention_df, target.full_name)}</p>
          {comment_note}
          {redacted_note}
          <table>
            <thead><tr>{header_cells}</tr></thead>
            <tbody>{body_rows}</tbody>
          </table>
        </section>
        """)

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Subject Access Report — {_esc(request_id)}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Arial, sans-serif; color: #1a1a1a; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }}
  h1 {{ font-size: 1.5rem; }}
  h2 {{ font-size: 1.15rem; border-bottom: 1px solid #ccc; padding-bottom: 0.25rem; margin-top: 2rem; }}
  h3 {{ font-family: ui-monospace, monospace; font-size: 1rem; }}
  .meta {{ color: #555; font-size: 0.85rem; }}
  .redacted-note {{ color: #8a5300; font-size: 0.85rem; }}
  .comment-note {{ color: #444; font-size: 0.85rem; }}
  table {{ border-collapse: collapse; width: 100%; margin: 0.5rem 0 1.5rem; font-size: 0.85rem; }}
  th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: left; vertical-align: top; }}
  th {{ background: #f2f2f2; }}
  .col-comment {{ font-weight: normal; color: #666; font-size: 0.75rem; }}
  @media print {{
    body {{ margin: 0; }}
    .table-section {{ page-break-inside: avoid; }}
  }}
</style>
</head>
<body>
  <h1>GDPR Article 15 Subject Access Report</h1>
  <p class="meta">Request ID {_esc(request_id)} &middot; Generated {_esc(generated_at.strftime('%Y-%m-%d %H:%M UTC'))}
     &middot; Prepared by {_esc(requested_by)}</p>

  <h2>Confirmation of processing</h2>
  <p>We confirm that personal data relating to <strong>{_esc(subject_display)}</strong> is being processed
     in the tables listed below.</p>

  <h2>Purpose of processing</h2>
  <p>{_esc(purpose)}</p>

  <h2>Categories of personal data</h2>
  <p>{_esc(', '.join(categories)) if categories else 'See per-table sections below.'}</p>

  <h2>Recipients</h2>
  <p>{RECIPIENTS_BOILERPLATE}</p>

  <h2>Automated decision-making</h2>
  <p>{AUTOMATED_DECISION_BOILERPLATE}</p>

  <h2>Your data</h2>
  {''.join(table_sections)}

  <h2>Your rights</h2>
  <p>{RIGHTS_BOILERPLATE}</p>
</body>
</html>"""


def write_access_request(
    client: DatabricksClient,
    subject_ref: str,
    requested_by: str,
    targets: list[TableAccessTarget],
) -> str:
    """Write the ``admin.access`` audit trail for a generated report and return its request_id.

    ``client`` must be the SAR app's own service-principal-backed
    ``DatabricksClient`` — data stewards only get read access to
    ``admin.access`` (see ``terraform/catalogs.tf: databricks_grants.admin_access``).
    ``subject_ref`` is the plaintext identifier used only to compute
    ``subject_ref_hash`` — never stored.
    """
    request_id = str(uuid.uuid4())
    requested_at = datetime.now(timezone.utc)
    subject_ref_hash = hash_value(client, "hash_access_subject_ref", subject_ref)

    client.execute(f"""
        INSERT INTO admin.access.requests
        (request_id, subject_ref_hash, requested_by, requested_at, status, completed_at)
        VALUES ({_sql_string(request_id)}, {_sql_string(subject_ref_hash)},
                {_sql_string(requested_by)}, {_sql_timestamp(requested_at)}, 'PENDING', NULL)
    """)

    for target in targets:
        _write_request_item(client, request_id, target)

    completed_at = datetime.now(timezone.utc)
    client.execute(f"""
        UPDATE admin.access.requests
        SET status = 'COMPLETED', completed_at = {_sql_timestamp(completed_at)}
        WHERE request_id = {_sql_string(request_id)}
    """)
    return request_id


def _write_request_item(client: DatabricksClient, request_id: str, target: TableAccessTarget) -> None:
    row_key_hashes = (
        hash_row_keys(client, target.rows, list(target.rows.columns), "hash_access_row_key")
        if len(target.rows) else []
    )

    def _array_sql(values: list[str]) -> str:
        return (
            "ARRAY(" + ", ".join(_sql_string(v) for v in values) + ")"
            if values else "CAST(ARRAY() AS ARRAY<STRING>)"
        )

    generated_at = datetime.now(timezone.utc)
    client.execute(f"""
        INSERT INTO admin.access.request_items
        (request_id, table_full_name, provenance, matched_column_or_tag, rows_disclosed,
         columns_included, columns_redacted, row_key_hash, generated_at)
        VALUES (
            {_sql_string(request_id)}, {_sql_string(target.full_name)},
            {_sql_string(target.provenance)}, {_sql_string(target.matched_column_or_tag)},
            {len(target.rows)}, {_array_sql(target.included_columns)},
            {_array_sql(target.redacted_columns)}, {_array_sql(row_key_hashes)},
            {_sql_timestamp(generated_at)}
        )
    """)


def list_access_requests(client: DatabricksClient) -> pd.DataFrame:
    """Return all rows from ``admin.access.requests``, most recent first."""
    return client.query("SELECT * FROM admin.access.requests ORDER BY requested_at DESC")


def list_access_request_items(client: DatabricksClient, request_id: str) -> pd.DataFrame:
    """Return all ``request_items`` rows for *request_id*, in generation order."""
    return client.query(f"""
        SELECT * FROM admin.access.request_items
        WHERE request_id = {_sql_string(request_id)}
        ORDER BY generated_at
    """)
