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
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

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

#: Shown only when the reviewer has selected no registered system for this
#: disclosure — deliberately scoped to "registered", not a blanket "none
#: exists" claim, since this reflects apps/sar_app/automated_decision_systems.json
#: (organisation-maintained, may be incomplete) rather than something this
#: platform can verify on its own.
AUTOMATED_DECISION_NONE_TEXT = (
    "No automated decision-making system registered as applicable to this "
    "disclosure was identified. This reflects the organisation's registered "
    "automated decision-making systems only."
)

_ADM_SYSTEMS_PATH = Path(__file__).parent / "automated_decision_systems.json"


def load_automated_decision_systems() -> list[dict]:
    """Load the DPO-maintained Art. 22 register from ``automated_decision_systems.json``.

    A JSON file rather than a database table so the DPO/compliance team can
    add, remove, or reword entries directly via a PR — no code change, no
    Databricks access needed. Matching a system to a given disclosure is a
    deliberately manual, reviewer-driven choice (not auto-suggested by
    schema/table) since getting this wrong in either direction — a missed
    system, or a wrongly-attributed one — is a compliance-relevant mistake
    a human should make, not the platform.
    """
    with open(_ADM_SYSTEMS_PATH, encoding="utf-8") as f:
        return json.load(f)["systems"]


_RECIPIENT_TEMPLATES_PATH = Path(__file__).parent / "recipient_templates.json"


def load_recipient_templates() -> list[dict]:
    """Load phrasing templates for the Recipients field from ``recipient_templates.json``.

    Unlike the automated-decision-making register, this is a drafting aid
    only, not a compliance gate — Recipients stays required free text (see
    ``build_report``'s docstring for why), and a template is just inserted
    text the reviewer edits and fills the ``[blanks]`` in themselves,
    exactly like typing it from scratch but with consistent grammar/tone/
    legal phrasing across reports. A JSON file so any organisation adopting
    this app can add, remove, or reword templates via a PR with no code
    change.
    """
    with open(_RECIPIENT_TEMPLATES_PATH, encoding="utf-8") as f:
        return json.load(f)["templates"]


@dataclass
class TableAccessTarget:
    """One matched table's worth of reviewer-confirmed disclosure scope."""

    full_name: str              # catalog.schema.table
    provenance: str              # "direct" | "upstream" | "downstream"
    matched_column_or_tag: str
    rows: pd.DataFrame           # every row this search matched for this table
    included_columns: list[str]  # reviewer-confirmed columns to disclose
    redacted_columns: list[str]  # class.*-tagged columns present but excluded
    column_tags: dict[str, str]  # {column: class.* tag}, governed columns only


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


def draft_purpose(targets: list[TableAccessTarget], comments: dict[str, dict]) -> str:
    """Draft a starting-point "purpose of processing" statement via an LLM, for the reviewer to edit.

    Builds the prompt from schema-level facts only — table/column names,
    table and column ``COMMENT``s, per-column governed tags, provenance — never
    ``TableAccessTarget.rows``. Feeding the model the *disclosed rows
    themselves* (rather than facts about the schema) would create a new
    processing purpose and a new recipient (the model provider) needing its
    own Art. 15(1)(c) disclosure, so that boundary is a hard invariant here,
    not just a style choice — see docs/sar-app.md's "AI-drafted purpose"
    section. Like ``get_table_comments``, this is a drafting aid only: the
    result always lands in the still-editable purpose text box, never
    submitted directly.
    """
    lines = []
    for target in targets:
        if not target.included_columns:
            continue
        if lines:
            lines.append("")  # blank line between tables — cheap disambiguation for multi-table prompts
        info = comments.get(target.full_name, {})
        lines.append(f"Table: {target.full_name} (found via {target.provenance} match on {target.matched_column_or_tag})")
        if info.get("table_comment"):
            lines.append(f"  Description: {info['table_comment']}")
        column_comments = info.get("column_comments", {})
        for col in target.included_columns:
            tag = target.column_tags.get(col)
            comment = column_comments.get(col)
            lines.append(f"  Column: {col}")
            if tag:
                lines.append(f"    Tag: {tag}")
            if comment:
                lines.append(f"    Comment: {comment}")

    if not lines:
        return "Unable to draft a purpose — no tables with included columns to describe yet."

    prompt = (
        "You are drafting the \"purpose of processing\" statement for a GDPR "
        "Article 15 subject access report. Based only on the schema metadata "
        "below (table names, descriptions, column names, column tags and descriptions) — "
        "not on any actual personal data, which you have not been given — "
        "write the business purpose explaining why this personal data is "
        "processed, for the data subject to read.\n\n"
        "Rules:\n"
        "- Write for the data subject reading this: never mention table "
        "names, schema/catalog names, or tag identifiers (e.g. "
        "class.full_name) — describe the business purpose only, in plain "
        "language.\n"
        "- Do not invent facts the metadata doesn't support. Only say the "
        "purpose cannot be determined if a table has no description and no "
        "column comments beyond bare column/tag names — otherwise give a "
        "confident best-effort purpose from what's present, without hedging "
        "words like \"it appears\" or \"possibly\".\n"
        "- Default to one short paragraph (1-3 sentences). Only switch to a "
        "plain \"- \" bulleted list (one purpose per line) if the tables "
        "serve clearly distinct, non-overlapping business functions (e.g. "
        "billing vs. marketing) — if purposes overlap or reinforce each "
        "other, merge them into the paragraph.\n"
        "- Use a formal, neutral register appropriate for a legal disclosure "
        "to a data subject (e.g. \"This data is processed to...\", not "
        "casual phrasing).\n"
        "- Use no markdown or HTML syntax that needs rendering to make "
        "sense — no headers, bold, italics, links, or code formatting — "
        "since this text goes verbatim into a plain text box.\n"
        "- Output only the purpose statement itself — no preamble, heading, "
        "or commentary before or after it.\n\n"
        "Example (one overlapping purpose, paragraph):\n"
        "This data is processed to provide and administer your travel "
        "bookings, including managing reservations, processing payments, "
        "and verifying your identity and age where required by law.\n\n"
        "Example (distinct purposes, list):\n"
        "- Processing your travel bookings and payments.\n"
        "- Sending you marketing communications about offers you have "
        "opted into.\n"
        "- Investigating and responding to customer support enquiries you "
        "have raised.\n\n"
        "Schema metadata:\n" + "\n".join(lines)
    )

    endpoint = os.environ["PURPOSE_DRAFT_ENDPOINT"]
    response = WorkspaceClient().serving_endpoints.query(
        name=endpoint,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        max_tokens=300,
    )
    return response.choices[0].message.content.strip()


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
    recipients: str,
    generated_at: datetime,
    targets: list[TableAccessTarget],
    retention_df: pd.DataFrame,
    comments: dict[str, dict],
    adm_selections: list[dict],
) -> str:
    """Assemble the final self-contained HTML disclosure document.

    A single string with inline CSS and no external resources — the
    reviewer downloads it and can open it standalone in a browser and use
    Print -> Save as PDF. Redacted columns are dropped entirely from the
    per-table tables (Art. 15(4) is about *not disclosing* another
    person's data, not about showing a masked placeholder).

    *recipients* is required reviewer-entered free text (like *purpose*),
    not a platform-derived value — there's no reliable way for this
    platform to enumerate an organisation's actual third parties (reinsurers,
    outsourced claims administrators, cloud vendors, etc.), and guessing
    wrong here is worse than requiring the reviewer to know the answer,
    the same reasoning that keeps *purpose* free text rather than derived.

    *adm_selections* is the reviewer-confirmed subset of
    ``load_automated_decision_systems()`` that applies to this disclosure —
    never auto-matched from the tables/schemas involved, since a wrong
    system attribution (missed or spurious) is a compliance-relevant
    mistake a human should make deliberately, not one the platform guesses
    at. Each selected system's own ``statement``/``safeguards_text`` is
    used verbatim; an empty list renders ``AUTOMATED_DECISION_NONE_TEXT``.
    """
    categories = sorted({
        t.column_tags[col]
        for t in targets
        for col in t.included_columns
        if col in t.column_tags
    })

    if adm_selections:
        adm_html = "".join(
            f"<p><strong>{_esc(system['system_name'])}:</strong> {_esc(system['statement'])} "
            f"{_esc(system['safeguards_text'])}</p>"
            for system in adm_selections
        )
    else:
        adm_html = f"<p>{AUTOMATED_DECISION_NONE_TEXT}</p>"

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
            "<tr>" + "".join(
                f"<td><div class='cell'>{_esc(row[col])}</div></td>" for col in target.included_columns
            ) + "</tr>"
            for _, row in target.rows.iterrows()
        )

        table_sections.append(f"""
        <section class="table-section">
          <h3>{_esc(target.full_name)}</h3>
          <p class="meta">Found via: {_esc(target.provenance)} match on {_esc(target.matched_column_or_tag)}
             &middot; {len(target.rows)} row(s) &middot; {_retention_line(retention_df, target.full_name)}</p>
          {comment_note}
          {redacted_note}
          <div class="table-scroll">
            <table>
              <thead><tr>{header_cells}</tr></thead>
              <tbody>{body_rows}</tbody>
            </table>
          </div>
        </section>
        """)

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Subject Access Report — {_esc(request_id)}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Arial, sans-serif; color: #1a1a1a; background: #fff; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }}
  h1 {{ font-size: 1.5rem; }}
  h2 {{ font-size: 1.15rem; border-bottom: 1px solid #ccc; padding-bottom: 0.25rem; margin-top: 2rem; }}
  h3 {{ font-family: ui-monospace, monospace; font-size: 1rem; }}
  .meta {{ color: #555; font-size: 0.85rem; }}
  .redacted-note {{ color: #8a5300; font-size: 0.85rem; }}
  .comment-note {{ color: #444; font-size: 0.85rem; }}
  .table-scroll {{ max-width: 100%; overflow-x: auto; margin: 0.5rem 0 1.5rem; }}
  table {{ border-collapse: collapse; width: max-content; font-size: 0.85rem; }}
  th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: left; vertical-align: top; }}
  th {{ background: #f2f2f2; }}
  .cell {{ display: inline-block; max-width: 300px; overflow-wrap: break-word; }}
  .col-comment {{ font-weight: normal; color: #666; font-size: 0.75rem; }}
  @media print {{
    @page {{ size: landscape; }}
    body {{ margin: 0; max-width: none; }}
    .table-section {{ page-break-inside: avoid; }}
    .table-scroll {{ overflow-x: visible; }}
    table {{ font-size: 0.7rem; }}
    th, td {{ padding: 2px 4px; }}
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
  <p>{_esc(recipients)}</p>

  <h2>Automated decision-making</h2>
  {adm_html}

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
