"""Erasure execution backend for confirmed SAR deletion requests.

``ErasureExecutor`` runs the actual ``DELETE`` for a reviewer-confirmed
selection and writes the audit trail to ``admin.erasure`` (owned by the
data_platform_admins team — see ``terraform/data-product-teams.tf`` and
``terraform/catalogs.tf: databricks_grants.admin_erasure``).

Hard constraints this module exists to satisfy (agreed DPO/GDPR review):
  - The audit trail is evidence that erasure happened, never a copy of the
    erased data — subject and row identifiers are always hashed via the
    ``admin.shared.hash_subject_ref``/``hash_row_key`` UDFs before being
    persisted, never stored as plaintext.
  - Every affected table gets its own ``request_items`` row, written
    immediately after that table's delete attempt (not batched until the
    whole request finishes), so a mid-run failure doesn't lose evidence for
    tables that already succeeded.
  - Row targeting must not delete more than the reviewer actually selected —
    see ``_dry_run_guard``.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd

from database import DatabricksClient

#: Default Delta VACUUM retention when a table has no explicit
#: delta.deletedFileRetentionDuration property set.
_DEFAULT_VACUUM_RETENTION_DAYS = 7


@dataclass
class TableErasureTarget:
    """One table's worth of reviewer-confirmed rows to delete."""

    full_name: str              # catalog.schema.table
    provenance: str              # "direct" | "upstream" | "downstream"
    matched_column_or_tag: str
    selected_rows: pd.DataFrame  # exactly the rows the reviewer selected


@dataclass
class TableErasureResult:
    """Outcome of executing one ``TableErasureTarget``."""

    full_name: str
    rows_selected: int
    rows_deleted: int
    row_targeting_method: str
    delete_sql: str              # for on-screen review only — never persisted raw
    vacuum_retention_raw: str
    vacuum_retention_days: int
    estimated_purge_by: datetime
    execution_status: str        # SUCCEEDED | FAILED | SKIPPED
    error_message: str | None


def _sql_string(val: object) -> str:
    """Return a quoted, escaped SQL string literal for *val*."""
    return "'" + str(val).replace("'", "''") + "'"


def _sql_timestamp(dt: datetime) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _row_predicate(row: pd.Series, columns: list[str]) -> str:
    """Build a single ``(col = 'val' AND ...)`` clause identifying *row*."""
    clauses = []
    for col in columns:
        val = row[col]
        if pd.isna(val):
            clauses.append(f"`{col}` IS NULL")
        else:
            clauses.append(f"CAST(`{col}` AS STRING) = {_sql_string(val)}")
    return "(" + " AND ".join(clauses) + ")"


def _parse_retention_days(raw: str | None) -> int:
    """Parse a Delta retention-duration string (e.g. ``'interval 7 days'``) to days."""
    if not raw:
        return _DEFAULT_VACUUM_RETENTION_DAYS
    match = re.search(r"(\d+)\s*day", raw, re.IGNORECASE)
    return int(match.group(1)) if match else _DEFAULT_VACUUM_RETENTION_DAYS


def get_vacuum_retention(client: DatabricksClient, full_name: str) -> tuple[str, int]:
    """Return ``(raw property value or default, retention in days)`` for *full_name*.

    Module-level (not tied to ``ErasureExecutor``) so the lineage map can show
    each table's actual VACUUM retention alongside the erasure-execution path
    that also needs it.
    """
    try:
        props = client.query(f"SHOW TBLPROPERTIES {full_name}")
        raw = next(
            (r["value"] for _, r in props.iterrows() if r["key"] == "delta.deletedFileRetentionDuration"),
            None,
        )
    except Exception:  # noqa: BLE001
        raw = None
    return raw or f"{_DEFAULT_VACUUM_RETENTION_DAYS} days (default — not explicitly set)", _parse_retention_days(raw)


def format_delete_sql_pretty(full_name: str, clauses: list[str]) -> str:
    """Multi-line, human-readable rendering of a DELETE statement for on-screen
    review — one OR-ed row predicate per line. Execution uses the compact
    single-line form from ``build_delete_sql``; this is display-only."""
    if not clauses:
        return f"DELETE FROM {full_name} WHERE FALSE;"
    lines = [f"DELETE FROM {full_name}", f"WHERE {clauses[0]}"]
    lines.extend(f"   OR {clause}" for clause in clauses[1:])
    return "\n".join(lines) + ";"


class ErasureExecutor:
    """Executes a confirmed erasure request and writes its audit trail.

    ``client`` must be the SAR app's own service-principal-backed
    ``DatabricksClient`` (see ``database.get_service_principal_token``) —
    no single non-admin principal has delete rights across every team's
    tables, so execution always escalates to the app's own SP, gated by the
    reviewer's confirmation.
    """

    def __init__(self, client: DatabricksClient) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_delete_sql(self, target: TableErasureTarget) -> tuple[str, str, list[str]]:
        """Return ``(row_targeting_method, delete_sql, predicate_clauses)`` for *target*.

        Exposed separately from ``execute_table`` so the review UI can show
        the reviewer the exact statement about to run *before* they confirm —
        this is purely an on-screen transparency aid; it is never persisted.
        """
        pk_columns = self._primary_key_columns(target.full_name)
        columns = pk_columns or list(target.selected_rows.columns)
        method = "primary_key" if pk_columns else "full_row_equality"

        clauses = [
            _row_predicate(row, columns)
            for _, row in target.selected_rows.iterrows()
        ]
        where = " OR ".join(clauses)
        sql = f"DELETE FROM {target.full_name} WHERE {where}"
        return method, sql, clauses

    def execute_table(self, target: TableErasureTarget) -> TableErasureResult:
        """Delete *target*'s selected rows, guarded by a pre-delete dry run."""
        vacuum_raw, vacuum_days = get_vacuum_retention(self._client, target.full_name)
        executed_at = datetime.now(timezone.utc)
        estimated_purge_by = executed_at + pd.Timedelta(days=vacuum_days)
        rows_selected = len(target.selected_rows)

        try:
            method, delete_sql, clauses = self.build_delete_sql(target)
        except Exception as exc:  # noqa: BLE001
            return TableErasureResult(
                full_name=target.full_name, rows_selected=rows_selected, rows_deleted=0,
                row_targeting_method="unknown", delete_sql="", vacuum_retention_raw=vacuum_raw,
                vacuum_retention_days=vacuum_days, estimated_purge_by=estimated_purge_by,
                execution_status="FAILED", error_message=f"Could not build delete predicate: {exc}",
            )

        # Dry-run guard: a full-row-equality predicate on a table with
        # duplicate rows (no natural key) could otherwise silently delete
        # more rows than the reviewer actually selected.
        where = " OR ".join(clauses)
        try:
            count_df = self._client.query(f"SELECT COUNT(*) AS n FROM {target.full_name} WHERE {where}")
            matched_count = int(count_df.iloc[0]["n"])
        except Exception as exc:  # noqa: BLE001
            return TableErasureResult(
                full_name=target.full_name, rows_selected=rows_selected, rows_deleted=0,
                row_targeting_method=method, delete_sql=delete_sql, vacuum_retention_raw=vacuum_raw,
                vacuum_retention_days=vacuum_days, estimated_purge_by=estimated_purge_by,
                execution_status="FAILED", error_message=f"Dry-run count query failed: {exc}",
            )

        if matched_count != rows_selected:
            return TableErasureResult(
                full_name=target.full_name, rows_selected=rows_selected, rows_deleted=0,
                row_targeting_method=method, delete_sql=delete_sql, vacuum_retention_raw=vacuum_raw,
                vacuum_retention_days=vacuum_days, estimated_purge_by=estimated_purge_by,
                execution_status="SKIPPED",
                error_message=(
                    f"Dry-run count ({matched_count}) did not match rows selected "
                    f"({rows_selected}) — aborted without deleting to avoid removing "
                    f"more than reviewed."
                ),
            )

        try:
            rows_deleted = self._client.execute(delete_sql)
        except Exception as exc:  # noqa: BLE001
            return TableErasureResult(
                full_name=target.full_name, rows_selected=rows_selected, rows_deleted=0,
                row_targeting_method=method, delete_sql=delete_sql, vacuum_retention_raw=vacuum_raw,
                vacuum_retention_days=vacuum_days, estimated_purge_by=estimated_purge_by,
                execution_status="FAILED", error_message=str(exc),
            )

        return TableErasureResult(
            full_name=target.full_name, rows_selected=rows_selected, rows_deleted=rows_deleted,
            row_targeting_method=method, delete_sql=delete_sql, vacuum_retention_raw=vacuum_raw,
            vacuum_retention_days=vacuum_days, estimated_purge_by=estimated_purge_by,
            execution_status="SUCCEEDED", error_message=None,
        )

    def run(
        self,
        subject_ref: str,
        requested_by: str,
        legal_basis: str,
        targets: list[TableErasureTarget],
    ) -> tuple[str, list[TableErasureResult]]:
        """Execute every target, writing the audit trail incrementally.

        Returns ``(request_id, results)``. ``subject_ref`` is the plaintext
        identifier used only to compute ``subject_ref_hash`` — never stored.
        """
        request_id = str(uuid.uuid4())
        requested_at = datetime.now(timezone.utc)
        subject_ref_hash = self._hash_value("hash_subject_ref", subject_ref)

        self._client.execute(f"""
            INSERT INTO admin.erasure.requests
            (request_id, subject_ref_hash, requested_by, legal_basis, requested_at, status, completed_at)
            VALUES ({_sql_string(request_id)}, {_sql_string(subject_ref_hash)},
                    {_sql_string(requested_by)}, {_sql_string(legal_basis)},
                    {_sql_timestamp(requested_at)}, 'PENDING', NULL)
        """)

        results: list[TableErasureResult] = []
        for target in targets:
            result = self.execute_table(target)
            results.append(result)
            self._write_request_item(request_id, target, result)

        overall_status = (
            "COMPLETED" if all(r.execution_status == "SUCCEEDED" for r in results)
            else "FAILED" if all(r.execution_status == "FAILED" for r in results)
            else "PARTIAL"
        )
        completed_at = datetime.now(timezone.utc)
        self._client.execute(f"""
            UPDATE admin.erasure.requests
            SET status = {_sql_string(overall_status)}, completed_at = {_sql_timestamp(completed_at)}
            WHERE request_id = {_sql_string(request_id)}
        """)

        return request_id, results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _primary_key_columns(self, full_name: str) -> list[str] | None:
        """Return the UC-declared primary key columns for *full_name*, if any."""
        catalog, schema, table = full_name.split(".", 2)
        df = self._client.query(f"""
            SELECT kcu.column_name
            FROM   {catalog}.information_schema.key_column_usage kcu
            JOIN   {catalog}.information_schema.table_constraints tc
                   ON  kcu.constraint_name = tc.constraint_name
                   AND kcu.table_schema    = tc.table_schema
                   AND kcu.table_name      = tc.table_name
            WHERE  tc.constraint_type = 'PRIMARY KEY'
              AND  kcu.table_schema   = '{schema}'
              AND  kcu.table_name     = '{table}'
            ORDER BY kcu.ordinal_position
        """)
        return list(df["column_name"]) if not df.empty else None

    def _hash_value(self, udf_name: str, val: str) -> str:
        df = self._client.query(f"SELECT admin.shared.{udf_name}({_sql_string(val)}) AS h")
        return str(df.iloc[0]["h"])

    def _hash_row_keys(self, target: TableErasureTarget) -> list[str]:
        """Hash a canonical per-row key (all column values joined) for each selected row."""
        keys = [
            "|".join(str(row[col]) for col in target.selected_rows.columns)
            for _, row in target.selected_rows.iterrows()
        ]
        if not keys:
            return []
        values_clause = ", ".join(f"({_sql_string(k)})" for k in keys)
        df = self._client.query(f"""
            SELECT admin.shared.hash_row_key(val) AS h
            FROM   (VALUES {values_clause}) AS t(val)
        """)
        return list(df["h"])

    def _write_request_item(
        self,
        request_id: str,
        target: TableErasureTarget,
        result: TableErasureResult,
    ) -> None:
        row_key_hashes = self._hash_row_keys(target) if result.rows_deleted else []
        hash_array_sql = (
            "ARRAY(" + ", ".join(_sql_string(h) for h in row_key_hashes) + ")"
            if row_key_hashes else "CAST(ARRAY() AS ARRAY<STRING>)"
        )
        executed_at = datetime.now(timezone.utc)

        self._client.execute(f"""
            INSERT INTO admin.erasure.request_items
            (request_id, table_full_name, provenance, matched_column_or_tag, rows_selected,
             rows_deleted, row_targeting_method, row_key_hash, vacuum_retention_raw,
             vacuum_retention_days, estimated_purge_by, execution_status, error_message, executed_at)
            VALUES (
                {_sql_string(request_id)}, {_sql_string(target.full_name)},
                {_sql_string(target.provenance)}, {_sql_string(target.matched_column_or_tag)},
                {result.rows_selected}, {result.rows_deleted},
                {_sql_string(result.row_targeting_method)}, {hash_array_sql},
                {_sql_string(result.vacuum_retention_raw)}, {result.vacuum_retention_days},
                {_sql_timestamp(result.estimated_purge_by)}, {_sql_string(result.execution_status)},
                {_sql_string(result.error_message) if result.error_message else "NULL"},
                {_sql_timestamp(executed_at)}
            )
        """)
