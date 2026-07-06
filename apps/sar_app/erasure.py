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
from datetime import date, datetime, timezone

import numpy as np
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
    execution_status: str        # SUCCEEDED | FAILED | SKIPPED | ABORTED
    error_message: str | None


@dataclass
class DryRunCheck:
    """Result of verifying one target's delete predicate *before* deleting anything.

    ``ErasureExecutor.run`` dry-runs every target in a request first and only
    proceeds to delete any of them if every single one checks out — this is
    what makes the request all-or-nothing without native DB transactions
    (which Databricks doesn't support here anyway: tables with column masks,
    which silver/gold both have, cannot participate in a
    BEGIN TRANSACTION/BEGIN ATOMIC block at all). Checking every table before
    touching any of them means a mismatch on one table can never leave an
    earlier table already deleted — the exact failure mode a real erasure hit
    (silver+gold deleted while bronze's mismatch was only caught after the
    fact, per-table, with no way to undo the other two automatically).
    """

    full_name: str
    row_targeting_method: str
    delete_sql: str
    rows_selected: int
    matched_count: int
    vacuum_retention_raw: str
    vacuum_retention_days: int
    error_message: str | None     # None means this target's dry run passed

    @property
    def ok(self) -> bool:
        return self.error_message is None


def _sql_string(val: object) -> str:
    """Return a quoted, escaped SQL string literal for *val*.

    Backslashes must be escaped, not just single quotes — Databricks SQL
    string literals process C-style escape sequences (\\n, \\u2013, etc.),
    so any text containing literal backslashes (e.g. a JSON blob with its
    own \\n/\\u-escaped content) gets silently reinterpreted into different
    characters than what's actually stored, permanently breaking any
    equality comparison against that column. Verified: this was the real
    reason a bronze table with a raw_payload JSON column kept failing its
    dry-run count even after the timestamp-precision predicate was fixed.
    """
    escaped = str(val).replace("\\", "\\\\").replace("'", "''")
    return "'" + escaped + "'"


def _sql_timestamp(dt: datetime) -> str:
    # %f (microseconds) matters here, not just cosmetically — truncating to
    # whole seconds means this literal silently stops matching any row whose
    # real column value has sub-second precision (routine for audit columns
    # like ingested_at/_inserted_at set via current_timestamp()), which made
    # the dry-run count come back 0 for every table with such a column.
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _sql_literal(val: object) -> tuple[str, bool]:
    """Return ``(literal, needs_string_cast)`` for *val*.

    ``needs_string_cast`` is True only for the generic/fallback branch —
    callers building a column predicate must compare against it as
    ``CAST(col AS STRING) = literal`` rather than ``col = literal``, since
    the column's declared type isn't one of the natively-handled ones
    below. Callers building bare ``INSERT ... VALUES`` rows can ignore it
    and use ``literal`` directly (the target column's own declared type
    governs there, no cast needed).

    Uses a type-appropriate literal rather than ``CAST(col AS STRING) =
    'val'`` for everything — Databricks' own TIMESTAMP/DATE-to-string
    serialization doesn't necessarily match pandas' ``str()`` rendering of
    the same value character-for-character (e.g. timezone suffix,
    fractional seconds), which silently made the dry-run count come back 0
    for any table with a timestamp/date column and no declared primary
    key. Shared between predicate-building (``_row_predicate``) and
    restore's ``INSERT ... VALUES`` rows — the exact same type-formatting
    risk applies to both.
    """
    if pd.isna(val):
        return "NULL", False
    if isinstance(val, (pd.Timestamp, datetime)):
        return _sql_timestamp(val), False
    if isinstance(val, date):
        return f"DATE '{val.strftime('%Y-%m-%d')}'", False
    if isinstance(val, (bool, np.bool_)):
        return ("TRUE" if val else "FALSE"), False
    if isinstance(val, (int, float, np.integer, np.floating)):
        # str(), not repr() — numpy >=2.0 changed scalar repr() to
        # "np.int64(5)" instead of a bare "5", which would break the SQL.
        return f"{val!s}", False
    return _sql_string(val), True


def _row_predicate(row: pd.Series, columns: list[str]) -> str:
    """Build a single ``(col = val AND ...)`` clause identifying *row*."""
    clauses = []
    for col in columns:
        lit, needs_cast = _sql_literal(row[col])
        if lit == "NULL":
            clauses.append(f"`{col}` IS NULL")
        elif needs_cast:
            clauses.append(f"CAST(`{col}` AS STRING) = {lit}")
        else:
            clauses.append(f"`{col}` = {lit}")
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


def hash_row_keys(client: DatabricksClient, df: pd.DataFrame, columns: list[str]) -> list[str]:
    """Hash a canonical per-row key (all *columns* values joined) for each row in *df*.

    Module-level (not tied to ``ErasureExecutor``) so restore can hash a
    time-travelled historical snapshot the exact same way a delete hashed
    the rows it removed — reusing this one function, rather than
    reimplementing the join/format logic in SQL, is what keeps the two
    sides guaranteed consistent (see ``restore_request_item``).
    """
    keys = ["|".join(str(row[col]) for col in columns) for _, row in df.iterrows()]
    if not keys:
        return []
    values_clause = ", ".join(f"({_sql_string(k)})" for k in keys)
    result = client.query(f"""
        SELECT admin.shared.hash_row_key(val) AS h
        FROM   (VALUES {values_clause}) AS t(val)
    """)
    return list(result["h"])


def find_pre_delete_version(client: DatabricksClient, table_full_name: str, executed_at: object) -> dict | None:
    """Find the Delta version just before the DELETE closest to *executed_at*.

    ``admin.erasure.request_items`` doesn't record which Delta version
    preceded its delete, so this correlates by time: among this table's
    ``DESCRIBE HISTORY`` rows with ``operation = 'DELETE'``, picks the one
    whose commit timestamp is closest to *executed_at* (that audit
    timestamp is written within the same synchronous flow as the delete
    itself, so it should be within a second or two of the real commit).
    Returns a dict with the matched version, its timestamp, how far off
    the match was, and its operationMetrics (shown to the reviewer so they
    can sanity-check the match before confirming a restore) — or ``None``
    if the table has no DELETE history at all.
    """
    try:
        history = client.query(f"DESCRIBE HISTORY {table_full_name}")
    except Exception:  # noqa: BLE001
        return None

    deletes = history[history["operation"] == "DELETE"].copy()
    if deletes.empty:
        return None

    deletes["timestamp"] = pd.to_datetime(deletes["timestamp"], utc=True)
    executed_at_ts = pd.Timestamp(executed_at)
    executed_at_ts = (
        executed_at_ts.tz_localize("UTC") if executed_at_ts.tzinfo is None
        else executed_at_ts.tz_convert("UTC")
    )
    deletes["_diff"] = (deletes["timestamp"] - executed_at_ts).abs()
    best = deletes.sort_values("_diff").iloc[0]
    matched_version = int(best["version"])

    return {
        "pre_delete_version": matched_version - 1,
        "matched_version": matched_version,
        "matched_timestamp": best["timestamp"],
        "time_diff_seconds": best["_diff"].total_seconds(),
        "operation_metrics": best.get("operationMetrics"),
    }


def format_delete_sql_pretty(full_name: str, clauses: list[str]) -> str:
    """Multi-line, human-readable rendering of a DELETE statement for on-screen
    review — one OR-ed row predicate per line. Execution uses the compact
    single-line form from ``build_delete_sql``; this is display-only."""
    if not clauses:
        return f"DELETE FROM {full_name} WHERE FALSE;"
    lines = [f"DELETE FROM {full_name}", f"WHERE {clauses[0]}"]
    lines.extend(f"   OR {clause}" for clause in clauses[1:])
    return "\n".join(lines) + ";"


@dataclass
class RestorationResult:
    """Outcome of attempting to restore one ``request_items`` row's deleted rows."""

    table_full_name: str
    rows_restored: int
    pre_delete_version: int | None
    execution_status: str        # SUCCEEDED | FAILED
    error_message: str | None


def restore_request_item(client: DatabricksClient, item: pd.Series) -> RestorationResult:
    """Surgically reinsert the rows described by one ``admin.erasure.request_items`` row.

    *item* is a row from ``request_items`` (``table_full_name``,
    ``executed_at``, ``row_key_hash``, ...). Finds the pre-delete Delta
    version, time-travels to it, hashes every historical row the same way
    the original delete did (``hash_row_keys`` — reused, not
    reimplemented in SQL, so the two sides can't drift out of format sync),
    keeps only rows whose hash is in *item*'s ``row_key_hash``, and inserts
    them back. Never touches rows unrelated to this specific erasure,
    unlike a full ``RESTORE TABLE``.
    """
    table_full_name = item["table_full_name"]
    raw_hashes = item["row_key_hash"]
    stored_hashes = set(raw_hashes) if raw_hashes is not None and len(raw_hashes) > 0 else set()

    if not stored_hashes:
        return RestorationResult(
            table_full_name, 0, None, "FAILED",
            "No row_key_hash values recorded for this item — nothing to restore.",
        )

    version_info = find_pre_delete_version(client, table_full_name, item["executed_at"])
    if version_info is None:
        return RestorationResult(
            table_full_name, 0, None, "FAILED",
            "Could not find a matching DELETE operation in this table's history.",
        )
    pre_version = version_info["pre_delete_version"]

    try:
        snapshot = client.query(f"SELECT * FROM {table_full_name} VERSION AS OF {pre_version}")
    except Exception as exc:  # noqa: BLE001
        return RestorationResult(
            table_full_name, 0, pre_version, "FAILED",
            f"Could not read historical version {pre_version} "
            f"(it may already be past its VACUUM retention window): {exc}",
        )
    if snapshot.empty:
        return RestorationResult(table_full_name, 0, pre_version, "FAILED", "Historical version has no rows.")

    columns = list(snapshot.columns)
    try:
        row_hashes = hash_row_keys(client, snapshot, columns)
    except Exception as exc:  # noqa: BLE001
        return RestorationResult(table_full_name, 0, pre_version, "FAILED", f"Could not hash historical rows: {exc}")

    matched_rows = snapshot[[h in stored_hashes for h in row_hashes]]
    if len(matched_rows) != len(stored_hashes):
        return RestorationResult(
            table_full_name, 0, pre_version, "FAILED",
            f"Matched {len(matched_rows)} historical row(s) but expected {len(stored_hashes)} "
            f"(based on stored row hashes) — aborted without inserting to avoid a partial or "
            f"incorrect restore.",
        )

    value_rows = [
        "(" + ", ".join(_sql_literal(row[col])[0] for col in columns) + ")"
        for _, row in matched_rows.iterrows()
    ]
    column_list = ", ".join(f"`{c}`" for c in columns)
    insert_sql = f"INSERT INTO {table_full_name} ({column_list}) VALUES {', '.join(value_rows)}"

    try:
        client.execute(insert_sql)
    except Exception as exc:  # noqa: BLE001
        return RestorationResult(table_full_name, 0, pre_version, "FAILED", str(exc))

    return RestorationResult(table_full_name, len(matched_rows), pre_version, "SUCCEEDED", None)


def write_restoration(
    client: DatabricksClient,
    request_id: str,
    restored_by: str,
    reason: str,
    result: RestorationResult,
) -> None:
    """Write one row to ``admin.erasure.restorations`` — evidence a restore was attempted."""
    restoration_id = str(uuid.uuid4())
    restored_at = datetime.now(timezone.utc)
    client.execute(f"""
        INSERT INTO admin.erasure.restorations
        (restoration_id, request_id, table_full_name, restored_by, restoration_reason,
         pre_delete_version, rows_restored, execution_status, error_message, restored_at)
        VALUES (
            {_sql_string(restoration_id)}, {_sql_string(request_id)}, {_sql_string(result.table_full_name)},
            {_sql_string(restored_by)}, {_sql_string(reason)},
            {result.pre_delete_version if result.pre_delete_version is not None else "NULL"},
            {result.rows_restored}, {_sql_string(result.execution_status)},
            {_sql_string(result.error_message) if result.error_message else "NULL"},
            {_sql_timestamp(restored_at)}
        )
    """)


def list_requests(client: DatabricksClient) -> pd.DataFrame:
    """Return all rows from ``admin.erasure.requests``, most recent first."""
    return client.query("SELECT * FROM admin.erasure.requests ORDER BY requested_at DESC")


def list_request_items(client: DatabricksClient, request_id: str) -> pd.DataFrame:
    """Return all ``request_items`` rows for *request_id*, in execution order."""
    return client.query(f"""
        SELECT * FROM admin.erasure.request_items
        WHERE request_id = {_sql_string(request_id)}
        ORDER BY executed_at
    """)


def list_restorations(client: DatabricksClient, request_id: str) -> pd.DataFrame:
    """Return all ``restorations`` rows for *request_id* — used to tell whether a
    given request_item already has a successful restore, so its button/state
    can reflect that instead of allowing a duplicate reinsertion."""
    return client.query(f"""
        SELECT * FROM admin.erasure.restorations
        WHERE request_id = {_sql_string(request_id)}
        ORDER BY restored_at
    """)


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

    def dry_run_check(self, target: TableErasureTarget) -> DryRunCheck:
        """Verify *target*'s delete predicate without deleting anything.

        A full-row-equality predicate on a table with duplicate rows (no
        natural key) could otherwise silently delete more rows than the
        reviewer actually selected — this confirms the predicate matches
        exactly ``rows_selected`` rows before ``run`` lets any target in the
        request proceed to an actual delete.
        """
        vacuum_raw, vacuum_days = get_vacuum_retention(self._client, target.full_name)
        rows_selected = len(target.selected_rows)

        try:
            method, delete_sql, clauses = self.build_delete_sql(target)
        except Exception as exc:  # noqa: BLE001
            return DryRunCheck(
                full_name=target.full_name, row_targeting_method="unknown", delete_sql="",
                rows_selected=rows_selected, matched_count=0, vacuum_retention_raw=vacuum_raw,
                vacuum_retention_days=vacuum_days,
                error_message=f"Could not build delete predicate: {exc}",
            )

        where = " OR ".join(clauses)
        try:
            count_df = self._client.query(f"SELECT COUNT(*) AS n FROM {target.full_name} WHERE {where}")
            matched_count = int(count_df.iloc[0]["n"])
        except Exception as exc:  # noqa: BLE001
            return DryRunCheck(
                full_name=target.full_name, row_targeting_method=method, delete_sql=delete_sql,
                rows_selected=rows_selected, matched_count=0, vacuum_retention_raw=vacuum_raw,
                vacuum_retention_days=vacuum_days,
                error_message=f"Dry-run count query failed: {exc}",
            )

        error_message = None
        if matched_count != rows_selected:
            error_message = (
                f"Dry-run count ({matched_count}) did not match rows selected ({rows_selected})."
            )
        return DryRunCheck(
            full_name=target.full_name, row_targeting_method=method, delete_sql=delete_sql,
            rows_selected=rows_selected, matched_count=matched_count, vacuum_retention_raw=vacuum_raw,
            vacuum_retention_days=vacuum_days, error_message=error_message,
        )

    def execute_table(self, dry_run: DryRunCheck) -> TableErasureResult:
        """Delete the rows verified by *dry_run*.

        Caller (``run``) must only call this once every target in the same
        request has passed ``dry_run_check`` — this performs no count
        verification of its own, it trusts ``dry_run.matched_count``.
        """
        estimated_purge_by = datetime.now(timezone.utc) + pd.Timedelta(days=dry_run.vacuum_retention_days)
        try:
            # Not self._client.execute(...)'s return value — the Databricks
            # SQL connector's cursor.rowcount comes back -1 for DELETE (the
            # standard DB-API convention for "driver doesn't report affected
            # rows"). dry_run.matched_count is exactly right instead: the
            # dry-run check already confirmed this identical predicate
            # matches precisely rows_selected rows.
            self._client.execute(dry_run.delete_sql)
        except Exception as exc:  # noqa: BLE001
            return TableErasureResult(
                full_name=dry_run.full_name, rows_selected=dry_run.rows_selected, rows_deleted=0,
                row_targeting_method=dry_run.row_targeting_method, delete_sql=dry_run.delete_sql,
                vacuum_retention_raw=dry_run.vacuum_retention_raw, vacuum_retention_days=dry_run.vacuum_retention_days,
                estimated_purge_by=estimated_purge_by, execution_status="FAILED", error_message=str(exc),
            )

        return TableErasureResult(
            full_name=dry_run.full_name, rows_selected=dry_run.rows_selected, rows_deleted=dry_run.matched_count,
            row_targeting_method=dry_run.row_targeting_method, delete_sql=dry_run.delete_sql,
            vacuum_retention_raw=dry_run.vacuum_retention_raw, vacuum_retention_days=dry_run.vacuum_retention_days,
            estimated_purge_by=estimated_purge_by, execution_status="SUCCEEDED", error_message=None,
        )

    def run(
        self,
        subject_ref: str,
        requested_by: str,
        legal_basis: str,
        targets: list[TableErasureTarget],
    ) -> tuple[str, list[TableErasureResult]]:
        """Dry-run every target, and only if *all* pass, delete any of them.

        This request is all-or-nothing: every target is dry-run *before* any
        delete is attempted, so a mismatch on one table can never leave an
        earlier table already deleted. (Native Databricks transactions can't
        provide this instead — tables with column masks, which silver/gold
        both have, cannot participate in a transaction at all, per
        https://docs.databricks.com/aws/en/transactions/.)

        Returns ``(request_id, results)``. ``subject_ref`` is the plaintext
        identifier used only to compute ``subject_ref_hash`` — never stored.
        The audit trail is still written incrementally, one request_items row
        per target as each is resolved, so a mid-run failure during the
        delete phase doesn't lose evidence for targets already written.
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

        dry_runs = [self.dry_run_check(target) for target in targets]
        all_ok = all(d.ok for d in dry_runs)

        results: list[TableErasureResult] = []
        for target, dry_run in zip(targets, dry_runs):
            if all_ok:
                result = self.execute_table(dry_run)
            else:
                reason = dry_run.error_message or (
                    "Aborted before deleting — a different table in this request failed "
                    "its dry-run check, so nothing in this request was deleted."
                )
                result = TableErasureResult(
                    full_name=dry_run.full_name, rows_selected=dry_run.rows_selected, rows_deleted=0,
                    row_targeting_method=dry_run.row_targeting_method, delete_sql=dry_run.delete_sql,
                    vacuum_retention_raw=dry_run.vacuum_retention_raw,
                    vacuum_retention_days=dry_run.vacuum_retention_days,
                    estimated_purge_by=datetime.now(timezone.utc) + pd.Timedelta(days=dry_run.vacuum_retention_days),
                    execution_status="ABORTED", error_message=reason,
                )
            results.append(result)
            self._write_request_item(request_id, target, result)

        overall_status = (
            "COMPLETED" if all_ok and all(r.execution_status == "SUCCEEDED" for r in results)
            else "ABORTED" if not all_ok
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

    def _write_request_item(
        self,
        request_id: str,
        target: TableErasureTarget,
        result: TableErasureResult,
    ) -> None:
        row_key_hashes = (
            hash_row_keys(self._client, target.selected_rows, list(target.selected_rows.columns))
            if result.rows_deleted else []
        )
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
