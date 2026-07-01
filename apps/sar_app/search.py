"""SAR table search orchestration and fuzzy post-filtering.

``SARSearcher`` builds SQL WHERE clauses for each tagged identifier type,
executes the query via ``DatabricksClient``, and applies a rapidfuzz WRatio
post-filter for name conditions.  A row must satisfy *all* conditions to be
returned.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from database import DatabricksClient
from matching import NameMatcher
from normalise import SearchNormaliser

#: Tags that use LIKE containment in SQL rather than exact equality.
LIKE_TAGS: frozenset[str] = frozenset({"class.location"})


class SARSearcher:
    """Executes tagged-column searches against a single Delta table.

    Builds parameterised SQL WHERE clauses for each identifier type and
    applies a rapidfuzz WRatio post-filter for name matches. A row must
    satisfy *all* supplied conditions to be returned.
    """

    def __init__(self, client: DatabricksClient, name_matcher: NameMatcher) -> None:
        self._client = client
        self._matcher = name_matcher

    def search(
        self,
        catalog: str,
        schema: str,
        table: str,
        conditions: list[tuple[list[str], str, str]],
        fuzzy_threshold: int = 75,
    ) -> pd.DataFrame:
        """Search ``catalog.schema.table`` with *conditions*.

        Args:
            catalog: Unity Catalog catalog name.
            schema: Schema name.
            table: Table name.
            conditions: ``(columns, clean_value, tag)`` tuples.
                *columns* is the list of tagged columns for that identifier,
                *clean_value* is the normalised search string, and *tag* is
                the ``class.*`` tag name.
            fuzzy_threshold: Minimum WRatio score (0–100) for name matches.

        Returns:
            Matched rows. Name-condition rows include a ``_match_score``
            column. Empty DataFrame when nothing matched.
        """
        clauses: list[str] = []
        name_conditions: list[tuple[list[str], str]] = []

        for cols, val, tag in conditions:
            clause, name_cond = self._build_clause(cols, val, tag)
            clauses.append(clause)
            if name_cond is not None:
                name_conditions.append(name_cond)

        sql = (
            f"SELECT * FROM {catalog}.`{schema}`.`{table}`"
            f" WHERE {' AND '.join(clauses)}"
            f" LIMIT 500"
        )
        df = self._client.query(sql)

        if df.empty or not name_conditions:
            return df

        return self._apply_fuzzy_filter(df, name_conditions, fuzzy_threshold)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_clause(
        self,
        cols: list[str],
        val: str,
        tag: str,
    ) -> tuple[str, Optional[tuple[list[str], str]]]:
        """Return ``(sql_clause, name_condition_or_None)`` for one condition.

        When a table has more than one column tagged with the same identifier
        (e.g. both ``home_phone`` and ``mobile_phone`` tagged
        ``class.phone_number``), each column is checked and the results are
        ORed together so a match in any of them counts.
        """
        if tag == "class.name":
            return self._matcher.sql_clause(cols, val), (cols, val)

        safe = val.replace("'", "''")
        col_clauses = [self._column_clause(col, val, safe, tag) for col in cols]
        return "(" + " OR ".join(col_clauses) + ")", None

    def _column_clause(self, col: str, val: str, safe: str, tag: str) -> str:
        """Return the SQL clause for a single non-name column."""
        if tag == "class.phone_number":
            norm = f"RIGHT(REGEXP_REPLACE(CAST(`{col}` AS STRING), '[^0-9]', ''), 9)"
            return f"{norm} = '{safe}'"

        if tag in LIKE_TAGS:
            if SearchNormaliser.is_clean_postcode(val):
                norm = f"REGEXP_REPLACE(UPPER(CAST(`{col}` AS STRING)), ' +', '')"
                return f"{norm} LIKE '%{safe}%'"
            return f"LOWER(CAST(`{col}` AS STRING)) LIKE LOWER('%{safe}%')"

        return f"LOWER(CAST(`{col}` AS STRING)) = LOWER('{safe}')"

    def _apply_fuzzy_filter(
        self,
        df: pd.DataFrame,
        name_conditions: list[tuple[list[str], str]],
        threshold: int,
    ) -> pd.DataFrame:
        """Drop rows where any name condition scores below *threshold*."""

        def _score(row: pd.Series) -> int:
            # Row must satisfy ALL name conditions; take the minimum across them
            return min(
                self._matcher.row_score(row, cols, val)
                for cols, val in name_conditions
            )

        df = df.copy()
        df["_match_score"] = df.apply(_score, axis=1)
        df = df[df["_match_score"] >= threshold]
        return df.sort_values("_match_score", ascending=False).reset_index(drop=True)
