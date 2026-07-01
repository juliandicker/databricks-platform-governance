"""Name column grouping, SQL clause construction, and fuzzy scoring.

``NameMatcher`` holds the NickNamer instance and owns all name-specific
logic: grouping tagged columns into per-person slots, building SQL WHERE
clauses with nickname expansion, and computing per-slot WRatio scores.
"""

from __future__ import annotations

import re

import pandas as pd
from nicknames import NickNamer
from rapidfuzz import fuzz


class NameMatcher:
    """Groups tagged name columns into per-person slots and builds SQL clauses.

    Columns such as ``firstname_1, lastname_1, firstname_2, lastname_2`` are
    automatically grouped so each person's fields are searched together and
    scored independently by WRatio.

    Column grouping strategy (in priority order):

    1. **Numeric** — ``_N`` suffix / ``_N_`` infix groups by the number.
    2. **Common character prefix** — uses the shared prefix as the slot key
       unless that prefix is itself a name-field keyword (firstname, lastname …),
       in which case the role lives at the other end of the column name.
    3. **Common character suffix** — used when the prefix is a name-field token
       or too short (< 3 chars).
    4. **Fallback** — all columns in one slot when no discriminator is found.
    """

    #: Tokens that identify a name *field type* rather than a person/role.
    _NAME_FIELD_TOKENS: frozenset[str] = frozenset({
        "name", "firstname", "lastname", "surname", "forename",
        "fname", "lname", "sname", "givenname", "familyname",
        "middlename", "fullname",
    })

    _MIN_PREFIX: int = 3  # Fewer shared chars than this is coincidence, not a role prefix

    def __init__(self) -> None:
        self._namer = NickNamer()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def variants(self, token: str) -> list[str]:
        """Return *token* plus all bidirectional nickname variants.

        Calls both ``nicknames_of`` (canonical → nicknames) and
        ``canonicals_of`` (nickname → canonical) so that searching "Tony"
        also matches "Anthony", "Ant", etc. and vice versa.
        """
        result: set[str] = {token.lower()}
        for form in (token, token.capitalize()):
            result.update(v.lower() for v in self._namer.nicknames_of(form))
            result.update(v.lower() for v in self._namer.canonicals_of(form))
        return list(result)

    def group_columns(self, cols: list[str]) -> dict[str, list[str]]:
        """Group *cols* into per-person slots (see class docstring for strategy)."""
        if len(cols) == 1:
            return {"_slot": list(cols)}

        if any(re.search(r"_(\d+)(?:_|$)", col) for col in cols):
            return self._group_by_number(cols)

        grouped: dict[str, list[str]] = {}
        for col in cols:
            grouped.setdefault(self._slot_key(col, cols), []).append(col)

        if all(len(v) == 1 for v in grouped.values()):
            # No useful discriminator — treat all columns as one person's fields
            return {"_slot": sorted(cols)}

        return {k: sorted(v) for k, v in grouped.items()}

    def sql_clause(self, cols: list[str], clean_val: str) -> str:
        """Build a WHERE clause that searches *clean_val* across *cols*.

        Columns are grouped into per-person slots; each slot is matched
        independently and the slots are ORed together::

            (CONCAT(firstname_1, lastname_1) LIKE '%tony%' AND ... LIKE '%hill%')
            OR
            (CONCAT(firstname_2, lastname_2) LIKE '%tony%' AND ... LIKE '%hill%')

        Nickname expansion is applied to every token in every slot.
        """
        groups = self.group_columns(cols)
        slot_clauses = [
            self._slot_clause(group_cols, clean_val)
            for group_cols in groups.values()
        ]
        return "(" + " OR ".join(slot_clauses) + ")"

    def row_score(self, row: pd.Series, cols: list[str], clean_val: str) -> int:
        """Return the best WRatio score across all person-slots for *row*.

        Concatenates each slot's columns into a composite string and scores
        it with WRatio. Returns the maximum slot score so a strong match in
        any slot is not masked by a weak match in another.
        """
        groups = self.group_columns(cols)
        scores = [
            fuzz.WRatio(
                clean_val.lower(),
                " ".join(str(row.get(c, "")) for c in group_cols).strip().lower(),
            )
            for group_cols in groups.values()
            if any(row.get(c) for c in group_cols)
        ]
        return max(scores) if scores else 0

    # ------------------------------------------------------------------
    # Private — grouping
    # ------------------------------------------------------------------

    def _group_by_number(self, cols: list[str]) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for col in cols:
            m = re.search(r"_(\d+)(?:_|$)", col)
            key = m.group(1) if m else "_unnumbered"
            groups.setdefault(key, []).append(col)

        def _sort_key(k: str) -> tuple:
            return (0, int(k)) if k != "_unnumbered" else (1, 0)

        return {k: sorted(groups[k]) for k in sorted(groups, key=_sort_key)}

    def _slot_key(self, col: str, all_cols: list[str]) -> str:
        """Determine the slot key for *col* using prefix/suffix heuristics."""
        best_prefix = max(
            (self._char_prefix_len(col, other) for other in all_cols if other != col),
            default=0,
        )

        if best_prefix >= self._MIN_PREFIX and not self._is_name_field(col[:best_prefix]):
            prefix = col[:best_prefix]
            suffix = col[best_prefix:]
            # Digit immediately after the shared prefix → number distinguishes persons
            if suffix and suffix[0].isdigit():
                digit_end = next(
                    (i for i, c in enumerate(suffix) if not c.isdigit()),
                    len(suffix),
                )
                return prefix + suffix[:digit_end]
            return prefix

        # Prefix is a name-field keyword or too short — role lives at the other end
        best_suffix = max(
            (self._char_suffix_len(col, other) for other in all_cols if other != col),
            default=0,
        )
        if best_suffix >= self._MIN_PREFIX and not self._is_name_field(col[-best_suffix:]):
            return col[-best_suffix:]

        return col  # No usable signal — own slot (may trigger all-singleton fallback)

    # ------------------------------------------------------------------
    # Private — SQL construction
    # ------------------------------------------------------------------

    def _slot_clause(self, group_cols: list[str], clean_val: str) -> str:
        col_expr = self._col_expr(group_cols)
        token_parts = [
            "("
            + " OR ".join(
                f"{col_expr} LIKE '%{v.replace(chr(39), chr(39) * 2)}%'"
                for v in self.variants(token)
            )
            + ")"
            for token in clean_val.lower().split()
        ]
        return "(" + " AND ".join(token_parts) + ")"

    @staticmethod
    def _col_expr(group_cols: list[str]) -> str:
        if len(group_cols) == 1:
            return f"LOWER(CAST(`{group_cols[0]}` AS STRING))"
        parts = ", ".join(f"CAST(`{c}` AS STRING)" for c in group_cols)
        return f"LOWER(CONCAT_WS(' ', {parts}))"

    # ------------------------------------------------------------------
    # Private — character-level helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _char_prefix_len(a: str, b: str) -> int:
        n = 0
        for x, y in zip(a, b):
            if x == y:
                n += 1
            else:
                break
        return n

    @staticmethod
    def _char_suffix_len(a: str, b: str) -> int:
        n = 0
        for x, y in zip(reversed(a), reversed(b)):
            if x == y:
                n += 1
            else:
                break
        return n

    def _is_name_field(self, s: str) -> bool:
        """Return ``True`` when *s* is or ends with a name-field keyword.

        Uses ``endswith`` rather than substring containment to avoid false
        positives on role names that happen to contain keyword letters
        (e.g. "manager" does not end with any name-field keyword).
        """
        clean = re.sub(r"[_\s]+", "", s).lower()
        return any(clean == kw or clean.endswith(kw) for kw in self._NAME_FIELD_TOKENS)
