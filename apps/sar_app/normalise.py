"""Input normalisation for SAR search identifier types.

``SearchNormaliser`` is a pure namespace class (no instance state) whose
class methods clean raw sidebar input into a consistent form before SQL
clauses are built.  A single dispatch entry-point ``for_tag`` routes to the
appropriate method given a Unity Catalog ``class.*`` tag name.
"""

from __future__ import annotations

import re
import unicodedata


class SearchNormaliser:
    """Normalises raw user input into clean, comparable search values.

    All methods are class methods or static methods — the class carries no
    instance state and is used as a namespace rather than instantiated.
    ``for_tag`` dispatches to the appropriate method given a ``class.*`` tag.
    """

    _HONORIFICS: frozenset[str] = frozenset({
        "mr", "mrs", "ms", "miss", "dr", "prof",
        "sir", "mx", "rev", "lord", "lady",
        "master", "mstr", "dame", "fr", "hon", "cllr",
        "capt", "col", "maj", "sgt", "lt", "gen", "judge",
        "baron", "baroness",
    })

    #: Matches a stripped, uppercased full UK postcode (e.g. "SW1A1AA").
    #: Used to decide which SQL branch to apply after ``postcode()`` cleans
    #: the value.
    _POSTCODE_RE: re.Pattern = re.compile(
        r"^[A-Z]{1,2}[0-9][0-9A-Z]?[0-9][A-Z]{2}$"
    )

    #: Finds a UK postcode anywhere in a string, including inside an address.
    #: Groups 1 and 2 are the outward and inward codes respectively.
    _POSTCODE_SEARCH_RE: re.Pattern = re.compile(
        r"\b([A-Z]{1,2}[0-9][0-9A-Z]?)\s?([0-9][A-Z]{2})\b",
        re.IGNORECASE,
    )

    @classmethod
    def for_tag(cls, tag: str, val: str) -> str:
        """Dispatch to the normaliser appropriate for *tag*.

        Tags without a dedicated normaliser are returned unchanged (e.g.
        ``class.email_address``, ``class.date_of_birth``).
        """
        dispatch = {
            "class.name":         cls.name,
            "class.phone_number": cls.phone,
            "class.location":     cls.postcode,
        }
        return dispatch.get(tag, lambda v: v)(val)

    @classmethod
    def name(cls, val: str) -> str:
        """Normalise a free-text name string.

        Steps applied in order:

        1. NFKD decomposition + ASCII re-encode strips diacritics
           (José → Jose, Müller → Muller).
        2. Leading honorific removed (handles trailing dots: "Mr.").
        3. Non-letter, non-space, non-hyphen characters dropped
           (preserves compound surnames: Smith-Jones).
        4. Whitespace collapsed; result returned lowercase.
        """
        nfkd = unicodedata.normalize("NFKD", val)
        ascii_val = nfkd.encode("ascii", errors="ignore").decode("ascii")

        parts = ascii_val.strip().split()
        if parts and parts[0].lower().rstrip(".") in cls._HONORIFICS:
            parts = parts[1:]

        cleaned = re.sub(r"[^a-zA-Z\s\-]", "", " ".join(parts))
        return re.sub(r"\s+", " ", cleaned).strip().lower()

    @staticmethod
    def phone(val: str) -> str:
        """Return the last 9 digits of *val* (strips country-code prefixes).

        Country-code prefixes (+44, 0044, 001 …) are 1–3 digits, so
        discarding everything but the trailing 9 digits removes them without
        needing to know which country's dialling code is in use.
        """
        digits = re.sub(r"\D", "", val)
        return digits[-9:] if len(digits) >= 9 else digits

    @classmethod
    def postcode(cls, val: str) -> str:
        """Extract and normalise a UK postcode from *val*.

        Searches anywhere in the string so embedded addresses are handled
        (e.g. "27 My Street, N1 9PF"). Returns outward + inward codes joined
        without a space, uppercased ("N19PF"). Returns *val* unchanged if no
        recognisable postcode is found, preserving free-text locations.
        """
        m = cls._POSTCODE_SEARCH_RE.search(val)
        if m:
            return m.group(1).upper() + m.group(2).upper()
        return val

    @classmethod
    def is_clean_postcode(cls, val: str) -> bool:
        """Return ``True`` when *val* is already a stripped, uppercased postcode."""
        return bool(cls._POSTCODE_RE.match(val))
