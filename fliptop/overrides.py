"""
fliptop.overrides

Loaders for the hand-maintained correction tables under ``data/overrides/``.

FlipTop's raw sources occasionally get an event's location or date wrong (COVID-era
obfuscation, descriptions with no ``@`` delimiter that leak the event name into the
location, a battle the YouTube description mis-dates, ...). Rather than bury these
fixes as literal dicts in the build code, they live as small reference-data CSVs -
edited like the project's other data (see :mod:`fliptop.rename_map`) - and the build
pipeline applies them.

Five tables, each keyed differently:

    event_locations.csv          event_name -> event_location   (exact event_name match)
    event_location_patterns.csv  substring  -> event_location   (event_location contains)
    location_aliases.csv         location   -> canonical         (exact value match)
    event_dates.csv              id         -> event_date (ISO)  (exact video-id match)
    manual_matchups.csv          id         -> matchup/roles     (exact video-id match)

Every table carries a free-text ``note`` column recording *why* the correction
exists; it is ignored on load. Loading validates the required columns, skips blank
rows, and raises if a key is mapped to two conflicting values.
"""

from __future__ import annotations

import csv
from pathlib import Path

from . import DATA_DIR

PathLike = str | Path

OVERRIDES_DIR = DATA_DIR / "overrides"

EVENT_LOCATIONS_CSV = OVERRIDES_DIR / "event_locations.csv"
EVENT_LOCATION_PATTERNS_CSV = OVERRIDES_DIR / "event_location_patterns.csv"
LOCATION_ALIASES_CSV = OVERRIDES_DIR / "location_aliases.csv"
EVENT_DATES_CSV = OVERRIDES_DIR / "event_dates.csv"
MANUAL_MATCHUPS_CSV = OVERRIDES_DIR / "manual_matchups.csv"


def _load_mapping(path: PathLike, key_col: str, value_col: str) -> dict[str, str]:
    """
    Load a two-column ``key -> value`` CSV as an insertion-ordered dict.

    A missing file yields ``{}`` so the pipeline still runs without the table.
    Extra columns (e.g. ``note``) are ignored. Blank rows are skipped; a row with
    only one of the two fields, or a key mapped to two different values, is an
    error.
    """
    path = Path(path)
    if not path.exists():
        return {}

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {key_col, value_col}
        if reader.fieldnames is None or not required <= set(reader.fieldnames):
            raise ValueError(
                f"{path}: expected columns {sorted(required)}, got {reader.fieldnames}"
            )

        mapping: dict[str, str] = {}
        for lineno, row in enumerate(reader, start=2):  # row 1 is the header
            key = (row.get(key_col) or "").strip()
            value = (row.get(value_col) or "").strip()

            if not key and not value:
                continue
            if not key or not value:
                raise ValueError(
                    f"{path}:{lineno}: both {key_col!r} and {value_col!r} are required"
                )
            if key in mapping and mapping[key] != value:
                raise ValueError(
                    f"{path}:{lineno}: {key!r} maps to both "
                    f"{mapping[key]!r} and {value!r}"
                )
            mapping[key] = value

    return mapping


def load_event_location_overrides(path: PathLike = EVENT_LOCATIONS_CSV) -> dict[str, str]:
    """Load ``event_name -> corrected event_location`` (applied by exact match)."""
    return _load_mapping(path, "event_name", "event_location")


def load_event_location_patterns(
    path: PathLike = EVENT_LOCATION_PATTERNS_CSV,
) -> list[tuple[str, str]]:
    """
    Load ``(substring, corrected event_location)`` pairs, in file order.

    An event_location that *contains* the substring is replaced wholesale. Kept
    as an ordered list because these are applied sequentially.
    """
    return list(_load_mapping(path, "contains", "event_location").items())


def load_location_aliases(path: PathLike = LOCATION_ALIASES_CSV) -> dict[str, str]:
    """Load ``raw location value -> canonical value`` (applied by exact match)."""
    return _load_mapping(path, "location", "canonical")


def load_event_date_overrides(path: PathLike = EVENT_DATES_CSV) -> dict[str, str]:
    """Load ``YouTube video id -> corrected event_date`` (ISO date string)."""
    return _load_mapping(path, "id", "event_date")


def _manual_name(value: str | None) -> str | None:
    """Normalize unresolved manual-matchup markers."""
    value = (value or "").strip()
    if not value or value.casefold() == "na":
        return None
    return value


MANUAL_PARTICIPATION_STATUSES = {"appeared", "no_show"}


def _manual_status(value: str | None) -> str | None:
    """Normalize manual participation status markers."""
    value = (value or "").strip()
    if not value or value.casefold() == "na":
        return None
    status = value.casefold()
    if status not in MANUAL_PARTICIPATION_STATUSES:
        allowed = ", ".join(sorted(MANUAL_PARTICIPATION_STATUSES | {"NA"}))
        raise ValueError(f"participation status must be one of: {allowed}")
    return status


def load_manual_matchups(
    path: PathLike = MANUAL_MATCHUPS_CSV,
) -> dict[str, dict[str, str | None]]:
    """
    Load manually resolved or pending matchup/participation overrides.

    Rows with ``emcee1`` and ``emcee2`` set to ``NA`` are intentional pending
    work: the pipeline surfaces them in audit output but does not publish them
    as final battles until both names are filled in. Resolved no-show rows also
    record participation status so event-history analyses can credit only the
    emcees who actually appeared.
    """
    path = Path(path)
    if not path.exists():
        return {}

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {
            "id",
            "emcee1",
            "emcee2",
            "helper_emcee",
            "emcee1_status",
            "emcee2_status",
            "helper_status",
        }
        if reader.fieldnames is None or not required <= set(reader.fieldnames):
            raise ValueError(
                f"{path}: expected columns {sorted(required)}, got {reader.fieldnames}"
            )

        overrides: dict[str, dict[str, str | None]] = {}
        for lineno, row in enumerate(reader, start=2):
            battle_id = (row.get("id") or "").strip()
            emcee1 = _manual_name(row.get("emcee1"))
            emcee2 = _manual_name(row.get("emcee2"))
            helper_emcee = _manual_name(row.get("helper_emcee"))
            try:
                emcee1_status = _manual_status(row.get("emcee1_status"))
                emcee2_status = _manual_status(row.get("emcee2_status"))
                helper_status = _manual_status(row.get("helper_status"))
            except ValueError as exc:
                raise ValueError(f"{path}:{lineno}: {exc}") from exc
            note = (row.get("note") or "").strip() or None

            if not any(
                [
                    battle_id,
                    emcee1,
                    emcee2,
                    helper_emcee,
                    emcee1_status,
                    emcee2_status,
                    helper_status,
                    note,
                ]
            ):
                continue
            if not battle_id:
                raise ValueError(f"{path}:{lineno}: 'id' is required")
            if bool(emcee1) != bool(emcee2):
                raise ValueError(
                    f"{path}:{lineno}: emcee1 and emcee2 must both be filled "
                    "or both be NA"
                )
            if bool(emcee1_status) != bool(emcee1) or bool(emcee2_status) != bool(emcee2):
                raise ValueError(
                    f"{path}:{lineno}: emcee1/emcee2 statuses must match "
                    "whether emcee1/emcee2 are filled"
                )
            if bool(helper_emcee) != bool(helper_status):
                raise ValueError(
                    f"{path}:{lineno}: helper_emcee and helper_status must "
                    "both be filled or both be NA"
                )
            if helper_status is not None and helper_status != "appeared":
                raise ValueError(f"{path}:{lineno}: helper_status must be 'appeared'")

            value = {
                "emcee1": emcee1,
                "emcee2": emcee2,
                "helper_emcee": helper_emcee,
                "emcee1_status": emcee1_status,
                "emcee2_status": emcee2_status,
                "helper_status": helper_status,
                "note": note,
            }
            if battle_id in overrides and overrides[battle_id] != value:
                raise ValueError(
                    f"{path}:{lineno}: {battle_id!r} has conflicting manual matchups"
                )
            overrides[battle_id] = value

    return overrides
