"""
fliptop.rename_map

Loads and validates the canonical emcee-name mapping.

FlipTop emcees often appear in YouTube titles under different aliases or
formatting variations. The cleaning pipeline standardizes these so the dataset
uses consistent canonical names.

The mapping is hand-maintained reference data and lives in a CSV so it can be
edited like the project's other data (one row per alias):

    data/emcee_aliases.csv
    columns: alias,canonical

`load_rename_map()` reads that file and returns an ``alias -> canonical`` dict,
validating the data as it goes (see the function for the rules).
"""

from __future__ import annotations

import csv
from pathlib import Path

from . import DATA_DIR

PathLike = str | Path

ALIASES_CSV = DATA_DIR / "emcee_aliases.csv"

EXPECTED_COLUMNS = {"alias", "canonical"}


def load_rename_map(path: PathLike = ALIASES_CSV) -> dict[str, str]:
    """
    Load the alias -> canonical emcee mapping from CSV.

    The CSV must have ``alias`` and ``canonical`` columns. This:

      - strips whitespace and skips blank rows;
      - skips no-op self-maps (alias == canonical);
      - de-duplicates identical rows silently;
      - raises ValueError if an alias maps to two *different* canonicals;
      - resolves alias chains transitively (A->B, B->C  =>  A->C), with cycle
        detection, so the result is order-independent and chain-safe.

    Returns
    -------
    dict[str, str]
        Mapping from alias to its terminal canonical name.
    """
    path = Path(path)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or not set(reader.fieldnames) >= EXPECTED_COLUMNS:
            raise ValueError(
                f"{path}: expected columns {sorted(EXPECTED_COLUMNS)}, "
                f"got {reader.fieldnames}"
            )

        direct: dict[str, str] = {}
        for lineno, row in enumerate(reader, start=2):  # row 1 is the header
            alias = (row.get("alias") or "").strip()
            canonical = (row.get("canonical") or "").strip()

            if not alias and not canonical:
                continue
            if not alias or not canonical:
                raise ValueError(
                    f"{path}:{lineno}: both 'alias' and 'canonical' are required"
                )
            if alias == canonical:
                continue  # no-op self-map
            if alias in direct and direct[alias] != canonical:
                raise ValueError(
                    f"{path}:{lineno}: alias {alias!r} maps to both "
                    f"{direct[alias]!r} and {canonical!r}"
                )
            direct[alias] = canonical

    return _resolve_chains(direct, path)


def _resolve_chains(direct: dict[str, str], path: Path) -> dict[str, str]:
    """Follow alias chains to their terminal canonical, detecting cycles."""
    resolved: dict[str, str] = {}
    for alias in direct:
        seen = [alias]
        current = direct[alias]
        while current in direct:  # the canonical is itself an alias -> keep going
            if current in seen:
                cycle = " -> ".join(seen + [current])
                raise ValueError(f"{path}: alias cycle detected: {cycle}")
            seen.append(current)
            current = direct[current]
        resolved[alias] = current
    return resolved
