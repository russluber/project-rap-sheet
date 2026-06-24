"""
Tests for fliptop.rename_map.load_rename_map: loading and validating the
alias -> canonical emcee mapping from CSV.
"""

from __future__ import annotations

import pytest

from fliptop.rename_map import ALIASES_CSV, load_rename_map


def _write_csv(path, rows):
    """rows: list of (alias, canonical) tuples."""
    lines = ["alias,canonical"] + [f"{a},{c}" for a, c in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# basic load
# ---------------------------------------------------------------------------

def test_load_basic_mapping(tmp_path):
    csv = _write_csv(tmp_path / "a.csv", [("Looniee", "Loonie"), ("Akt", "AKT")])
    m = load_rename_map(csv)
    assert m == {"Looniee": "Loonie", "Akt": "AKT"}


def test_blank_rows_and_whitespace_are_handled(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("alias,canonical\n  Looniee , Loonie \n\n", encoding="utf-8")
    assert load_rename_map(p) == {"Looniee": "Loonie"}


def test_self_maps_are_dropped(tmp_path):
    csv = _write_csv(tmp_path / "a.csv", [("AKT", "AKT"), ("Akt", "AKT")])
    assert load_rename_map(csv) == {"Akt": "AKT"}


def test_identical_duplicate_rows_are_deduped(tmp_path):
    csv = _write_csv(tmp_path / "a.csv", [("Akt", "AKT"), ("Akt", "AKT")])
    assert load_rename_map(csv) == {"Akt": "AKT"}


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def test_conflicting_alias_raises(tmp_path):
    csv = _write_csv(tmp_path / "a.csv", [("Ghostly", "Goriong Talas"), ("Ghostly", "Spade")])
    with pytest.raises(ValueError, match="maps to both"):
        load_rename_map(csv)


def test_missing_column_raises(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("alias,name\nLooniee,Loonie\n", encoding="utf-8")
    with pytest.raises(ValueError, match="expected columns"):
        load_rename_map(p)


def test_half_empty_row_raises(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("alias,canonical\nLooniee,\n", encoding="utf-8")
    with pytest.raises(ValueError, match="required"):
        load_rename_map(p)


# ---------------------------------------------------------------------------
# chain resolution (the future-proofing payoff)
# ---------------------------------------------------------------------------

def test_chains_resolve_transitively(tmp_path):
    # Spade -> Ghostly -> Goriong Talas should collapse to Spade -> Goriong Talas
    csv = _write_csv(
        tmp_path / "a.csv",
        [("Ghostly", "Goriong Talas"), ("Spade", "Ghostly")],
    )
    m = load_rename_map(csv)
    assert m["Spade"] == "Goriong Talas"
    assert m["Ghostly"] == "Goriong Talas"


def test_cycle_raises(tmp_path):
    csv = _write_csv(tmp_path / "a.csv", [("A", "B"), ("B", "A")])
    with pytest.raises(ValueError, match="cycle"):
        load_rename_map(csv)


# ---------------------------------------------------------------------------
# real data
# ---------------------------------------------------------------------------

def test_real_aliases_csv_loads_and_is_chain_free(tmp_path):
    m = load_rename_map()  # default path: data/emcee_aliases.csv
    assert ALIASES_CSV.exists()
    assert m["Daddie Joe D"] == "Daddy Joe D"   # a known mapping
    # no canonical is itself an alias (load already resolves, so this asserts the
    # committed data is terminal / chain-free)
    assert set(m.values()).isdisjoint(set(m.keys()))
