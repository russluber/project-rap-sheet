"""
Tests for fliptop.overrides: loading and validating the hand-maintained
correction tables under data/overrides/.
"""

from __future__ import annotations

import pytest

from fliptop import overrides as ov


def _write(path, header, rows):
    lines = [header] + list(rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# basic loading
# ---------------------------------------------------------------------------

def test_load_event_location_overrides(tmp_path):
    csv = _write(
        tmp_path / "e.csv",
        "event_name,event_location,note",
        ['Ahon 3,"San Juan Gym, San Juan City",leaked name'],
    )
    assert ov.load_event_location_overrides(csv) == {"Ahon 3": "San Juan Gym, San Juan City"}


def test_load_event_date_overrides(tmp_path):
    csv = _write(tmp_path / "d.csv", "id,event_date,note", ["IdPP-JPtk4M,2023-09-29,site wins"])
    assert ov.load_event_date_overrides(csv) == {"IdPP-JPtk4M": "2023-09-29"}


def test_load_location_aliases(tmp_path):
    csv = _write(
        tmp_path / "a.csv",
        "location,canonical,note",
        ['Davao City,"Davao City, Philippines",normalize'],
    )
    assert ov.load_location_aliases(csv) == {"Davao City": "Davao City, Philippines"}


def test_load_event_location_patterns_is_ordered_list(tmp_path):
    csv = _write(
        tmp_path / "p.csv",
        "contains,event_location,note",
        ["alpha,LocA,x", "beta,LocB,y"],
    )
    # order preserved, returned as a list of (substring, location) pairs
    assert ov.load_event_location_patterns(csv) == [("alpha", "LocA"), ("beta", "LocB")]


# ---------------------------------------------------------------------------
# behavior of the shared loader
# ---------------------------------------------------------------------------

def test_note_column_is_ignored(tmp_path):
    csv = _write(tmp_path / "d.csv", "id,event_date,note", ["abc,2020-01-01,some long reason"])
    assert ov.load_event_date_overrides(csv) == {"abc": "2020-01-01"}


def test_blank_rows_and_whitespace_handled(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("id,event_date,note\n  abc , 2020-01-01 ,r\n\n", encoding="utf-8")
    assert ov.load_event_date_overrides(p) == {"abc": "2020-01-01"}


def test_missing_file_returns_empty(tmp_path):
    assert ov.load_event_date_overrides(tmp_path / "nope.csv") == {}


def test_conflicting_key_raises(tmp_path):
    csv = _write(
        tmp_path / "d.csv",
        "id,event_date,note",
        ["abc,2020-01-01,a", "abc,2021-02-02,b"],
    )
    with pytest.raises(ValueError, match="maps to both"):
        ov.load_event_date_overrides(csv)


def test_duplicate_identical_row_is_deduped(tmp_path):
    csv = _write(
        tmp_path / "d.csv",
        "id,event_date,note",
        ["abc,2020-01-01,a", "abc,2020-01-01,a"],
    )
    assert ov.load_event_date_overrides(csv) == {"abc": "2020-01-01"}


def test_missing_column_raises(tmp_path):
    csv = _write(tmp_path / "d.csv", "id,wrong,note", ["abc,2020-01-01,a"])
    with pytest.raises(ValueError, match="expected columns"):
        ov.load_event_date_overrides(csv)


def test_half_empty_row_raises(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("id,event_date,note\nabc,,r\n", encoding="utf-8")
    with pytest.raises(ValueError, match="required"):
        ov.load_event_date_overrides(p)


# ---------------------------------------------------------------------------
# the committed data
# ---------------------------------------------------------------------------

def test_shipped_csvs_load_with_expected_keys():
    assert ov.EVENT_LOCATIONS_CSV.exists()
    locs = ov.load_event_location_overrides()
    assert locs["Ahon 12 (Day 1)"] == "Jenerick Resort, Tanauan City, Batangas, Philippines"
    # the tricky embedded-quote key round-trips through the CSV
    assert 'Masamang Damo (Batas - "Ako" Video Launch)' in locs

    assert ov.load_event_date_overrides()["IdPP-JPtk4M"] == "2023-09-29"
    assert ov.load_location_aliases()["Davao City"] == "Davao City, Philippines"
    assert ov.load_event_location_patterns()[0] == (
        "D' mention",
        "FlipTop Baraks, Mandaluyong City, Philippines",
    )
