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


def test_load_manual_matchups_with_pending_and_resolved_rows(tmp_path):
    csv = _write(
        tmp_path / "m.csv",
        "id,emcee1,emcee2,helper_emcee,emcee1_status,emcee2_status,helper_status,note",
        [
            "pending,NA,NA,NA,NA,NA,NA,needs watching",
            "resolved,A,B,C,appeared,no_show,appeared,watched",
        ],
    )
    assert ov.load_manual_matchups(csv) == {
        "pending": {
            "emcee1": None,
            "emcee2": None,
            "helper_emcee": None,
            "emcee1_status": None,
            "emcee2_status": None,
            "helper_status": None,
            "note": "needs watching",
        },
        "resolved": {
            "emcee1": "A",
            "emcee2": "B",
            "helper_emcee": "C",
            "emcee1_status": "appeared",
            "emcee2_status": "no_show",
            "helper_status": "appeared",
            "note": "watched",
        },
    }


def test_load_upload_decisions_validates_and_skips_inactive_rows(tmp_path):
    csv = _write(
        tmp_path / "u.csv",
        "id,decision,reason,note,active",
        [
            "keepme,include,special_case_include,parseable battle with noisy title,true",
            "dropme,exclude,not_battle,not a battle,true",
            "later,review,manual_review_required,needs watching,false",
        ],
    )

    assert ov.load_upload_decisions(csv) == {
        "keepme": {
            "decision": "include",
            "reason": "special_case_include",
            "note": "parseable battle with noisy title",
        },
        "dropme": {
            "decision": "exclude",
            "reason": "not_battle",
            "note": "not a battle",
        },
    }


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


def test_maintained_table_rejects_reordered_columns(tmp_path):
    csv = _write(tmp_path / "d.csv", "event_date,id,note", ["2020-01-01,abc,reason"])
    with pytest.raises(ValueError, match="columns are out of order"):
        ov.load_event_date_overrides(csv)


def test_half_empty_row_raises(tmp_path):
    p = tmp_path / "d.csv"
    p.write_text("id,event_date,note\nabc,,r\n", encoding="utf-8")
    with pytest.raises(ValueError, match="required"):
        ov.load_event_date_overrides(p)


def test_manual_matchups_require_both_emcees_or_na(tmp_path):
    csv = _write(
        tmp_path / "m.csv",
        "id,emcee1,emcee2,helper_emcee,emcee1_status,emcee2_status,helper_status,note",
        ["abc,A,NA,NA,appeared,NA,NA,partial"],
    )
    with pytest.raises(ValueError, match="both be filled"):
        ov.load_manual_matchups(csv)


def test_manual_matchups_require_statuses_for_filled_emcees(tmp_path):
    csv = _write(
        tmp_path / "m.csv",
        "id,emcee1,emcee2,helper_emcee,emcee1_status,emcee2_status,helper_status,note",
        ["abc,A,B,NA,appeared,NA,NA,missing status"],
    )
    with pytest.raises(ValueError, match="statuses must match"):
        ov.load_manual_matchups(csv)


def test_manual_matchups_reject_bad_status(tmp_path):
    csv = _write(
        tmp_path / "m.csv",
        "id,emcee1,emcee2,helper_emcee,emcee1_status,emcee2_status,helper_status,note",
        ["abc,A,B,C,appeared,absent,appeared,bad"],
    )
    with pytest.raises(ValueError, match="participation status"):
        ov.load_manual_matchups(csv)


def test_upload_decisions_reject_bad_decision(tmp_path):
    csv = _write(
        tmp_path / "u.csv",
        "id,decision,reason,note,active",
        ["abc,maybe,not_battle,nope,true"],
    )
    with pytest.raises(ValueError, match="decision must be one of"):
        ov.load_upload_decisions(csv)


def test_upload_decisions_reject_bad_reason(tmp_path):
    csv = _write(
        tmp_path / "u.csv",
        "id,decision,reason,note,active",
        ["abc,exclude,because,nope,true"],
    )
    with pytest.raises(ValueError, match="reason must be one of"):
        ov.load_upload_decisions(csv)


def test_upload_decisions_reject_conflicting_duplicate(tmp_path):
    csv = _write(
        tmp_path / "u.csv",
        "id,decision,reason,note,active",
        [
            "abc,exclude,not_battle,nope,true",
            "abc,review,manual_review_required,watch,true",
        ],
    )
    with pytest.raises(ValueError, match="conflicting upload decisions"):
        ov.load_upload_decisions(csv)


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
    manual = ov.load_manual_matchups()
    assert manual["Um2XyeCDEew"]["helper_emcee"] == "Aelekz"
    assert manual["Um2XyeCDEew"]["emcee2_status"] == "no_show"
    assert manual["IO6AaGSupuY"]["emcee2"] is None

    assert ov.UPLOAD_DECISIONS_CSV.exists()
    assert ov.load_upload_decisions() == {}
