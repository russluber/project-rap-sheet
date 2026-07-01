"""
Tests for fliptop.annotations: the id-keyed structured battle-results store and
its helpers (battle key, validation, pending, merge, round-trip).
"""

from __future__ import annotations

import pandas as pd
import pytest

from fliptop import annotations as ann

# ---------------------------------------------------------------------------
# battle_key / extract_video_id
# ---------------------------------------------------------------------------

def test_battle_key_scalar_and_list():
    assert ann.battle_key("abc123") == "abc123"
    assert ann.battle_key(["first", "second"]) == "first"  # multi-part -> first id


def test_battle_key_handles_missing():
    assert ann.battle_key(None) is None
    assert ann.battle_key([]) is None


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://www.youtube.com/watch?v=AGvrtaQPb3c", "AGvrtaQPb3c"),
        ("https://youtu.be/IPfQtKyCWiQ", "IPfQtKyCWiQ"),
        ("not a url", None),
    ],
)
def test_extract_video_id(url, expected):
    assert ann.extract_video_id(url) == expected


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def test_validate_winner_must_be_one_of_the_emcees():
    assert ann.validate_winner("Loonie", "Loonie", "Abra")
    assert ann.validate_winner("abra", "Loonie", "Abra")  # case-insensitive
    assert not ann.validate_winner("Shehyee", "Loonie", "Abra")


def test_validate_votes_and_overtime():
    assert ann.validate_votes("5") and ann.validate_votes("0") and ann.validate_votes(ann.NA)
    assert not ann.validate_votes("five") and not ann.validate_votes("-1")
    assert ann.validate_overtime("yes") and ann.validate_overtime("no") and ann.validate_overtime(ann.NA)
    assert not ann.validate_overtime("maybe")


def test_validate_result_row_judged_with_score_requires_integer_votes():
    ok = ann.make_result_row(
        id="x", winner="A", battle_type="judged",
        votes_winner=5, votes_loser=0, votes_nv=0, votes_ot=0, overtime="no",
    )
    assert ann.validate_result_row(ok) == []

    bad = dict(ok, votes_winner=ann.NA)  # half-filled tally (some NA, some int)
    assert ann.validate_result_row(bad)


def test_validate_result_row_judged_score_unknown_is_all_na_votes():
    ok = ann.make_result_row(id="x", winner="A", battle_type="judged")  # votes default NA
    assert ann.validate_result_row(ok) == []

    bad = dict(ok, overtime="no")  # score unknown but overtime asserted
    assert ann.validate_result_row(bad)


def test_validate_result_row_judged_requires_a_winner():
    bad = ann.make_result_row(id="x", winner=ann.NA, battle_type="judged")
    assert ann.validate_result_row(bad)


def test_validate_result_row_promo_has_no_winner_and_no_votes():
    ok = ann.make_result_row(id="x", winner=ann.NA, battle_type="promo")
    assert "promo" in ann.BATTLE_TYPES
    assert ann.validate_result_row(ok) == []

    assert ann.validate_result_row(dict(ok, votes_winner="5"))  # promo never has votes
    assert ann.validate_result_row(dict(ok, winner="A"))        # promo never has a winner


# ---------------------------------------------------------------------------
# store round-trip + upsert (no blank cells)
# ---------------------------------------------------------------------------

def test_save_keeps_explicit_markers_and_sorts(tmp_path):
    path = tmp_path / "battle_results.csv"
    rows = [
        ann.make_result_row(id="b", winner=ann.NA, battle_type="promo"),
        ann.make_result_row(
            id="a", winner="Y", battle_type="judged",
            votes_winner=4, votes_loser=1, votes_nv=0, votes_ot=0, overtime="no",
        ),
    ]
    ann.save_results(pd.DataFrame(rows, columns=ann.RESULTS_COLUMNS), path)
    loaded = ann.load_results(path)

    assert list(loaded.columns) == ann.RESULTS_COLUMNS
    assert loaded["id"].tolist() == ["a", "b"]            # sorted on write
    # no blank cells anywhere
    assert (loaded.map(lambda v: str(v).strip() != "")).all().all()
    # explicit markers survive the round-trip (not coerced to NaN)
    nd = loaded[loaded["id"] == "b"].iloc[0]
    assert nd["votes_winner"] == "NA" and nd["notes"] == "none"


def test_upsert_replaces_existing_id():
    row1 = ann.make_result_row(id="a", winner="X", battle_type="judged",
                               votes_winner=5, votes_loser=0, votes_nv=0, votes_ot=0, overtime="no")
    results = pd.DataFrame([row1], columns=ann.RESULTS_COLUMNS)
    row2 = ann.make_result_row(id="a", winner="Y", battle_type="judged",
                               votes_winner=4, votes_loser=1, votes_nv=0, votes_ot=0, overtime="no")
    results = ann.upsert_result(results, row2)
    assert len(results) == 1 and results.iloc[0]["winner"] == "Y"


def test_make_result_row_fills_empty_notes_with_none():
    row = ann.make_result_row(id="a", winner="X", battle_type="judged", notes="")
    assert row["notes"] == "none"


# ---------------------------------------------------------------------------
# pending / merge
# ---------------------------------------------------------------------------

def _battles():
    return pd.DataFrame(
        {
            "id": ["a", "b", ["c1", "c2"]],
            "emcee1": ["Loonie", "Abra", "Shehyee"],
            "emcee2": ["Abra", "Shehyee", "Loonie"],
            "upload_date": pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"]),
        }
    )


def test_pending_excludes_battles_in_store():
    results = pd.DataFrame(
        [ann.make_result_row(id="a", winner="Loonie", battle_type="judged",
                             votes_winner=5, votes_loser=0, votes_nv=0, votes_ot=0, overtime="no")],
        columns=ann.RESULTS_COLUMNS,
    )
    keys = set(ann.pending_battles(_battles(), results)["battle_key"])
    assert "a" not in keys              # recorded -> excluded
    assert {"b", "c1"} <= keys          # multi-part keyed by first id


def test_merge_results_adds_columns_without_mutating_input():
    battles = _battles()
    results = pd.DataFrame(
        [ann.make_result_row(id="c1", winner="Shehyee", battle_type="judged",
                             votes_winner=7, votes_loser=0, votes_nv=0, votes_ot=0, overtime="no")],
        columns=ann.RESULTS_COLUMNS,
    )
    merged = ann.merge_results(battles, results)
    assert "winner" not in battles.columns          # input untouched
    row = merged[merged["battle_key"] == "c1"].iloc[0]
    assert row["winner"] == "Shehyee" and row["votes_winner"] == "7"
    assert merged[merged["battle_key"] == "a"]["winner"].isna().all()  # unannotated -> NaN
