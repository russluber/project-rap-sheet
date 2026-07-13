"""
Tests for fliptop.validate: data-quality gates for metadata and final output.

Covers a synthetic valid frame plus each failure mode, and asserts the real
committed pipeline output passes the gate clean.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from fliptop.battles import METADATA_COLUMNS
from fliptop.publish import FINAL_COLUMNS, FINAL_OUTPUT_FORBIDDEN_COLUMNS
from fliptop.validate import (
    summarize_battle_metadata,
    summarize_ft_battles,
    validate_battle_metadata,
    validate_ft_battles,
)


def _valid_metadata() -> pd.DataFrame:
    """A minimal battle metadata frame with every expected column."""
    return pd.DataFrame(
        {
            "id": ["aaaaaaaaaaa", ["bbbbbbbbbbb", "ccccccccccc"]],  # scalar + multi-part list
            "title": ["A vs B", "C vs D"],
            "description": ["", ""],
            "upload_date": pd.to_datetime(["2015-01-01", "2016-02-02"]),
            "duration_seconds": [100.0, 200.0],
            "duration_hms": ["00:01:40", "00:03:20"],
            "emcee1": ["A", "C"],
            "emcee2": ["B", "D"],
            "matchup": ["A vs B", "C vs D"],
            "event_name": ["Ev1", "Ev2"],
            "event_date": pd.to_datetime(["2015-01-01", "2016-02-02"]),
            "event_date_source": ["website", "versetracker"],
            "event_location": ["Manila", "Cebu"],
            "url": ["u1", ["u2a", "u2b"]],
        }
    )


def _valid_final() -> pd.DataFrame:
    """A minimal final ft_battles frame with every expected column."""
    return pd.DataFrame(
        {
            "id": ["aaaaaaaaaaa", "bbbbbbbbbbb"],
            "title": ["A vs B", "C vs D"],
            "upload_date": pd.to_datetime(["2015-01-01", "2016-02-02"]),
            "duration_seconds": [100.0, 200.0],
            "emcee1": ["A", "C"],
            "emcee2": ["B", "D"],
            "matchup": ["A vs B", "C vs D"],
            "event_name": ["Ev1", "Ev2"],
            "event_date": pd.to_datetime(["2015-01-01", "2016-02-02"]),
            "event_location": ["Manila", "Cebu"],
            "battle_type": ["judged", "promo"],
            "winner": ["A", "NA"],
            "votes_winner": ["5", "NA"],
            "votes_loser": ["0", "NA"],
            "url": ["u1", ["u2a", "u2b"]],
        }
    )


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

def test_valid_frame_has_no_problems():
    assert validate_battle_metadata(_valid_metadata(), today=date(2020, 1, 1)) == []
    assert validate_ft_battles(_valid_final(), today=date(2020, 1, 1)) == []


def test_final_schema_excludes_metadata_and_audit_only_columns():
    assert set(FINAL_OUTPUT_FORBIDDEN_COLUMNS).isdisjoint(FINAL_COLUMNS)
    assert {"description", "duration_hms", "event_date_source"} <= set(METADATA_COLUMNS)


def test_missing_metadata_event_date_and_source_are_allowed():
    # An undated battle (COVID-era NaT with no source) is legal, not a problem.
    df = _valid_metadata()
    df.loc[1, "event_date"] = pd.NaT
    df["event_date_source"] = ["website", pd.NA]
    assert validate_battle_metadata(df, today=date(2020, 1, 1)) == []


def test_metadata_requires_event_name_and_location_for_release():
    df = _valid_metadata()
    df.loc[0, "event_name"] = pd.NA
    df.loc[1, "event_location"] = ""

    problems = validate_battle_metadata(df, today=date(2020, 1, 1))

    assert any("blank event_name" in problem for problem in problems)
    assert any("blank event_location" in problem for problem in problems)


# ---------------------------------------------------------------------------
# failure modes
# ---------------------------------------------------------------------------

def test_empty_frame():
    assert validate_ft_battles(pd.DataFrame()) == ["ft_battles is empty"]


def test_missing_expected_columns():
    df = _valid_final().drop(columns=["matchup", "event_location"])
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("missing expected columns" in p and "matchup" in p for p in problems)


def test_final_output_rejects_metadata_and_audit_only_columns():
    df = _valid_final()
    df["event_date_source"] = "website"
    df["rule_id"] = pd.NA

    problems = validate_ft_battles(df, today=date(2020, 1, 1))

    assert any(
        "metadata/audit-only" in p
        and "event_date_source" in p
        and "rule_id" in p
        for p in problems
    )


def test_duplicate_scalar_id():
    df = _valid_final()
    df["id"] = ["dup", "dup"]
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("duplicate battle id" in p for p in problems)


def test_final_list_id_is_rejected():
    df = _valid_final()
    df["id"] = df["id"].astype(object)
    df.at[1, "id"] = ["bbbbbbbbbbb", "ccccccccccc"]
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("list-valued id" in p for p in problems)


def test_metadata_duplicate_id_across_scalar_and_multipart_list():
    # a scalar id equal to the first part of a multi-part battle collides
    df = _valid_metadata()
    df["id"] = ["xxxxxxxxxxx", ["xxxxxxxxxxx", "yyyyyyyyyyy"]]
    problems = validate_battle_metadata(df, today=date(2020, 1, 1))
    assert any("duplicate battle id" in p for p in problems)


def test_missing_id_flagged():
    df = _valid_final()
    df["id"] = [pd.NA, "bbbbbbbbbbb"]
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("no usable id" in p for p in problems)


def test_blank_emcee():
    df = _valid_final()
    df.loc[1, "emcee2"] = "   "
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("blank emcee2" in p for p in problems)


def test_unknown_event_date_source():
    df = _valid_metadata()
    df.loc[0, "event_date_source"] = "guessed"
    problems = validate_battle_metadata(df, today=date(2020, 1, 1))
    assert any("unexpected event_date_source" in p and "guessed" in p for p in problems)


def test_future_event_date():
    df = _valid_final()
    problems = validate_ft_battles(df, today=date(2015, 6, 1))  # row 1 is 2016-02-02
    assert any("in the future" in p for p in problems)


def test_too_early_event_date():
    df = _valid_final()
    df.loc[0, "event_date"] = pd.Timestamp("1999-01-01")
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("before 2010-01-01" in p for p in problems)


def test_missing_battle_type_flagged():
    df = _valid_final()
    df.loc[0, "battle_type"] = pd.NA
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("missing battle_type" in p for p in problems)


def test_winner_must_be_one_of_the_emcees():
    df = _valid_final()
    df.loc[0, "winner"] = "Z"
    problems = validate_ft_battles(df, today=date(2020, 1, 1))
    assert any("winner" in p and "not one of" in p for p in problems)


# ---------------------------------------------------------------------------
# summary + real data
# ---------------------------------------------------------------------------

def test_summarize_metadata_reports_count_and_sources():
    s = summarize_battle_metadata(_valid_metadata())
    assert s.startswith("2 battles")
    assert "website=1" in s and "versetracker=1" in s


def test_summarize_final_reports_count_and_battle_types():
    s = summarize_ft_battles(_valid_final())
    assert s.startswith("2 battles")
    assert "judged=1" in s and "promo=1" in s


def test_real_battle_metadata_passes_the_gate(battle_metadata):
    assert validate_battle_metadata(battle_metadata) == []


def test_real_ft_battles_passes_the_gate(ft_battles):
    # The committed pipeline output must satisfy every invariant (guards against
    # a raw-source change silently producing a malformed table).
    assert validate_ft_battles(ft_battles) == []


def test_real_ft_battles_contains_no_metadata_or_audit_only_columns(ft_battles):
    assert set(FINAL_OUTPUT_FORBIDDEN_COLUMNS).isdisjoint(ft_battles.columns)
