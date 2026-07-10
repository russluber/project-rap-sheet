"""
Tests for matchup / emcee-name extraction:

    extract_matchup_from_title, add_matchup_and_split,
    apply_emcee_rename, add_matchup_clean

These cover the division of labour between the two extraction functions:
``extract_matchup_from_title`` strips the FlipTop prefix and trailing clutter,
while ``add_matchup_and_split`` is what removes a trailing "- Finals"-style
annotation and splits the matchup into emcee columns.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fliptop.uploads import (
    add_matchup_and_split,
    add_matchup_clean,
    apply_emcee_rename,
    extract_matchup_from_title,
)

# ---------------------------------------------------------------------------
# extract_matchup_from_title
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "title, expected",
    [
        ("FlipTop - Loonie vs Abra", "Loonie vs Abra"),
        ("FlipTop Pamilya Edition - Dello vs Batas", "Dello vs Batas"),
        ("FlipTop - Loonie vs Abra @fliptopbattles", "Loonie vs Abra"),
        ("FlipTop - Loonie vs Abra | Tagalog", "Loonie vs Abra"),
        ("FlipTop - A vs B (Conference)", "A vs B"),
        ("FlipTop - Bisente vs Jamy Sykes 2", "Bisente vs Jamy Sykes"),  # trailing number trimmed
        ("FlipTop Year-End Awards", None),     # no "vs"
        ("FlipTop - A vs B vs C", "A vs B vs C"),  # split maxsplit=1: extra "vs" stays on the right
    ],
)
def test_extract_matchup_from_title(title, expected):
    assert extract_matchup_from_title(title) == expected


def test_extract_matchup_does_not_strip_finals_suffix():
    # "- Finals" survives here; removing it is add_matchup_and_split's job.
    assert (
        extract_matchup_from_title("FlipTop - Shehyee vs Loonie - Finals")
        == "Shehyee vs Loonie - Finals"
    )


def test_extract_matchup_non_string_returns_none():
    assert extract_matchup_from_title(None) is None
    assert extract_matchup_from_title(12345) is None


# ---------------------------------------------------------------------------
# add_matchup_and_split
# ---------------------------------------------------------------------------

def test_add_matchup_and_split_basic():
    df = pd.DataFrame({"title": ["FlipTop - Loonie vs Abra"]})
    out = add_matchup_and_split(df)
    row = out.iloc[0]
    assert row["matchup"] == "Loonie vs Abra"
    assert row["emcee1"] == "Loonie"
    assert row["emcee2"] == "Abra"


def test_add_matchup_and_split_removes_trailing_annotation():
    df = pd.DataFrame({"title": ["FlipTop - Shehyee vs Loonie - Finals"]})
    out = add_matchup_and_split(df)
    assert out.iloc[0]["matchup"] == "Shehyee vs Loonie"
    assert out.iloc[0]["emcee2"] == "Loonie"


# ---------------------------------------------------------------------------
# apply_emcee_rename
# ---------------------------------------------------------------------------

def test_apply_emcee_rename_canonicalizes_known_aliases():
    df = pd.DataFrame({"emcee1": ["Looniee", "Smugglaz"], "emcee2": ["Abra", "Daddie Joe D"]})
    out = apply_emcee_rename(df, {"Looniee": "Loonie", "Daddie Joe D": "Daddy Joe D"})
    assert out["emcee1"].tolist() == ["Loonie", "Smugglaz"]
    assert out["emcee2"].tolist() == ["Abra", "Daddy Joe D"]


def test_apply_emcee_rename_is_case_sensitive():
    # Documents a gotcha: the rename map is matched exactly, so a differently
    # cased alias is left untouched.
    df = pd.DataFrame({"emcee1": ["looniee"], "emcee2": ["Abra"]})
    out = apply_emcee_rename(df, {"Looniee": "Loonie"})
    assert out["emcee1"].iloc[0] == "looniee"


def test_apply_emcee_rename_none_map_is_noop():
    df = pd.DataFrame({"emcee1": ["Looniee"], "emcee2": ["Abra"]})
    pd.testing.assert_frame_equal(apply_emcee_rename(df, None), df)


# ---------------------------------------------------------------------------
# add_matchup_clean
# ---------------------------------------------------------------------------

def test_add_matchup_clean_uses_canonical_columns():
    df = pd.DataFrame({"emcee1": ["Loonie"], "emcee2": ["Abra"]})
    out = add_matchup_clean(df)
    assert out["matchup_clean"].iloc[0] == "Loonie vs Abra"
