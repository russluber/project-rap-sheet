"""
Tests for the title-level transforms that turn raw YouTube uploads into a
candidate set of 1v1 battle videos:

    clean_titles, filter_titles_with_vs, drop_non_battles, keep_1v1

These encode the project's judgment calls about "what counts as a battle", so
each rule gets a case that proves it fires and a case that proves it doesn't.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fliptop.battles import (
    clean_titles,
    drop_non_battles,
    filter_titles_with_vs,
    keep_1v1,
)

# ---------------------------------------------------------------------------
# clean_titles
# ---------------------------------------------------------------------------

def test_clean_titles_strips_whitespace_and_wrapping_quotes():
    df = pd.DataFrame({"title": ['  "Loonie vs Abra"  ', "Shehyee vs Pricetagg"]})
    out = clean_titles(df)
    assert out["title"].tolist() == ["Loonie vs Abra", "Shehyee vs Pricetagg"]


def test_clean_titles_leaves_inner_quotes_untouched():
    # Only a fully wrapping pair of quotes is removed.
    df = pd.DataFrame({"title": ['A vs "B" guy']})
    assert clean_titles(df)["title"].iloc[0] == 'A vs "B" guy'


def test_clean_titles_missing_column_is_a_noop():
    df = pd.DataFrame({"other": [1, 2]})
    out = clean_titles(df)
    pd.testing.assert_frame_equal(out, df)


# ---------------------------------------------------------------------------
# filter_titles_with_vs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "title, kept",
    [
        ("Loonie vs Abra", True),
        ("Loonie VS Abra", True),       # case-insensitive
        ("Loonie versus Abra", False),  # \bvs\b does not match inside "versus"
        ("FlipTop Year-End Awards", False),
    ],
)
def test_filter_titles_with_vs(title, kept):
    df = pd.DataFrame({"title": [title]})
    out = filter_titles_with_vs(df)
    assert (len(out) == 1) is kept


# ---------------------------------------------------------------------------
# drop_non_battles
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "title, kept",
    [
        ("Loonie vs Abra", True),
        ("BygShaqz vs Negatibo beatbox", False),
        ("Some Newcomer Tryouts", False),
        ("Pre-Battle Interviews: Loonie", False),
        ("FlipTop Festival [LIVE]", False),
        ("Anniversary Party Recap", False),
        ("FlipTop - sKarm vs Luck Loosh (PROMO)", True),
    ],
)
def test_drop_non_battles_keyword_matching(title, kept):
    df = pd.DataFrame({"title": [title]})
    out = drop_non_battles(df)
    assert (len(out) == 1) is kept


def test_drop_non_battles_matches_substrings_not_words():
    # Documents a known over-match: EXCLUDE_KEYWORDS are matched as substrings,
    # so "Reviewing" trips the "review" keyword even though it is a real battle.
    # If this is ever judged wrong, this test is the place to change it.
    df = pd.DataFrame({"title": ["Reviewing Loonie vs Abra"]})
    assert len(drop_non_battles(df)) == 0


# ---------------------------------------------------------------------------
# keep_1v1
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "title, kept",
    [
        ("Loonie vs Abra", True),          # plain 1v1
        ("Batas and Loonie vs Abra", True),  # a single "and" is allowed
        ("A vs B vs C", False),            # more than one "vs"
        ("Team A and B vs C and D", False),  # tag-team: and...vs...and
        ("Mike and Ana and Joe", False),   # more than one "and"
        ("5 on 5 Crew Battle", False),     # N on M format
        ("A / B vs C", False),             # slash
        ("A + B vs C", False),             # plus
    ],
)
def test_keep_1v1_heuristics(title, kept):
    df = pd.DataFrame({"title": [title]})
    out = keep_1v1(df)
    assert (len(out) == 1) is kept


def test_keep_1v1_non_string_titles_are_dropped():
    df = pd.DataFrame({"title": ["Loonie vs Abra", None, 12345]})
    out = keep_1v1(df)
    assert out["title"].tolist() == ["Loonie vs Abra"]
