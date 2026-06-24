"""
Tests for event-metadata transforms:

    split_event_description, clean_event_location,
    extract_event_name_from_description, apply_manual_event_location_overrides
"""

from __future__ import annotations

import pandas as pd

from fliptop.data_cleaning import (
    apply_manual_event_location_overrides,
    clean_event_location,
    extract_event_name_from_description,
    split_event_description,
)


# ---------------------------------------------------------------------------
# split_event_description
# ---------------------------------------------------------------------------

def test_split_event_description_extracts_date_and_location():
    df = pd.DataFrame(
        {
            "event_description": [
                "FlipTop presents: Tectonics @ Katips Bar, Quezon City, Philippines. Dec. 4, 2010. Notes."
            ]
        }
    )
    out = split_event_description(df)
    assert out["event_date"].iloc[0] == "2010-12-04"
    # location is the text after the last colon, before the date
    assert "Katips Bar" in out["event_location"].iloc[0]


def test_split_event_description_date_range_takes_first_day():
    df = pd.DataFrame({"event_description": ["Ahon @ The Tent. Dec. 20-21, 2024."]})
    assert split_event_description(df)["event_date"].iloc[0] == "2024-12-20"


def test_split_event_description_no_date_keeps_full_string_as_location():
    df = pd.DataFrame({"event_description": ["No recognizable date here"]})
    out = split_event_description(df)
    assert pd.isna(out["event_date"].iloc[0])
    assert out["event_location"].iloc[0] == "No recognizable date here"


# ---------------------------------------------------------------------------
# clean_event_location
# ---------------------------------------------------------------------------

def test_clean_event_location_prefers_text_after_at_sign():
    df = pd.DataFrame({"event_location": ["FlipTop presents: Ahon 12 @ B-Side, Makati City"]})
    assert clean_event_location(df)["event_location_clean"].iloc[0] == "B-Side, Makati City"


def test_clean_event_location_normalizes_davao_variants():
    df = pd.DataFrame(
        {"event_location": ["Davao City", "Davao City, Metro Manila, Philippines"]}
    )
    out = clean_event_location(df)["event_location_clean"]
    assert out.tolist() == ["Davao City, Philippines", "Davao City, Philippines"]


# ---------------------------------------------------------------------------
# extract_event_name_from_description
# ---------------------------------------------------------------------------

def test_extract_event_name_from_description():
    df = pd.DataFrame(
        {
            "description": [
                "FlipTop presents: Ahon 16 @ The Tent, Las Pinas City. Dec 13, 2025.",
                "Some unrelated description with no event marker",
            ]
        }
    )
    out = extract_event_name_from_description(df)
    assert out["event_name_from_desc"].iloc[0] == "Ahon 16"
    assert pd.isna(out["event_name_from_desc"].iloc[1])


# ---------------------------------------------------------------------------
# apply_manual_event_location_overrides
# ---------------------------------------------------------------------------

def test_manual_overrides_d_mention_and_ahon12():
    df = pd.DataFrame(
        {
            "event_name": ["Some Event", "Ahon 12 (Day 1)", "Ahon 12 (Day 2)"],
            "event_location": ["D' mention bar somewhere", "wrong place", "wrong place"],
        }
    )
    out = apply_manual_event_location_overrides(df)
    assert out["event_location"].iloc[0] == "FlipTop Baraks, Mandaluyong City, Philippines"
    assert (
        out["event_location"].iloc[1]
        == "Jenerick Resort, Tanauan City, Batangas, Philippines"
    )
    assert (
        out["event_location"].iloc[2]
        == "Jenerick Resort, Tanauan City, Batangas, Philippines"
    )
