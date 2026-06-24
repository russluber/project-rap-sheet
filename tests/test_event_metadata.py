"""
Tests for event-metadata transforms:

    split_event_description, clean_event_location,
    extract_event_name_from_description, apply_manual_event_location_overrides
"""

from __future__ import annotations

import pandas as pd

from fliptop.battles import (
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


def test_clean_event_location_normalizes_country_separator():
    # The country should always be preceded by ", ". Sources write it three ways:
    # period ("City. Philippines"), no separator ("Metro Manila Philippines"),
    # and the already-correct comma (must be left unchanged / idempotent).
    df = pd.DataFrame(
        {
            "event_location": [
                "FlipTop presents: Event @ Makati Central Square, Makati City. Philippines",
                "FlipTop presents: Event @ San Juan Gym, San Juan City, Metro Manila Philippines",
                "FlipTop presents: Event @ B-Side, Makati City, Metro Manila, Philippines",
            ]
        }
    )
    out = clean_event_location(df)["event_location_clean"]
    assert out.tolist() == [
        "Makati Central Square, Makati City, Philippines",
        "San Juan Gym, San Juan City, Metro Manila, Philippines",
        "B-Side, Makati City, Metro Manila, Philippines",
    ]


def test_clean_event_location_collapses_doubled_word():
    df = pd.DataFrame(
        {"event_location": ["FlipTop presents: Event @ TIU Theater, MCS, Makati City City, Metro Manila, Philippines"]}
    )
    out = clean_event_location(df)["event_location_clean"].iloc[0]
    assert out == "TIU Theater, MCS, Makati City, Metro Manila, Philippines"


def test_clean_event_location_leaves_abbreviation_periods_untouched():
    # The fix must not corrupt legitimate abbreviation periods.
    df = pd.DataFrame(
        {
            "event_location": [
                "FlipTop presents: Event @ Club Z, Dr. A. Santos Avenue, Paranaque City, Metro Manila, Philippines"
            ]
        }
    )
    out = clean_event_location(df)["event_location_clean"].iloc[0]
    assert "Dr. A. Santos Avenue" in out
    assert out.endswith("Metro Manila, Philippines")


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


def test_manual_overrides_grafilipinas_and_poi4():
    df = pd.DataFrame(
        {
            "event_name": [
                "Grafilipinas (FlipTop x Meiday x Wall Lords)",
                "Process of Illumination 4",
            ],
            "event_location": [
                "Grafilipinas. FlipTop x Meiday x Wall Lords",
                "Process of Illumination 4 Tryouts, B-Side, Malugay Street, Makati City, Metro Manila, Philippines",
            ],
        }
    )
    out = apply_manual_event_location_overrides(df)
    assert out["event_location"].iloc[0] == "Marikina River Banks, Marikina City, Metro Manila, Philippines"
    assert out["event_location"].iloc[1] == "B-Side, Malugay Street, Makati City, Metro Manila, Philippines"
