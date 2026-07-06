"""
Tests for event-metadata transforms:

    split_event_description, clean_event_location,
    extract_event_name_from_description, apply_manual_event_location_overrides
"""

from __future__ import annotations

import pandas as pd

from fliptop.battles import (
    _parse_event_date_range,
    _split_event_day,
    apply_manual_event_date_overrides,
    apply_manual_event_location_overrides,
    clean_event_location,
    drop_excluded_events,
    extract_event_name_from_description,
    impute_event_dates_from_versetracker,
    load_versetracker_event_dates,
    normalize_event_day,
    split_event_description,
)

# ---------------------------------------------------------------------------
# event exclusions
# ---------------------------------------------------------------------------

def test_drop_excluded_events_matches_categories_case_insensitively():
    df = pd.DataFrame(
        {
            "id": ["poi", "tryout", "keep", "missing"],
            "event_name": [
                "PROCESS OF ILLUMINATION 6 (Visayas)",
                "Mindfields Mindanao TrYoUtS",
                "Ahon 15",
                pd.NA,
            ],
        }
    )

    out = drop_excluded_events(df)

    assert out["id"].tolist() == ["keep", "missing"]


def test_drop_excluded_events_without_event_column_is_noop():
    df = pd.DataFrame({"id": ["a", "b"]})
    pd.testing.assert_frame_equal(drop_excluded_events(df), df)


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
# _parse_event_date_range
# ---------------------------------------------------------------------------

def test_parse_event_date_range_single_day():
    assert _parse_event_date_range("Tectonics. Dec. 4, 2010.") == ("2010-12-04", "2010-12-04")


def test_parse_event_date_range_two_day_range():
    assert _parse_event_date_range("Ahon 16. December 13-14, 2025.") == ("2025-12-13", "2025-12-14")


def test_parse_event_date_range_no_date_or_non_string():
    assert _parse_event_date_range("no date here") == (None, None)
    assert _parse_event_date_range(None) == (None, None)


# ---------------------------------------------------------------------------
# _split_event_day
# ---------------------------------------------------------------------------

def test_split_event_day_parenthesized_and_comma_forms():
    assert _split_event_day("Ahon 16 (Day 2)") == ("Ahon 16", 2)
    assert _split_event_day("Gubat 12, Day 1") == ("Gubat 12", 1)
    assert _split_event_day("The FlipTop Festival (Day 1)") == ("The FlipTop Festival", 1)


def test_split_event_day_no_day_label():
    assert _split_event_day("Tectonics") == ("Tectonics", None)
    # a non-day parenthetical is preserved; "Meiday" must not trip the matcher
    assert _split_event_day("Grafilipinas (FlipTop x Meiday x Wall Lords)") == (
        "Grafilipinas (FlipTop x Meiday x Wall Lords)",
        None,
    )


# ---------------------------------------------------------------------------
# normalize_event_day
# ---------------------------------------------------------------------------

def test_normalize_event_day_fixes_day2_pinned_to_range_start():
    df = pd.DataFrame(
        {
            "event_name": ["Ahon 16 (Day 1)", "Ahon 16 (Day 2)"],
            "description": ["FlipTop presents: Ahon 16 @ The Tent. December 13-14, 2025."] * 2,
            "event_date": pd.to_datetime(["2025-12-13", "2025-12-13"]),  # both pinned to day 1
        }
    )
    out = normalize_event_day(df)
    # day suffix stripped; the day itself is not kept as a column
    assert out["event_name"].tolist() == ["Ahon 16", "Ahon 16"]
    assert "event_day" not in out.columns
    assert out["event_date"].tolist() == [
        pd.Timestamp("2025-12-13"),
        pd.Timestamp("2025-12-14"),
    ]


def test_normalize_event_day_leaves_already_disambiguated_date():
    # Day 2 already moved off the range start -> trusted, not recomputed.
    df = pd.DataFrame(
        {
            "event_name": ["Ahon 10 (Day 2)"],
            "description": ["FlipTop presents: Ahon 10 @ TIU. December 13-14, 2019."],
            "event_date": pd.to_datetime(["2019-12-14"]),
        }
    )
    out = normalize_event_day(df)
    assert out["event_name"].iloc[0] == "Ahon 10"
    assert out["event_date"].iloc[0] == pd.Timestamp("2019-12-14")


def test_normalize_event_day_single_day_event_left_alone():
    df = pd.DataFrame(
        {
            "event_name": ["Tectonics"],
            "description": ["FlipTop presents: Tectonics @ Katips. Dec. 4, 2010."],
            "event_date": pd.to_datetime(["2010-12-04"]),
        }
    )
    out = normalize_event_day(df)
    assert out["event_name"].iloc[0] == "Tectonics"
    assert out["event_date"].iloc[0] == pd.Timestamp("2010-12-04")
    assert "event_day" not in out.columns


def test_normalize_event_day_never_unmasks_missing_date():
    # A NaT date (e.g. COVID-masked) stays NaT, even though the name has a day
    # and a range parses -> only the name is normalized.
    df = pd.DataFrame(
        {
            "event_name": ["Ahon 12 (Day 2)"],
            "description": ["FlipTop presents: Ahon 12 (Day 2) @ X. December 13-14, 2021."],
            "event_date": [pd.NaT],
        }
    )
    out = normalize_event_day(df)
    assert out["event_name"].iloc[0] == "Ahon 12"
    assert pd.isna(out["event_date"].iloc[0])


# ---------------------------------------------------------------------------
# impute_event_dates_from_versetracker
# ---------------------------------------------------------------------------

VT = {
    "Ahon 12": pd.Timestamp("2021-12-08"),
    "Zoning 10": pd.Timestamp("2020-09-30"),
}


def test_impute_single_day_uses_mapped_date():
    df = pd.DataFrame(
        {"event_name": ["Zoning 10"], "event_date": [pd.NaT]}
    )
    out = impute_event_dates_from_versetracker(df, VT)
    assert out["event_date"].iloc[0] == pd.Timestamp("2020-09-30")


def test_impute_multi_day_offsets_from_day_suffix():
    df = pd.DataFrame(
        {
            "event_name": ["Ahon 12 (Day 1)", "Ahon 12 (Day 2)"],
            "event_date": [pd.NaT, pd.NaT],
        }
    )
    out = impute_event_dates_from_versetracker(df, VT)
    assert out["event_date"].tolist() == [
        pd.Timestamp("2021-12-08"),  # day 1 = base
        pd.Timestamp("2021-12-09"),  # day 2 = base + 1
    ]
    # filled rows are tagged with their provenance
    assert out["event_date_source"].tolist() == ["versetracker", "versetracker"]


def test_impute_tags_only_filled_rows():
    df = pd.DataFrame(
        {
            "event_name": ["Zoning 10", "Not In Map"],
            "event_date": [pd.NaT, pd.NaT],
        }
    )
    out = impute_event_dates_from_versetracker(df, VT)
    assert out["event_date_source"].iloc[0] == "versetracker"   # filled -> tagged
    assert pd.isna(out["event_date_source"].iloc[1])            # unknown -> untagged


def test_impute_leaves_existing_dates_untouched():
    df = pd.DataFrame(
        {"event_name": ["Zoning 10"], "event_date": pd.to_datetime(["2020-01-01"])}
    )
    out = impute_event_dates_from_versetracker(df, VT)
    assert out["event_date"].iloc[0] == pd.Timestamp("2020-01-01")  # not overwritten


def test_impute_unknown_event_stays_nat():
    df = pd.DataFrame({"event_name": ["Not In Map"], "event_date": [pd.NaT]})
    out = impute_event_dates_from_versetracker(df, VT)
    assert pd.isna(out["event_date"].iloc[0])


def test_impute_empty_map_is_noop():
    df = pd.DataFrame({"event_name": ["Ahon 12 (Day 1)"], "event_date": [pd.NaT]})
    assert pd.isna(impute_event_dates_from_versetracker(df, {})["event_date"].iloc[0])
    assert pd.isna(impute_event_dates_from_versetracker(df, None)["event_date"].iloc[0])


# ---------------------------------------------------------------------------
# load_versetracker_event_dates
# ---------------------------------------------------------------------------

def test_load_versetracker_event_dates_reads_map(tmp_path):
    p = tmp_path / "vt.csv"
    pd.DataFrame(
        {
            "event_name": ["Ahon 12", "Zoning 10"],
            "event_date": ["2021-12-08", "2020-09-30"],
            "source_url": ["u1", "u2"],
        }
    ).to_csv(p, index=False)
    out = load_versetracker_event_dates(p)
    assert out == {
        "Ahon 12": pd.Timestamp("2021-12-08"),
        "Zoning 10": pd.Timestamp("2020-09-30"),
    }


def test_load_versetracker_event_dates_missing_file_is_empty(tmp_path):
    assert load_versetracker_event_dates(tmp_path / "nope.csv") == {}


def test_load_versetracker_event_dates_drops_unparseable_dates(tmp_path):
    p = tmp_path / "vt.csv"
    pd.DataFrame(
        {"event_name": ["Good", "Bad"], "event_date": ["2021-12-08", "not a date"]}
    ).to_csv(p, index=False)
    out = load_versetracker_event_dates(p)
    assert out == {"Good": pd.Timestamp("2021-12-08")}


# ---------------------------------------------------------------------------
# apply_manual_event_date_overrides
# ---------------------------------------------------------------------------

def test_manual_event_date_override_pins_known_battle():
    # Nikki vs K-Ram: YouTube description mis-dates it; website wins (Sept 29).
    df = pd.DataFrame(
        {
            "id": ["IdPP-JPtk4M", "other", ["x", "y"]],
            "event_date": pd.to_datetime(["2023-09-30", "2020-01-01", "2020-01-01"]),
        }
    )
    out = apply_manual_event_date_overrides(df)
    assert out["event_date"].iloc[0] == pd.Timestamp("2023-09-29")  # corrected
    assert out["event_date"].iloc[1] == pd.Timestamp("2020-01-01")  # untouched
    assert out["event_date"].iloc[2] == pd.Timestamp("2020-01-01")  # list id, untouched
    # the pinned row is tagged "manual"; others are left untagged
    assert out["event_date_source"].iloc[0] == "manual"
    assert pd.isna(out["event_date_source"].iloc[1])


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


def test_manual_overrides_strip_leaked_event_names():
    # Events whose description had no '@', leaking the event name into the front
    # of the extracted location.
    df = pd.DataFrame(
        {
            "event_name": [
                "Bara Ko, Barako",
                "Ahon 3",
                'Masamang Damo (Batas - "Ako" Video Launch)',
            ],
            "event_location": [
                "Bara ko, Barako, Naic Covered Court, Naic, Cavite, Philippines",
                "Ahon 3, San Juan Gym, San Juan City, Metro Manila, Philippines",
                'Masamang Damo, Batas "Ako" Video Launch, Tavern Asia, BF Homes, Paranaque City, Philippines',
            ],
        }
    )
    out = apply_manual_event_location_overrides(df)["event_location"].tolist()
    assert out == [
        "Naic Covered Court, Naic, Cavite, Philippines",
        "San Juan Gym, San Juan City, Metro Manila, Philippines",
        "Tavern Asia, BF Homes, Paranaque City, Philippines",
    ]
