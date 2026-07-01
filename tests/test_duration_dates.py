"""
Tests for duration parsing and date parsing:

    add_duration_columns, parse_upload_date, parse_event_date,
    convert_video_metrics_to_numeric
"""

from __future__ import annotations

import pandas as pd

from fliptop.battles import (
    add_duration_columns,
    convert_video_metrics_to_numeric,
    parse_event_date,
    parse_upload_date,
)

# ---------------------------------------------------------------------------
# add_duration_columns
# ---------------------------------------------------------------------------

def test_add_duration_columns_parses_iso8601():
    df = pd.DataFrame({"duration": ["PT33M42S", "PT1H2M3S"]})
    out = add_duration_columns(df)
    assert out["duration_seconds"].tolist() == [2022.0, 3723.0]
    assert out["duration_hms"].tolist() == ["00:33:42", "01:02:03"]


def test_add_duration_columns_handles_missing_and_garbage():
    df = pd.DataFrame({"duration": [None, "not-a-duration"]})
    out = add_duration_columns(df)
    assert out["duration_seconds"].isna().all()
    assert out["duration_hms"].isna().all()


# ---------------------------------------------------------------------------
# parse_upload_date
# ---------------------------------------------------------------------------

def test_parse_upload_date_is_tz_naive():
    df = pd.DataFrame({"upload_date": ["2026-02-19T12:40:15Z"]})
    out = parse_upload_date(df)
    ts = out["upload_date"].iloc[0]
    assert ts == pd.Timestamp("2026-02-19 12:40:15")
    assert ts.tzinfo is None


def test_parse_upload_date_bad_value_is_nat():
    df = pd.DataFrame({"upload_date": ["nonsense"]})
    assert pd.isna(parse_upload_date(df)["upload_date"].iloc[0])


# ---------------------------------------------------------------------------
# parse_event_date
# ---------------------------------------------------------------------------

def test_parse_event_date_to_datetime():
    df = pd.DataFrame({"event_date": ["2010-12-04", None, "bad"]})
    out = parse_event_date(df)
    assert out["event_date"].iloc[0] == pd.Timestamp("2010-12-04")
    assert pd.isna(out["event_date"].iloc[1])
    assert pd.isna(out["event_date"].iloc[2])


# ---------------------------------------------------------------------------
# convert_video_metrics_to_numeric
# ---------------------------------------------------------------------------

def test_convert_video_metrics_to_numeric():
    df = pd.DataFrame(
        {
            "view_count": ["1000", "2500", None],
            "likeCount": ["50", "x", "10"],
            "commentCount": ["5", "6", "7"],
        }
    )
    out = convert_video_metrics_to_numeric(df)
    assert out["view_count"].tolist()[:2] == [1000, 2500]
    assert pd.isna(out["view_count"].iloc[2])
    assert pd.isna(out["likeCount"].iloc[1])  # non-numeric coerced to NaN
