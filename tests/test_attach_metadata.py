"""
Tests for attach_event_metadata, including the COVID-era date mask.

The join is on YouTube video id (left ``id`` <-> right ``video_id``). After the
join, event_date is cleared for uploads inside the COVID window
(2020-05-01 .. 2022-04-27), reflecting that FlipTop obfuscated those dates.
"""

from __future__ import annotations

import pandas as pd

from fliptop.events import attach_event_metadata


def _events():
    # Mirrors the scraped CSV schema: matchup, event_name, event_description, video_id
    return pd.DataFrame(
        {
            "video_id": ["a", "b", "c"],
            "event_name": ["Event A", "Event B", "Event C"],
            "event_description": [
                "E @ X. Jan. 1, 2021.",   # inside COVID window
                "E @ Y. Jan. 1, 2019.",   # before
                "E @ Z. Jan. 1, 2023.",   # after
            ],
        }
    )


def _uploads():
    return pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "upload_date": pd.to_datetime(["2021-01-01", "2019-01-01", "2023-01-01"]),
            "description": ["", "", ""],
        }
    )


def test_attach_joins_event_metadata_on_video_id():
    out = attach_event_metadata(_uploads(), _events())
    assert "event_name" in out.columns
    assert "event_date" in out.columns
    assert len(out) == 3


def test_covid_window_clears_event_date():
    out = attach_event_metadata(_uploads(), _events()).set_index("id")
    assert pd.isna(out.loc["a", "event_date"])               # 2021 -> cleared
    assert out.loc["b", "event_date"] == pd.Timestamp("2019-01-01")  # kept
    assert out.loc["c", "event_date"] == pd.Timestamp("2023-01-01")  # kept


def test_attach_with_empty_events_returns_uploads_unchanged():
    uploads = _uploads()
    out = attach_event_metadata(uploads, pd.DataFrame())
    assert len(out) == len(uploads)
    assert out["id"].tolist() == uploads["id"].tolist()
