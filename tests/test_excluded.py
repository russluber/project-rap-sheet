"""
Tests for build_excluded_uploads: the audit of what the 1v1 filtering drops.
"""

from __future__ import annotations

import json

import pandas as pd

from fliptop import RAW_DATA_DIR
from fliptop.battles import build_excluded_uploads, build_ft_battles


def _write_raw(tmp_path, videos, events=None):
    (tmp_path / "youtube_videos.json").write_text(json.dumps(videos), encoding="utf-8")
    pd.DataFrame(
        events or [],
        columns=["matchup", "event_name", "event_description", "video_id"],
    ).to_csv(tmp_path / "matchup_events_metadata.csv", index=False)
    return tmp_path


def test_excluded_tags_each_filter_reason(tmp_path):
    videos = [
        {"id": "keep1", "title": "FlipTop - Loonie vs Abra", "upload_date": "2020-01-01T00:00:00Z", "duration": "PT10M", "url": "u1"},
        {"id": "novs",  "title": "FlipTop Year-End Awards 2019", "upload_date": "2020-01-02T00:00:00Z", "duration": "PT10M", "url": "u2"},
        {"id": "bbox",  "title": "FlipTop - A vs B beatbox",     "upload_date": "2020-01-03T00:00:00Z", "duration": "PT10M", "url": "u3"},
        {"id": "three", "title": "FlipTop - A vs B vs C",        "upload_date": "2020-01-04T00:00:00Z", "duration": "PT10M", "url": "u4"},
    ]
    ex = build_excluded_uploads(_write_raw(tmp_path, videos))

    reasons = dict(zip(ex["id"], ex["excluded_reason"]))
    assert reasons == {
        "novs": "no 'vs' token",
        "bbox": "non-battle keyword",
        "three": "not 1v1",
    }
    assert "keep1" not in set(ex["id"])  # a clean 1v1 is not excluded
    # the matched exclusion keyword is recorded for the non-battle drop
    assert ex.loc[ex["id"] == "bbox", "matched_keyword"].iloc[0] == "beatbox"


def test_excluded_tags_event_name_filter(tmp_path):
    videos = [
        {"id": "keep1", "title": "FlipTop - Loonie vs Abra", "upload_date": "2020-01-01T00:00:00Z", "duration": "PT10M", "url": "u1"},
        {"id": "poione", "title": "FlipTop - A vs B", "upload_date": "2020-01-02T00:00:00Z", "duration": "PT10M", "url": "u2"},
    ]
    events = [
        {
            "matchup": "A vs B",
            "event_name": "Process of Illumination 6 (Visayas)",
            "event_description": "FlipTop presents: POI 6 @ Test Venue. January 1, 2020.",
            "video_id": "poione",
        }
    ]

    ex = build_excluded_uploads(_write_raw(tmp_path, videos, events))
    row = ex.loc[ex["id"] == "poione"].iloc[0]

    assert row["excluded_reason"] == "excluded event"
    assert row["event_name"] == "Process of Illumination 6 (Visayas)"
    assert row["matched_keyword"].casefold() == "process of illumination"
    assert "keep1" not in set(ex["id"])


def test_excluded_ids_disjoint_from_final_battles():
    ex = build_excluded_uploads(RAW_DATA_DIR)
    df = build_ft_battles(raw_dir=RAW_DATA_DIR)

    final_ids = set()
    for v in df["id"]:
        final_ids.update(v) if isinstance(v, list) else final_ids.add(v)

    # nothing the audit lists as excluded should appear in the final battles
    assert set(ex["id"]).isdisjoint(final_ids)
    assert set(ex["excluded_reason"]) <= {
        "no 'vs' token",
        "non-battle keyword",
        "not 1v1",
        "excluded event",
    }


def test_real_event_exclusions_are_complete_and_keep_first_reason():
    ex = build_excluded_uploads(RAW_DATA_DIR)
    events = pd.read_csv(RAW_DATA_DIR / "matchup_events_metadata.csv")
    related = events[
        events["event_name"].str.contains(
            r"Process of Illumination|tryout",
            case=False,
            na=False,
        )
    ]
    audited = ex[ex["id"].isin(related["video_id"])]

    assert len(related) == len(audited) == 26
    assert audited["excluded_reason"].value_counts().to_dict() == {
        "excluded event": 18,
        "non-battle keyword": 6,
        "no 'vs' token": 1,
        "not 1v1": 1,
    }
