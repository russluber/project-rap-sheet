"""
Tests for build_upload_lineage: the one-row-per-raw-upload audit table.
"""

from __future__ import annotations

import json

import pandas as pd

from fliptop import RAW_DATA_DIR
from fliptop.battles import (
    build_excluded_uploads,
    build_upload_lineage,
    load_youtube_uploads,
    write_audit_outputs,
)


def _write_raw(tmp_path, videos, events=None):
    (tmp_path / "youtube_videos.json").write_text(json.dumps(videos), encoding="utf-8")
    pd.DataFrame(
        events or [],
        columns=["matchup", "event_name", "event_description", "video_id"],
    ).to_csv(tmp_path / "matchup_events_metadata.csv", index=False)
    return tmp_path


def test_lineage_tags_included_excluded_and_consolidated_rows(tmp_path):
    videos = [
        {"id": "keep1", "title": "FlipTop - A vs B", "upload_date": "2020-01-01T00:00:00Z", "duration": "PT10M", "url": "u1"},
        {"id": "promo", "title": "FlipTop - C vs D (PROMO)", "upload_date": "2020-01-02T00:00:00Z", "duration": "PT10M", "url": "u2"},
        {"id": "novs", "title": "FlipTop Trailer", "upload_date": "2020-01-03T00:00:00Z", "duration": "PT10M", "url": "u3"},
        {"id": "three", "title": "FlipTop - A vs B vs C", "upload_date": "2020-01-04T00:00:00Z", "duration": "PT10M", "url": "u4"},
        {"id": "part1", "title": "FlipTop - E vs F pt. 1", "upload_date": "2020-01-05T00:00:00Z", "duration": "PT10M", "url": "u5"},
        {"id": "part2", "title": "FlipTop - E vs F pt. 2", "upload_date": "2020-01-06T00:00:00Z", "duration": "PT10M", "url": "u6"},
    ]

    lineage = build_upload_lineage(_write_raw(tmp_path, videos)).set_index("id")

    assert len(lineage) == len(videos)
    assert lineage.loc["keep1", "pipeline_status"] == "included"
    assert lineage.loc["promo", "pipeline_status"] == "included"
    assert lineage.loc["promo", "final_matchup"] == "C vs D"
    assert lineage.loc["novs", "pipeline_status"] == "excluded"
    assert lineage.loc["novs", "excluded_reason"] == "no 'vs' token"
    assert lineage.loc["three", "pipeline_status"] == "excluded"
    assert lineage.loc["three", "stage"] == "keep_1v1"
    assert lineage.loc["part1", "pipeline_status"] == "included"
    assert lineage.loc["part2", "pipeline_status"] == "consolidated_part"
    assert lineage.loc["part2", "battle_key"] == "part1"


def test_real_lineage_has_one_row_per_raw_upload_and_matches_excluded_view():
    raw = load_youtube_uploads(RAW_DATA_DIR / "youtube_videos.json")
    lineage = build_upload_lineage(RAW_DATA_DIR)
    excluded = build_excluded_uploads(RAW_DATA_DIR)

    assert len(lineage) == len(raw)
    assert set(lineage["id"]) == set(raw["id"].astype(str))

    lineage_excluded = lineage[lineage["pipeline_status"] == "excluded"]
    assert set(lineage_excluded["id"]) == set(excluded["id"].astype(str))
    reasons = dict(zip(lineage_excluded["id"], lineage_excluded["excluded_reason"]))
    expected_reasons = dict(zip(excluded["id"].astype(str), excluded["excluded_reason"]))
    assert reasons == expected_reasons


def test_real_lineage_accounts_for_every_upload_once():
    lineage = build_upload_lineage(RAW_DATA_DIR)

    assert not lineage["id"].duplicated().any()
    assert set(lineage["pipeline_status"]) <= {
        "included",
        "consolidated_part",
        "excluded",
    }
    assert (lineage["pipeline_status"] == "unclassified").sum() == 0
    assert (lineage["annotation_status"] == "missing").sum() == 0


def test_write_audit_outputs_writes_filtered_and_lineage_files(tmp_path):
    excluded_path, lineage_path = write_audit_outputs(RAW_DATA_DIR, tmp_path)

    assert excluded_path.exists()
    assert lineage_path.exists()
    assert excluded_path.name == "filtered_out.csv"
    assert lineage_path.name == "upload_lineage.csv"

    lineage = pd.read_csv(lineage_path)
    raw = load_youtube_uploads(RAW_DATA_DIR / "youtube_videos.json")
    assert len(lineage) == len(raw)
