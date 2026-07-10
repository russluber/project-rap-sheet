"""
Tests for build_upload_lineage: the one-row-per-raw-upload audit table.
"""

from __future__ import annotations

import json

import pandas as pd

from fliptop import RAW_DATA_DIR
from fliptop.battles import (
    build_battle_metadata,
    build_excluded_uploads,
    build_manual_matchup_review_uploads,
    build_upload_lineage,
    load_youtube_uploads,
    write_audit_outputs,
)

IN_SCOPE_NO_SHOW_IDS = {
    "Um2XyeCDEew",
    "CoGBvfTVOzA",
    "YHLut41dCG8",
    "pib1VW3SELA",
}
POI_NO_SHOW_ID = "IO6AaGSupuY"


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


def test_pending_manual_matchup_is_surfaced_not_filtered(tmp_path):
    videos = [
        {"id": "noshow", "title": "FlipTop - A + B vs C", "upload_date": "2020-01-01T00:00:00Z", "duration": "PT10M", "url": "u1"},
        {"id": "plain", "title": "FlipTop - D vs E", "upload_date": "2020-01-02T00:00:00Z", "duration": "PT10M", "url": "u2"},
    ]
    manual = {
        "noshow": {
            "emcee1": None,
            "emcee2": None,
            "helper_emcee": None,
            "emcee1_status": None,
            "emcee2_status": None,
            "helper_status": None,
            "note": "needs watching",
        }
    }
    raw_dir = _write_raw(tmp_path, videos)

    excluded = build_excluded_uploads(raw_dir, manual_matchups=manual)
    lineage = build_upload_lineage(raw_dir, manual_matchups=manual).set_index("id")
    needed = build_manual_matchup_review_uploads(raw_dir, manual_matchups=manual)

    assert "noshow" not in set(excluded["id"])
    assert lineage.loc["noshow", "pipeline_status"] == "needs_manual_matchup"
    assert lineage.loc["noshow", "manual_note"] == "needs watching"
    assert needed["id"].tolist() == ["noshow"]


def test_resolved_manual_matchup_survives_plus_title_for_in_scope_event(tmp_path):
    videos = [
        {"id": "noshow", "title": "FlipTop - A + B vs C", "upload_date": "2020-01-01T00:00:00Z", "duration": "PT10M", "url": "u1"},
    ]
    events = [
        {
            "matchup": "A + B vs C",
            "event_name": "Gubat 3",
            "event_description": "FlipTop presents: Gubat 3 @ Test Venue. January 1, 2020.",
            "video_id": "noshow",
        }
    ]
    manual = {
        "noshow": {
            "emcee1": "B",
            "emcee2": "C",
            "helper_emcee": "A",
            "emcee1_status": "appeared",
            "emcee2_status": "no_show",
            "helper_status": "appeared",
            "note": "watched",
        }
    }

    metadata = build_battle_metadata(
        _write_raw(tmp_path, videos, events),
        rename_map={},
        manual_matchups=manual,
        vt_event_dates={},
    )

    assert metadata["id"].tolist() == ["noshow"]
    assert metadata["matchup"].tolist() == ["B vs C"]
    assert metadata["event_name"].tolist() == ["Gubat 3"]


def test_resolved_manual_matchup_still_respects_event_exclusion(tmp_path):
    videos = [
        {"id": "noshow", "title": "FlipTop - A + B vs C", "upload_date": "2020-01-01T00:00:00Z", "duration": "PT10M", "url": "u1"},
    ]
    events = [
        {
            "matchup": "A + B vs C",
            "event_name": "Process of Illumination 6",
            "event_description": "FlipTop presents: POI 6 @ Test Venue. January 1, 2020.",
            "video_id": "noshow",
        }
    ]
    manual = {
        "noshow": {
            "emcee1": "B",
            "emcee2": "C",
            "helper_emcee": "A",
            "emcee1_status": "appeared",
            "emcee2_status": "no_show",
            "helper_status": "appeared",
            "note": "watched",
        }
    }

    metadata = build_battle_metadata(
        _write_raw(tmp_path, videos, events),
        rename_map={},
        manual_matchups=manual,
        vt_event_dates={},
    )

    assert metadata.empty


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
        "needs_manual_matchup",
    }
    assert (lineage["pipeline_status"] == "unclassified").sum() == 0
    missing_ids = set(lineage.loc[lineage["annotation_status"] == "missing", "id"])
    assert missing_ids <= IN_SCOPE_NO_SHOW_IDS


def test_real_no_show_manual_matchups_are_not_generic_exclusions_unless_event_excluded():
    excluded = build_excluded_uploads(RAW_DATA_DIR)
    lineage = build_upload_lineage(RAW_DATA_DIR)
    needed = build_manual_matchup_review_uploads(RAW_DATA_DIR)

    assert IN_SCOPE_NO_SHOW_IDS.isdisjoint(set(excluded["id"]))
    assert needed.empty

    no_shows = lineage.set_index("id").loc[list(IN_SCOPE_NO_SHOW_IDS)]
    assert set(no_shows["pipeline_status"]) == {"included"}
    assert set(no_shows["emcee1_status"]) == {"appeared"}
    assert set(no_shows["emcee2_status"]) == {"no_show"}
    assert set(no_shows["helper_status"]) == {"appeared"}
    assert no_shows["helper_emcee"].notna().all()

    poi_excluded = excluded.set_index("id").loc[POI_NO_SHOW_ID]
    assert poi_excluded["excluded_reason"] == "excluded event"
    assert poi_excluded["matched_keyword"].casefold() == "process of illumination"
    assert lineage.set_index("id").loc[POI_NO_SHOW_ID, "pipeline_status"] == "excluded"


def test_write_audit_outputs_writes_filtered_and_lineage_files(tmp_path):
    excluded_path, lineage_path, manual_path = write_audit_outputs(RAW_DATA_DIR, tmp_path)

    assert excluded_path.exists()
    assert lineage_path.exists()
    assert manual_path.exists()
    assert excluded_path.name == "filtered_out.csv"
    assert lineage_path.name == "upload_lineage.csv"
    assert manual_path.name == "manual_matchup_needed.csv"

    lineage = pd.read_csv(lineage_path)
    manual = pd.read_csv(manual_path)
    raw = load_youtube_uploads(RAW_DATA_DIR / "youtube_videos.json")
    assert len(lineage) == len(raw)
    assert manual.empty
