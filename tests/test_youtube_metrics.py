"""Tests for current YouTube metrics storage and battle-level attachment."""

from __future__ import annotations

import pandas as pd
import pytest

import fliptop
from fliptop import RAW_DATA_DIR
from fliptop.battles import load_youtube_uploads
from fliptop.contracts import ContractViolation
from fliptop.youtube_metrics import (
    ATTACHED_METRIC_COLUMNS,
    attach_youtube_metrics,
    build_battle_video_map,
    load_youtube_video_metrics,
    merge_youtube_metric_refresh,
    save_youtube_video_metrics,
)


def _metrics(*rows):
    defaults = {
        "view_count": 100,
        "like_count": 10,
        "comment_count": 2,
        "observed_at": "2026-07-01T00:00:00Z",
        "checked_at": "2026-07-01T00:00:00Z",
        "fetch_status": "ok",
    }
    return pd.DataFrame(
        [{**defaults, **row} for row in rows],
        columns=[
            "video_id",
            "view_count",
            "like_count",
            "comment_count",
            "observed_at",
            "checked_at",
            "fetch_status",
        ],
    )


def test_package_exports_metrics_api():
    assert fliptop.load_youtube_video_metrics is load_youtube_video_metrics
    assert fliptop.build_battle_video_map is build_battle_video_map
    assert fliptop.attach_youtube_metrics is attach_youtube_metrics


def test_committed_metrics_exactly_cover_upload_inventory():
    uploads = load_youtube_uploads(RAW_DATA_DIR / "youtube_videos.json")
    metrics = load_youtube_video_metrics(
        RAW_DATA_DIR / "youtube_video_metrics.csv"
    )

    assert set(metrics["video_id"]) == set(uploads["id"])
    assert metrics["fetch_status"].eq("ok").all()


def test_metrics_round_trip_uses_nullable_counts_and_utc_dates(tmp_path):
    path = tmp_path / "metrics.csv"
    frame = _metrics(
        {
            "video_id": "aaaaaaaaaaa",
            "like_count": pd.NA,
            "comment_count": pd.NA,
        }
    )

    save_youtube_video_metrics(frame, path)
    loaded = load_youtube_video_metrics(path)

    assert loaded["view_count"].dtype == "Int64"
    assert loaded["like_count"].dtype == "Int64"
    assert str(loaded["observed_at"].dtype) == "datetime64[us, UTC]"
    assert loaded.loc[0, "video_id"] == "aaaaaaaaaaa"
    assert pd.isna(loaded.loc[0, "like_count"])


def test_metrics_contract_rejects_duplicates_negative_counts_and_bad_status():
    frame = _metrics(
        {"video_id": "aaaaaaaaaaa", "view_count": -1},
        {"video_id": "aaaaaaaaaaa", "fetch_status": "mystery"},
    )

    with pytest.raises(ContractViolation) as exc_info:
        save_youtube_video_metrics(frame, "unused.csv")

    message = str(exc_info.value)
    assert "duplicate key [video_id]" in message
    assert "invalid value" in message


def test_refresh_overwrites_counts_even_when_views_decrease():
    existing = _metrics({"video_id": "aaaaaaaaaaa", "view_count": 100})
    fetched = pd.DataFrame(
        [
            {
                "video_id": "aaaaaaaaaaa",
                "view_count": 95,
                "like_count": 11,
                "comment_count": 3,
            }
        ]
    )

    out = merge_youtube_metric_refresh(
        existing,
        fetched,
        ["aaaaaaaaaaa"],
        checked_at="2026-07-08T00:00:00Z",
    )

    assert out.loc[0, "view_count"] == 95
    assert out.loc[0, "fetch_status"] == "ok"
    assert out.loc[0, "observed_at"] == pd.Timestamp("2026-07-08T00:00:00Z")


def test_not_returned_video_keeps_last_counts_and_marks_check():
    existing = _metrics({"video_id": "aaaaaaaaaaa", "view_count": 100})
    fetched = pd.DataFrame(
        columns=["video_id", "view_count", "like_count", "comment_count"]
    )

    out = merge_youtube_metric_refresh(
        existing,
        fetched,
        ["aaaaaaaaaaa"],
        checked_at="2026-07-08T00:00:00Z",
    )

    assert out.loc[0, "view_count"] == 100
    assert out.loc[0, "observed_at"] == pd.Timestamp("2026-07-01T00:00:00Z")
    assert out.loc[0, "checked_at"] == pd.Timestamp("2026-07-08T00:00:00Z")
    assert out.loc[0, "fetch_status"] == "not_returned"


def test_new_not_returned_video_has_missing_counts():
    fetched = pd.DataFrame(
        columns=["video_id", "view_count", "like_count", "comment_count"]
    )

    out = merge_youtube_metric_refresh(
        None,
        fetched,
        ["aaaaaaaaaaa"],
        checked_at="2026-07-08T00:00:00Z",
    )

    assert pd.isna(out.loc[0, "view_count"])
    assert pd.isna(out.loc[0, "observed_at"])
    assert out.loc[0, "fetch_status"] == "not_returned"


def test_refresh_rejects_existing_ids_outside_inventory():
    existing = _metrics({"video_id": "orphaned-id"})
    fetched = pd.DataFrame(
        columns=["video_id", "view_count", "like_count", "comment_count"]
    )

    with pytest.raises(ValueError, match="absent from the upload inventory"):
        merge_youtube_metric_refresh(
            existing,
            fetched,
            ["aaaaaaaaaaa"],
            checked_at="2026-07-08T00:00:00Z",
        )


def test_battle_video_map_supports_scalar_and_published_multipart_rows():
    battles = pd.DataFrame(
        {
            "id": ["aaaaaaaaaaa", "bbbbbbbbbbb"],
            "url": [
                "https://www.youtube.com/watch?v=aaaaaaaaaaa",
                [
                    "https://www.youtube.com/watch?v=bbbbbbbbbbb",
                    "https://www.youtube.com/watch?v=ccccccccccc",
                ],
            ],
        }
    )

    mapping = build_battle_video_map(battles)

    assert mapping.to_dict("records") == [
        {
            "battle_id": "aaaaaaaaaaa",
            "video_id": "aaaaaaaaaaa",
            "part_number": 1,
        },
        {
            "battle_id": "bbbbbbbbbbb",
            "video_id": "bbbbbbbbbbb",
            "part_number": 1,
        },
        {
            "battle_id": "bbbbbbbbbbb",
            "video_id": "ccccccccccc",
            "part_number": 2,
        },
    ]


def test_battle_video_map_prefers_rich_list_ids():
    battles = pd.DataFrame(
        {
            "id": [["aaaaaaaaaaa", "bbbbbbbbbbb"]],
            "url": [["not-needed", "not-needed"]],
        }
    )

    mapping = build_battle_video_map(battles)

    assert mapping["video_id"].tolist() == ["aaaaaaaaaaa", "bbbbbbbbbbb"]


def test_attach_metrics_sums_multipart_uploads_and_preserves_rows():
    battles = pd.DataFrame(
        {
            "id": ["aaaaaaaaaaa", "bbbbbbbbbbb"],
            "title": ["A vs B", "C vs D"],
            "url": [
                "https://www.youtube.com/watch?v=aaaaaaaaaaa",
                [
                    "https://www.youtube.com/watch?v=bbbbbbbbbbb",
                    "https://www.youtube.com/watch?v=ccccccccccc",
                ],
            ],
        }
    )
    metrics = _metrics(
        {"video_id": "aaaaaaaaaaa", "view_count": 100, "like_count": 10},
        {"video_id": "bbbbbbbbbbb", "view_count": 200, "like_count": 20},
        {"video_id": "ccccccccccc", "view_count": 300, "like_count": 30},
    )

    out = attach_youtube_metrics(battles, metrics)

    assert out["id"].tolist() == battles["id"].tolist()
    assert out["youtube_view_count"].tolist() == [100, 500]
    assert out["youtube_like_count"].tolist() == [10, 50]
    assert out["youtube_video_count"].tolist() == [1, 2]
    assert out["youtube_metrics_complete"].tolist() == [True, True]
    assert "youtube_view_count" not in battles.columns


def test_attach_metrics_refuses_partial_multipart_totals():
    battles = pd.DataFrame(
        {
            "id": ["aaaaaaaaaaa"],
            "url": [
                [
                    "https://www.youtube.com/watch?v=aaaaaaaaaaa",
                    "https://www.youtube.com/watch?v=bbbbbbbbbbb",
                ]
            ],
        }
    )
    metrics = _metrics(
        {"video_id": "aaaaaaaaaaa", "view_count": 100},
        {
            "video_id": "bbbbbbbbbbb",
            "view_count": 200,
            "fetch_status": "not_returned",
        },
    )

    out = attach_youtube_metrics(battles, metrics)

    assert pd.isna(out.loc[0, "youtube_view_count"])
    assert not out.loc[0, "youtube_metrics_complete"]
    assert out.loc[0, "youtube_video_count"] == 2


def test_attach_metrics_supports_an_empty_battle_table():
    battles = pd.DataFrame(columns=["id", "url"])

    out = attach_youtube_metrics(
        battles,
        _metrics({"video_id": "aaaaaaaaaaa"}),
    )

    assert out.empty
    assert set(out.columns) == {"id", "url", *ATTACHED_METRIC_COLUMNS}
