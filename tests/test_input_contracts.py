"""Boundary tests for raw-source table contracts."""

import json

import pandas as pd
import pytest

from fliptop.battles import load_event_metadata, load_youtube_uploads
from fliptop.contracts import ContractViolation


def _youtube_row(**changes):
    row = {
        "id": "video-one",
        "title": "FlipTop - A vs B",
        "description": "event description",
        "upload_date": "2026-01-01T00:00:00Z",
        "view_count": "100",
        "duration": "PT10M",
        "url": "https://example.test/video-one",
        "likeCount": "10",
        "commentCount": "2",
        "tags": ["battle"],
    }
    row.update(changes)
    return row


def test_youtube_loader_rejects_missing_source_field(tmp_path):
    path = tmp_path / "youtube.json"
    row = _youtube_row()
    del row["duration"]
    path.write_text(json.dumps([row]), encoding="utf-8")

    with pytest.raises(ContractViolation, match="missing required columns: duration"):
        load_youtube_uploads(path)


def test_youtube_loader_rejects_duplicate_video_ids(tmp_path):
    path = tmp_path / "youtube.json"
    path.write_text(json.dumps([_youtube_row(), _youtube_row()]), encoding="utf-8")

    with pytest.raises(ContractViolation, match=r"duplicate key \[id\]"):
        load_youtube_uploads(path)


def test_event_loader_rejects_blank_and_duplicate_keys_together(tmp_path):
    path = tmp_path / "events.csv"
    pd.DataFrame(
        {
            "matchup": ["A vs B", "C vs D"],
            "event_name": ["", "Event"],
            "event_description": ["Description", "Description"],
            "video_id": ["same", "same"],
        }
    ).to_csv(path, index=False)

    with pytest.raises(ContractViolation) as exc_info:
        load_event_metadata(path)

    message = str(exc_info.value)
    assert "event_name has 1 blank value" in message
    assert "duplicate key [video_id]" in message
