"""Network-free tests for transactional raw snapshot refreshes."""

import json
from pathlib import Path

import pandas as pd
import pytest

from fliptop import raw_snapshot as raw_mod
from fliptop import refresh as refresh_mod
from fliptop.contracts import ContractViolation


def _youtube_row(video_id: str):
    return {
        "id": video_id,
        "title": f"FlipTop - A vs B {video_id}",
        "description": "FlipTop presents: Test @ Venue. January 1, 2020.",
        "upload_date": "2020-02-01T00:00:00Z",
        "view_count": "100",
        "duration": "PT10M",
        "url": f"https://example.test/{video_id}",
        "likeCount": "10",
        "commentCount": "2",
        "tags": ["battle"],
    }


def _write_snapshot(directory: Path, marker: str) -> dict[str, bytes]:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "youtube_videos.json").write_text(
        json.dumps([_youtube_row(f"video-{marker}")]),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "matchup": "A vs B",
                "event_name": f"Event {marker}",
                "event_description": "Event @ Venue. January 1, 2020.",
                "video_id": f"video-{marker}",
            }
        ]
    ).to_csv(directory / "matchup_events_metadata.csv", index=False)
    pd.DataFrame(
        [
            {
                "event_name": f"Event {marker}",
                "event_date": "2020-01-01",
                "source_url": "https://example.test/event",
            }
        ]
    ).to_csv(directory / "versetracker_event_dates.csv", index=False)
    return {
        filename: (directory / filename).read_bytes()
        for filename in raw_mod.RAW_SNAPSHOT_FILENAMES
    }


def _output_path(args: list[str]) -> Path:
    return Path(args[args.index("--output") + 1])


def test_fetch_raw_publishes_only_after_both_collectors_succeed(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    old = _write_snapshot(raw_dir, "old")
    called_outputs = []

    def fake_run(script_path, args):
        output = _output_path(args)
        called_outputs.append(output)
        assert output.parent != raw_dir
        assert {
            filename: (raw_dir / filename).read_bytes()
            for filename in raw_mod.RAW_SNAPSHOT_FILENAMES
        } == old
        if "youtube" in script_path.name:
            output.write_text(json.dumps([_youtube_row("video-new")]), encoding="utf-8")
        else:
            pd.DataFrame(
                [
                    {
                        "matchup": "A vs B",
                        "event_name": "Event new",
                        "event_description": "Event @ Venue. January 1, 2020.",
                        "video_id": "video-new",
                    }
                ]
            ).to_csv(output, index=False)

    monkeypatch.setattr(refresh_mod, "_run_script", fake_run)

    refresh_mod.fetch_raw(raw_dir, end_year=2020)

    assert len(called_outputs) == 2
    assert "video-new" in (raw_dir / "youtube_videos.json").read_text(encoding="utf-8")
    assert "Event new" in (raw_dir / "matchup_events_metadata.csv").read_text(
        encoding="utf-8"
    )
    assert (raw_dir / "versetracker_event_dates.csv").read_bytes() == old[
        "versetracker_event_dates.csv"
    ]


def test_fetch_raw_keeps_old_snapshot_when_second_collector_fails(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    old = _write_snapshot(raw_dir, "old")

    def fake_run(script_path, args):
        output = _output_path(args)
        if "youtube" in script_path.name:
            output.write_text(json.dumps([_youtube_row("video-new")]), encoding="utf-8")
            return
        raise RuntimeError("event collection failed")

    monkeypatch.setattr(refresh_mod, "_run_script", fake_run)

    with pytest.raises(RuntimeError, match="event collection failed"):
        refresh_mod.fetch_raw(raw_dir, end_year=2020)

    for filename, contents in old.items():
        assert (raw_dir / filename).read_bytes() == contents


def test_fetch_raw_keeps_old_snapshot_when_candidate_is_invalid(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    old = _write_snapshot(raw_dir, "old")

    def fake_run(script_path, args):
        output = _output_path(args)
        if "youtube" in script_path.name:
            output.write_text(json.dumps([_youtube_row("video-new")]), encoding="utf-8")
        else:
            output.write_text("wrong,column\nvalue,value\n", encoding="utf-8")

    monkeypatch.setattr(refresh_mod, "_run_script", fake_run)

    with pytest.raises(ContractViolation, match="raw FlipTop event metadata"):
        refresh_mod.fetch_raw(raw_dir, end_year=2020)

    for filename, contents in old.items():
        assert (raw_dir / filename).read_bytes() == contents


def test_raw_promotion_restores_every_file_after_mid_publish_failure(
    tmp_path,
    monkeypatch,
):
    raw_dir = tmp_path / "raw"
    old = _write_snapshot(raw_dir, "old")
    staging_dir = tmp_path / "staging"
    _write_snapshot(staging_dir, "new")

    real_replace = raw_mod._replace_for_raw_publish
    call_count = 0

    def fail_during_promotion(source, destination):
        nonlocal call_count
        call_count += 1
        if call_count == 5:
            raise OSError("forced raw promotion failure")
        real_replace(source, destination)

    monkeypatch.setattr(raw_mod, "_replace_for_raw_publish", fail_during_promotion)

    with pytest.raises(OSError, match="forced raw promotion failure"):
        raw_mod.publish_raw_snapshot(staging_dir, raw_dir)

    for filename, contents in old.items():
        assert (raw_dir / filename).read_bytes() == contents
    assert not list(tmp_path.glob(".raw-backup-*"))
