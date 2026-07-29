"""Safety tests for the standalone current-metrics collector."""

from __future__ import annotations

import importlib.util
import json
import sys

import pandas as pd
import pytest

from fliptop import PROJECT_ROOT
from fliptop.youtube_metrics import load_youtube_video_metrics

_SCRIPT = PROJECT_ROOT / "scripts" / "fetch_youtube_video_metrics.py"
_SPEC = importlib.util.spec_from_file_location("fetch_youtube_metrics", _SCRIPT)
metrics_script = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(metrics_script)


def _write_ids(path, *video_ids):
    path.write_text(
        json.dumps([{"id": video_id} for video_id in video_ids]),
        encoding="utf-8",
    )


def test_load_video_ids_rejects_duplicates(tmp_path):
    path = tmp_path / "youtube.json"
    _write_ids(path, "same", "same")

    with pytest.raises(ValueError, match="duplicate video IDs"):
        metrics_script.load_video_ids(path)


def test_refresh_creates_valid_current_store(tmp_path, monkeypatch):
    ids_path = tmp_path / "youtube.json"
    output = tmp_path / "metrics.csv"
    _write_ids(ids_path, "aaaaaaaaaaa", "bbbbbbbbbbb")
    monkeypatch.setattr(metrics_script, "load_api_key", lambda path: "key")
    monkeypatch.setattr(
        metrics_script,
        "fetch_video_statistics",
        lambda ids, key: [
            {
                "video_id": video_id,
                "view_count": "100",
                "like_count": "10",
                "comment_count": "2",
            }
            for video_id in ids
        ],
    )

    metrics_script.refresh_youtube_video_metrics(
        ids_path=ids_path,
        output_path=output,
        checked_at="2026-07-28T00:00:00Z",
    )
    loaded = load_youtube_video_metrics(output)

    assert loaded["video_id"].tolist() == ["aaaaaaaaaaa", "bbbbbbbbbbb"]
    assert loaded["view_count"].tolist() == [100, 100]
    assert loaded["fetch_status"].tolist() == ["ok", "ok"]


def test_api_failure_leaves_existing_store_unchanged(tmp_path, monkeypatch):
    ids_path = tmp_path / "youtube.json"
    output = tmp_path / "metrics.csv"
    _write_ids(ids_path, "aaaaaaaaaaa")
    output.write_text("known-good\n", encoding="utf-8")
    old = output.read_bytes()
    monkeypatch.setattr(metrics_script, "load_youtube_video_metrics", lambda path: pd.DataFrame())
    monkeypatch.setattr(metrics_script, "load_api_key", lambda path: "key")

    def fail(ids, key):
        raise RuntimeError("API unavailable")

    monkeypatch.setattr(metrics_script, "fetch_video_statistics", fail)

    with pytest.raises(RuntimeError, match="API unavailable"):
        metrics_script.refresh_youtube_video_metrics(
            ids_path=ids_path,
            output_path=output,
        )

    assert output.read_bytes() == old


def test_main_returns_failure_status(monkeypatch, capsys):
    def fail(**kwargs):
        raise RuntimeError("API unavailable")

    monkeypatch.setattr(metrics_script, "refresh_youtube_video_metrics", fail)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT)])

    with pytest.raises(SystemExit) as exc:
        metrics_script.main()

    assert exc.value.code == 1
    assert "[error] API unavailable" in capsys.readouterr().err
