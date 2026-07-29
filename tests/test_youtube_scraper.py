"""Network-free safety tests for the standalone YouTube collection script."""

import importlib.util
import json
import sys

import pytest

from fliptop import PROJECT_ROOT

_SCRIPT = PROJECT_ROOT / "scripts" / "fetch_youtube_channel_uploads.py"
_spec = importlib.util.spec_from_file_location("fetch_youtube_uploads", _SCRIPT)
fy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fy)


def test_existing_invalid_json_fails_closed(tmp_path):
    path = tmp_path / "youtube.json"
    path.write_text("not json", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid JSON; refusing to overwrite"):
        fy.load_existing_metadata(str(path))


def test_existing_non_list_json_fails_closed(tmp_path):
    path = tmp_path / "youtube.json"
    path.write_text('{"id": "one-record"}', encoding="utf-8")

    with pytest.raises(ValueError, match="expected a JSON list"):
        fy.load_existing_metadata(str(path))


def test_main_returns_failure_status_when_collection_fails(monkeypatch, capsys):
    def fail(**kwargs):
        raise RuntimeError("API unavailable")

    monkeypatch.setattr(fy, "fetch_channel_uploads", fail)
    monkeypatch.setattr(sys, "argv", [str(_SCRIPT), "--channel", "channel-id"])

    with pytest.raises(SystemExit) as exc:
        fy.main()

    assert exc.value.code == 1
    assert "[error] API unavailable" in capsys.readouterr().err


def test_no_new_uploads_still_migrates_legacy_metrics(tmp_path, monkeypatch):
    path = tmp_path / "youtube.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "aaaaaaaaaaa",
                    "title": "A vs B",
                    "view_count": "100",
                    "likeCount": "10",
                    "commentCount": "2",
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(fy, "load_api_key", lambda **kwargs: "key")
    monkeypatch.setattr(fy, "get_uploads_playlist_id", lambda *args: "playlist")
    monkeypatch.setattr(
        fy,
        "get_all_upload_video_ids",
        lambda *args: ["aaaaaaaaaaa"],
    )
    monkeypatch.setattr(fy, "fetch_video_metadata", lambda *args, **kwargs: [])

    fy.fetch_channel_uploads("channel", output_path=path)

    [stored] = json.loads(path.read_text(encoding="utf-8"))
    assert stored == {"id": "aaaaaaaaaaa", "title": "A vs B"}
