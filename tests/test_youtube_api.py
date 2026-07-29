"""Network-free tests for shared YouTube API request helpers."""

from __future__ import annotations

from fliptop import youtube_api


class _Response:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        return None

    def json(self):
        return self._data


def test_statistics_are_fetched_in_batches_with_one_statistics_part(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append((url, params.copy(), timeout))
        ids = str(params["id"]).split(",")
        return _Response(
            {
                "items": [
                    {
                        "id": video_id,
                        "statistics": {
                            "viewCount": "100",
                            "likeCount": "10",
                            "commentCount": "2",
                        },
                    }
                    for video_id in ids
                ]
            }
        )

    monkeypatch.setattr(youtube_api.requests, "get", fake_get)
    video_ids = [f"video-{number:03d}" for number in range(51)]

    records = youtube_api.fetch_video_statistics(video_ids, "key", sleep=0)

    assert len(records) == 51
    assert len(calls) == 2
    assert all(call[1]["part"] == "statistics" for call in calls)
    assert [len(str(call[1]["id"]).split(",")) for call in calls] == [50, 1]
    assert records[0] == {
        "video_id": "video-000",
        "view_count": "100",
        "like_count": "10",
        "comment_count": "2",
    }


def test_descriptive_metadata_excludes_changing_statistics(monkeypatch):
    def fake_get(url, params, timeout):
        assert params["part"] == "snippet,contentDetails"
        return _Response(
            {
                "items": [
                    {
                        "id": "aaaaaaaaaaa",
                        "snippet": {
                            "title": "A vs B",
                            "description": "description",
                            "publishedAt": "2026-01-01T00:00:00Z",
                            "tags": ["battle"],
                        },
                        "contentDetails": {"duration": "PT10M"},
                    }
                ]
            }
        )

    monkeypatch.setattr(youtube_api.requests, "get", fake_get)

    records = youtube_api.fetch_video_metadata(["aaaaaaaaaaa"], "key", sleep=0)

    assert "view_count" not in records[0]
    assert "likeCount" not in records[0]
    assert "commentCount" not in records[0]
