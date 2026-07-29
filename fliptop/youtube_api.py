"""Small internal client helpers for the YouTube Data API v3."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import requests

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_SECRET_PATH = Path("data") / "secret" / "secret.json"


def load_api_key(secret_path: str | Path = DEFAULT_SECRET_PATH) -> str:
    """Load the API key from ``YOUTUBE_API_KEY`` or the project's secret JSON."""
    env_key = os.getenv("YOUTUBE_API_KEY")
    if env_key:
        return env_key

    secret_path = Path(secret_path)
    if secret_path.exists():
        data = json.loads(secret_path.read_text(encoding="utf-8"))
        key = data.get("YT_API_KEY")
        if key:
            return str(key)

    raise RuntimeError(
        "YouTube API key not found. Set YOUTUBE_API_KEY env var or "
        f"create {secret_path} with a 'YT_API_KEY' field."
    )


def _get_json(url: str, params: dict[str, object], *, label: str) -> dict[str, Any]:
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to {label}: {exc}") from exc
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Failed to {label}: expected a JSON object response")
    return data


def get_uploads_playlist_id(channel_id: str, api_key: str) -> str:
    """Return the uploads-playlist ID for a YouTube channel."""
    data = _get_json(
        f"{YOUTUBE_API_BASE}/channels",
        {
            "part": "contentDetails",
            "id": channel_id,
            "key": api_key,
        },
        label="fetch uploads playlist",
    )
    try:
        return str(data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"])
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(
            f"Could not retrieve uploads playlist ID for channel {channel_id}."
        ) from exc


def get_all_upload_video_ids(
    uploads_playlist_id: str,
    api_key: str,
    *,
    sleep: float = 0.2,
) -> list[str]:
    """Page through a channel's uploads playlist and return every video ID."""
    video_ids: list[str] = []
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    params: dict[str, object] = {
        "part": "contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 50,
        "key": api_key,
    }

    while True:
        data = _get_json(url, params, label="fetch video IDs")
        for item in data.get("items", []):
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                video_ids.append(str(video_id))

        next_token = data.get("nextPageToken")
        if not next_token:
            break
        params["pageToken"] = next_token
        time.sleep(sleep)

    if len(video_ids) != len(set(video_ids)):
        raise RuntimeError("YouTube uploads playlist returned duplicate video IDs")
    return video_ids


def fetch_video_metadata(
    video_ids: list[str],
    api_key: str,
    *,
    existing_ids: set[str] | None = None,
    sleep: float = 0.2,
) -> list[dict[str, Any]]:
    """Fetch descriptive metadata for video IDs not already stored."""
    existing_ids = existing_ids or set()
    records: list[dict[str, Any]] = []
    url = f"{YOUTUBE_API_BASE}/videos"

    for start in range(0, len(video_ids), 50):
        batch = [
            video_id
            for video_id in video_ids[start : start + 50]
            if video_id not in existing_ids
        ]
        if not batch:
            continue
        data = _get_json(
            url,
            {
                "part": "snippet,contentDetails",
                "id": ",".join(batch),
                "key": api_key,
            },
            label="fetch video metadata",
        )
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            content_details = item.get("contentDetails", {})
            video_id = item.get("id")
            records.append(
                {
                    "id": video_id,
                    "title": snippet.get("title", ""),
                    "description": snippet.get("description", ""),
                    "upload_date": snippet.get("publishedAt", ""),
                    "duration": content_details.get("duration", ""),
                    "url": (
                        f"https://www.youtube.com/watch?v={video_id}"
                        if video_id
                        else ""
                    ),
                    "tags": snippet.get("tags", []),
                }
            )
        time.sleep(sleep)
    return records


def fetch_video_statistics(
    video_ids: list[str],
    api_key: str,
    *,
    sleep: float = 0.2,
) -> list[dict[str, Any]]:
    """Fetch current public statistics for all returned video resources."""
    records: list[dict[str, Any]] = []
    url = f"{YOUTUBE_API_BASE}/videos"

    for start in range(0, len(video_ids), 50):
        batch = video_ids[start : start + 50]
        if not batch:
            continue
        data = _get_json(
            url,
            {
                "part": "statistics",
                "id": ",".join(batch),
                "key": api_key,
            },
            label="fetch video statistics",
        )
        for item in data.get("items", []):
            statistics = item.get("statistics", {})
            records.append(
                {
                    "video_id": item.get("id"),
                    "view_count": statistics.get("viewCount"),
                    "like_count": statistics.get("likeCount"),
                    "comment_count": statistics.get("commentCount"),
                }
            )
        time.sleep(sleep)
    return records
