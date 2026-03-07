#!/usr/bin/env python
"""
fetch_youtube_channel_uploads.py

Fetch metadata for all uploads from a YouTube channel and save them to a JSON file.

Features:
- Uses the channel's "uploads" playlist to list all videos.
- Fetches metadata in batches via the YouTube Data API v3.
- Supports incremental updates by skipping videos already present in the output JSON.
- Designed as the "Extract" step for the FlipTop Analysis project.

Usage (from repo root, in VS Code terminal for example):

    python scripts/fetch_youtube_channel_uploads.py \
        --channel UCBdHwFIE4AJWSa3Wxdu7bAQ \
        --output data/raw/youtube_videos.json
"""

import os
import json
import time
import argparse
from typing import List, Dict, Set, Any, Optional

import requests

# Default paths / constants
DEFAULT_OUTPUT = os.path.join("data", "raw", "youtube_videos.json")
DEFAULT_SECRET_PATH = os.path.join("data", "secret", "secret.json")
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


# ---------------------------------------------------------------------------
# API key loading
# ---------------------------------------------------------------------------

def load_api_key(secret_path: str = DEFAULT_SECRET_PATH) -> str:
    """
    Load the YouTube API key.

    Priority:
    1. Environment variable YOUTUBE_API_KEY
    2. JSON file at secret_path with key "YT_API_KEY"

    Raises:
        RuntimeError if no API key is found.
    """
    env_key = os.getenv("YOUTUBE_API_KEY")
    if env_key:
        return env_key

    if os.path.exists(secret_path):
        with open(secret_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        key = data.get("YT_API_KEY")
        if key:
            return key

    raise RuntimeError(
        "YouTube API key not found. Set YOUTUBE_API_KEY env var or "
        f"create {secret_path} with a 'YT_API_KEY' field."
    )


# ---------------------------------------------------------------------------
# YouTube helpers
# ---------------------------------------------------------------------------

def get_uploads_playlist_id(channel_id: str, api_key: str) -> str:
    """
    Get the 'uploads' playlist ID for a given channel.

    The uploads playlist contains every uploaded video for that channel.
    """
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": api_key,
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch uploads playlist: {e}")

    data = resp.json()
    try:
        return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except (KeyError, IndexError):
        raise ValueError(
            f"Could not retrieve uploads playlist ID for channel {channel_id}."
        )


def get_all_upload_video_ids(uploads_playlist_id: str, api_key: str) -> List[str]:
    """
    Retrieve all video IDs from the given uploads playlist.

    Uses paging with maxResults=50 until there is no nextPageToken.
    """
    video_ids: List[str] = []
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    params = {
        "part": "contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": 50,
        "key": api_key,
    }

    while True:
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch video IDs: {e}")

        data = resp.json()
        for item in data.get("items", []):
            vid = item.get("contentDetails", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        next_token = data.get("nextPageToken")
        if not next_token:
            break

        params["pageToken"] = next_token
        time.sleep(0.2)  # gentle pacing

    return video_ids


def fetch_video_metadata(
    video_ids: List[str],
    api_key: str,
    existing_ids: Optional[Set[str]] = None,
    sleep: float = 0.2,
) -> List[Dict[str, Any]]:
    """
    Fetch detailed metadata for each video ID.

    Skips IDs that are already in `existing_ids` (for incremental updates).

    Returns a list of dicts with keys:
      - id, title, description, upload_date, view_count, duration,
        url, likeCount, commentCount, tags
    """
    if existing_ids is None:
        existing_ids = set()

    out: List[Dict[str, Any]] = []
    url = f"{YOUTUBE_API_BASE}/videos"

    # YouTube API allows up to 50 IDs per call
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        # Only fetch new ones
        batch = [vid for vid in batch if vid not in existing_ids]
        if not batch:
            continue

        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(batch),
            "key": api_key,
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch video metadata: {e}")

        data = resp.json()
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            content_details = item.get("contentDetails", {})

            vid = item.get("id")
            # Build a compact metadata record that we can treat as staging later
            record = {
                "id": vid,
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "upload_date": snippet.get("publishedAt", ""),
                "view_count": statistics.get("viewCount"),
                "duration": content_details.get("duration", ""),
                "url": f"https://www.youtube.com/watch?v={vid}" if vid else "",
                "likeCount": statistics.get("likeCount"),
                "commentCount": statistics.get("commentCount"),
                "tags": snippet.get("tags", []),
            }
            out.append(record)

        time.sleep(sleep)

    return out


# ---------------------------------------------------------------------------
# JSON I/O helpers
# ---------------------------------------------------------------------------

def load_existing_metadata(path: str) -> List[Dict[str, Any]]:
    """
    Load existing video metadata from JSON file, if it exists.

    Returns an empty list if the file does not exist or is invalid.
    """
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except (json.JSONDecodeError, OSError):
        return []


def save_metadata(path: str, records: List[Dict[str, Any]]) -> None:
    """
    Save the list of metadata records to JSON with UTF-8 and nice indentation.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Main high-level function
# ---------------------------------------------------------------------------

def fetch_channel_uploads(
    channel_id: str,
    output_path: str = DEFAULT_OUTPUT,
    secret_path: str = DEFAULT_SECRET_PATH,
) -> None:
    """
    High-level function to:
    - Load API key
    - Resolve channel's uploads playlist
    - Fetch all video IDs
    - Load existing metadata JSON (if any)
    - Fetch metadata for new videos only
    - Save combined metadata to output_path
    """
    api_key = load_api_key(secret_path=secret_path)
    print(f"Using API key from env/file. Output: {output_path}")

    uploads_playlist_id = get_uploads_playlist_id(channel_id, api_key)
    print(f"Uploads playlist ID: {uploads_playlist_id}")

    video_ids = get_all_upload_video_ids(uploads_playlist_id, api_key)
    print(f"Found {len(video_ids)} total videos in uploads playlist.")

    existing_data = load_existing_metadata(output_path)
    existing_ids: Set[str] = {v.get("id") for v in existing_data if v.get("id")}
    print(f"Existing metadata contains {len(existing_ids)} videos.")

    new_records = fetch_video_metadata(video_ids, api_key, existing_ids=existing_ids)
    print(f"Fetched metadata for {len(new_records)} new videos.")

    if not new_records:
        print("No new videos to add. Done.")
        return

    all_records = existing_data + new_records
    save_metadata(output_path, all_records)
    print(f"Saved {len(all_records)} total video records to {output_path}.")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch all uploads from a YouTube channel and store metadata as JSON."
    )
    parser.add_argument(
        "--channel",
        required=True,
        help="YouTube channel ID (e.g. 'UCBdHwFIE4AJWSa3Wxdu7bAQ').",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--secret",
        default=DEFAULT_SECRET_PATH,
        help=f"Path to secret JSON with YT_API_KEY (default: {DEFAULT_SECRET_PATH}).",
    )

    args = parser.parse_args()

    try:
        fetch_channel_uploads(
            channel_id=args.channel,
            output_path=args.output,
            secret_path=args.secret,
        )
    except Exception as e:
        print(f"[error] {e}")


if __name__ == "__main__":
    main()