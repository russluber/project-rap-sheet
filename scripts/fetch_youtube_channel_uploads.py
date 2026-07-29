#!/usr/bin/env python
"""Incrementally collect descriptive metadata for a channel's uploads."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from fliptop.io import atomic_output_path
from fliptop.youtube_api import (
    DEFAULT_SECRET_PATH,
    fetch_video_metadata,
    get_all_upload_video_ids,
    get_uploads_playlist_id,
    load_api_key,
)

DEFAULT_OUTPUT = Path("data") / "raw" / "youtube_videos.json"
LEGACY_METRIC_FIELDS = frozenset({"view_count", "likeCount", "commentCount"})


def load_existing_metadata(path: str | Path) -> list[dict[str, Any]]:
    """Load an existing JSON-list store, failing closed on corruption."""
    path = Path(path)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON; refusing to overwrite it") from exc
    if not isinstance(data, list):
        raise ValueError(f"{path}: expected a JSON list of video records")
    return data


def save_metadata(path: str | Path, records: list[dict[str, Any]]) -> None:
    """Atomically save descriptive video metadata as a formatted JSON list."""
    path = Path(path)
    with atomic_output_path(path) as temporary:
        temporary.write_text(
            json.dumps(records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def remove_legacy_metrics(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove mutable statistics formerly embedded in upload metadata."""
    return [
        {
            key: value
            for key, value in record.items()
            if key not in LEGACY_METRIC_FIELDS
        }
        for record in records
    ]


def fetch_channel_uploads(
    channel_id: str,
    output_path: str | Path = DEFAULT_OUTPUT,
    secret_path: str | Path = DEFAULT_SECRET_PATH,
) -> None:
    """Discover channel uploads and append metadata for previously unseen IDs."""
    output_path = Path(output_path)
    api_key = load_api_key(secret_path=secret_path)
    print(f"Using API key from env/file. Output: {output_path}")

    uploads_playlist_id = get_uploads_playlist_id(channel_id, api_key)
    print(f"Uploads playlist ID: {uploads_playlist_id}")
    video_ids = get_all_upload_video_ids(uploads_playlist_id, api_key)
    print(f"Found {len(video_ids)} total videos in uploads playlist.")

    loaded_data = load_existing_metadata(output_path)
    existing_data = remove_legacy_metrics(loaded_data)
    existing_ids = {
        str(record.get("id"))
        for record in existing_data
        if record.get("id")
    }
    print(f"Existing metadata contains {len(existing_ids)} videos.")

    new_records = fetch_video_metadata(
        video_ids,
        api_key,
        existing_ids=existing_ids,
    )
    print(f"Fetched metadata for {len(new_records)} new videos.")
    migrated_legacy_metrics = existing_data != loaded_data
    if not new_records and not migrated_legacy_metrics:
        print("No new videos to add. Done.")
        return

    save_metadata(output_path, [*existing_data, *new_records])
    if migrated_legacy_metrics:
        print("Removed legacy metric fields from the upload metadata store.")
    print(f"Saved {len(existing_data) + len(new_records)} records to {output_path}.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch new uploads from a YouTube channel into metadata JSON."
    )
    parser.add_argument("--channel", required=True, help="YouTube channel ID.")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output JSON path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--secret",
        default=str(DEFAULT_SECRET_PATH),
        help=f"Path to secret JSON with YT_API_KEY (default: {DEFAULT_SECRET_PATH}).",
    )
    args = parser.parse_args()

    try:
        fetch_channel_uploads(
            channel_id=args.channel,
            output_path=args.output,
            secret_path=args.secret,
        )
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
