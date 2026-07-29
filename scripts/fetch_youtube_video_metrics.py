#!/usr/bin/env python
"""Refresh the current one-row-per-video YouTube metrics store."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from fliptop.youtube_api import (
    DEFAULT_SECRET_PATH,
    fetch_video_statistics,
    load_api_key,
)
from fliptop.youtube_metrics import (
    DEFAULT_METRICS_PATH,
    empty_youtube_video_metrics,
    load_youtube_video_metrics,
    merge_youtube_metric_refresh,
    save_youtube_video_metrics,
)

DEFAULT_IDS_PATH = Path("data") / "raw" / "youtube_videos.json"


def load_video_ids(path: str | Path) -> list[str]:
    """Load unique, nonblank IDs from the raw YouTube upload inventory."""
    path = Path(path)
    try:
        records = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON") from exc
    if not isinstance(records, list):
        raise ValueError(f"{path}: expected a JSON list of video records")

    video_ids = [
        str(record.get("id")).strip()
        for record in records
        if isinstance(record, dict) and record.get("id")
    ]
    if not video_ids:
        raise ValueError(f"{path}: contains no usable video IDs")
    if len(video_ids) != len(set(video_ids)):
        raise ValueError(f"{path}: contains duplicate video IDs")
    return video_ids


def refresh_youtube_video_metrics(
    *,
    ids_path: str | Path = DEFAULT_IDS_PATH,
    output_path: str | Path = DEFAULT_METRICS_PATH,
    secret_path: str | Path = DEFAULT_SECRET_PATH,
    checked_at=None,
) -> Path:
    """Fetch all known video statistics and atomically replace the CSV."""
    ids_path = Path(ids_path)
    output_path = Path(output_path)
    video_ids = load_video_ids(ids_path)
    existing = (
        load_youtube_video_metrics(output_path)
        if output_path.exists()
        else empty_youtube_video_metrics()
    )
    api_key = load_api_key(secret_path)
    observed = checked_at or datetime.now(UTC)

    fetched = pd.DataFrame(
        fetch_video_statistics(video_ids, api_key),
        columns=["video_id", "view_count", "like_count", "comment_count"],
    )
    refreshed = merge_youtube_metric_refresh(
        existing,
        fetched,
        video_ids,
        checked_at=observed,
    )

    old_views = (
        existing.set_index("video_id")["view_count"]
        if not existing.empty
        else pd.Series(dtype="Int64")
    )
    comparable = refreshed.loc[
        refreshed["video_id"].isin(old_views.index)
        & refreshed["fetch_status"].eq("ok")
    ].copy()
    comparable["_old_view_count"] = comparable["video_id"].map(old_views)
    decreases = int(
        (
            comparable["_old_view_count"].notna()
            & comparable["view_count"].lt(comparable["_old_view_count"])
        ).sum()
    )

    path = save_youtube_video_metrics(refreshed, output_path)
    ok_count = int(refreshed["fetch_status"].eq("ok").sum())
    missing_count = int(refreshed["fetch_status"].eq("not_returned").sum())
    print(
        f"Saved {len(refreshed)} video metrics to {path} "
        f"(ok={ok_count}, not_returned={missing_count}, view_decreases={decreases})."
    )
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh current YouTube views, likes, and comments."
    )
    parser.add_argument(
        "--ids-from",
        default=str(DEFAULT_IDS_PATH),
        help=f"Upload inventory JSON (default: {DEFAULT_IDS_PATH}).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_METRICS_PATH),
        help=f"Current metrics CSV (default: {DEFAULT_METRICS_PATH}).",
    )
    parser.add_argument(
        "--secret",
        default=str(DEFAULT_SECRET_PATH),
        help=f"Path to secret JSON with YT_API_KEY (default: {DEFAULT_SECRET_PATH}).",
    )
    args = parser.parse_args()

    try:
        refresh_youtube_video_metrics(
            ids_path=args.ids_from,
            output_path=args.output,
            secret_path=args.secret,
        )
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
