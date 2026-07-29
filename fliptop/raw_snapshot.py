"""Staging, validation, and rollback-safe publication for raw data snapshots."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .battles import load_event_metadata, load_youtube_uploads
from .contracts import YOUTUBE_VIDEO_METRICS, ContractViolation
from .events import load_versetracker_event_dates
from .youtube_metrics import load_youtube_video_metrics

RAW_SNAPSHOT_FILENAMES = (
    "youtube_videos.json",
    "youtube_video_metrics.csv",
    "matchup_events_metadata.csv",
    "versetracker_event_dates.csv",
)
REQUIRED_FETCH_FILENAMES = RAW_SNAPSHOT_FILENAMES[:3]


@contextmanager
def staged_raw_snapshot(raw_dir: Path) -> Iterator[Path]:
    """Yield a temporary sibling copy of the current raw snapshot."""
    raw_dir = Path(raw_dir)
    raw_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        dir=raw_dir.parent,
        prefix=".raw-candidate-",
    ) as staging_name:
        staging_dir = Path(staging_name)
        for filename in RAW_SNAPSHOT_FILENAMES:
            source = raw_dir / filename
            if source.exists():
                shutil.copy2(source, staging_dir / filename)
        yield staging_dir


def validate_raw_snapshot(raw_dir: Path) -> None:
    """Load all present raw tables through their Stage 4 contracts."""
    raw_dir = Path(raw_dir)
    for filename in REQUIRED_FETCH_FILENAMES:
        if not (raw_dir / filename).exists():
            raise FileNotFoundError(f"raw snapshot is missing required file: {filename}")

    uploads = load_youtube_uploads(raw_dir / "youtube_videos.json")
    metrics = load_youtube_video_metrics(raw_dir / "youtube_video_metrics.csv")
    load_event_metadata(raw_dir / "matchup_events_metadata.csv")
    upload_ids = set(uploads["id"].astype(str))
    metric_ids = set(metrics["video_id"].astype(str))
    coverage_problems = []
    missing = sorted(upload_ids - metric_ids)
    unexpected = sorted(metric_ids - upload_ids)
    if missing:
        coverage_problems.append(
            f"missing metrics for {len(missing)} upload ID(s): "
            + ", ".join(missing[:5])
        )
    if unexpected:
        coverage_problems.append(
            f"contains {len(unexpected)} unknown upload ID(s): "
            + ", ".join(unexpected[:5])
        )
    if coverage_problems:
        raise ContractViolation(
            YOUTUBE_VIDEO_METRICS.name,
            coverage_problems,
            source=raw_dir / "youtube_video_metrics.csv",
        )
    versetracker = raw_dir / "versetracker_event_dates.csv"
    if versetracker.exists():
        load_versetracker_event_dates(versetracker)


def _replace_for_raw_publish(source: Path, destination: Path) -> None:
    """Replace one raw file; separate for failure-injection tests."""
    os.replace(source, destination)


def publish_raw_snapshot(staging_dir: Path, raw_dir: Path) -> tuple[Path, ...]:
    """Promote a validated raw snapshot and restore every old file on failure."""
    staging_dir = Path(staging_dir)
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    staged_paths = tuple(
        staging_dir / filename
        for filename in RAW_SNAPSHOT_FILENAMES
        if (staging_dir / filename).exists()
    )
    staged_names = {path.name for path in staged_paths}
    missing = set(REQUIRED_FETCH_FILENAMES) - staged_names
    if missing:
        raise FileNotFoundError(
            "raw snapshot is missing required file(s): " + ", ".join(sorted(missing))
        )

    destinations = tuple(raw_dir / path.name for path in staged_paths)
    with tempfile.TemporaryDirectory(
        dir=raw_dir.parent,
        prefix=".raw-backup-",
    ) as backup_name:
        backup_dir = Path(backup_name)
        backups: dict[Path, Path] = {}
        published: list[Path] = []
        try:
            for destination in destinations:
                if destination.exists():
                    backup = backup_dir / destination.name
                    _replace_for_raw_publish(destination, backup)
                    backups[destination] = backup

            for staged, destination in zip(staged_paths, destinations, strict=True):
                _replace_for_raw_publish(staged, destination)
                published.append(destination)
        except BaseException:
            for destination in reversed(published):
                destination.unlink(missing_ok=True)
            for destination, backup in backups.items():
                if backup.exists():
                    _replace_for_raw_publish(backup, destination)
            raise

    return destinations
