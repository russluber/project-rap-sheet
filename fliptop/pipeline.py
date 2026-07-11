"""Single-execution orchestration for the raw-to-metadata pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .battles import finalize_battles, load_event_metadata, load_youtube_uploads
from .events import load_versetracker_event_dates
from .overrides import load_manual_matchups, load_upload_decisions
from .rename_map import load_rename_map
from .uploads import (
    ManualMatchupMap,
    PathLike,
    RenameMap,
    UploadDecisionMap,
)


@dataclass
class PipelineRun:
    """All reusable products from one execution of the metadata pipeline."""

    raw_dir: Path
    rename_map: Mapping[str, str]
    manual_matchups: ManualMatchupMap
    upload_decisions: UploadDecisionMap
    vt_event_dates: Mapping[str, pd.Timestamp]
    raw_uploads: pd.DataFrame
    raw_events: pd.DataFrame
    stages: dict[str, pd.DataFrame]
    excluded_uploads: pd.DataFrame
    review_uploads: pd.DataFrame
    battle_metadata: pd.DataFrame


def build_pipeline_run(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: dict[str, pd.Timestamp] | None = None,
) -> PipelineRun:
    """Execute filtering, metadata attachment, and finalization exactly once."""
    # Imported lazily while the old compatibility helpers still live in
    # lineage.py. A later migration step moves the trace implementation here.
    from .lineage import _upload_stage_trace

    raw_dir = Path(raw_dir)
    if rename_map is None:
        rename_map = load_rename_map()
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()
    if vt_event_dates is None:
        vt_event_dates = load_versetracker_event_dates(raw_dir / versetracker_csv_name)

    raw_uploads = load_youtube_uploads(raw_dir / youtube_json_name)
    raw_events = load_event_metadata(raw_dir / events_csv_name)
    stages, excluded, review = _upload_stage_trace(
        raw_uploads,
        raw_events,
        rename_map=rename_map,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    filtered = stages["drop_excluded_events"]
    battle_metadata = finalize_battles(filtered, vt_event_dates=vt_event_dates)
    stages = dict(stages)
    stages["finalize_battle_metadata"] = battle_metadata

    return PipelineRun(
        raw_dir=raw_dir,
        rename_map=rename_map,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
        vt_event_dates=vt_event_dates,
        raw_uploads=raw_uploads,
        raw_events=raw_events,
        stages=stages,
        excluded_uploads=excluded,
        review_uploads=review,
        battle_metadata=battle_metadata,
    )
