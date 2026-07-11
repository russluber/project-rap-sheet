"""Explicit, immutable snapshot of every file-backed pipeline input."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from . import DATA_DIR
from .annotations import load_results
from .battles import load_event_metadata, load_youtube_uploads
from .events import load_versetracker_event_dates
from .overrides import (
    load_event_date_overrides,
    load_event_location_overrides,
    load_event_location_patterns,
    load_location_aliases,
    load_manual_matchups,
    load_upload_decisions,
)
from .rename_map import load_rename_map
from .rules import ExclusionRule, load_event_exclusion_rules, load_title_exclusion_rules

PathLike = str | Path
ManualMatchupMap = Mapping[str, Mapping[str, str | None]]
UploadDecisionMap = Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class PipelineInputs:
    """All raw tables, maintained decisions, and annotations used by one run."""

    raw_dir: Path
    data_dir: Path
    raw_uploads: pd.DataFrame
    raw_events: pd.DataFrame
    rename_map: Mapping[str, str]
    manual_matchups: ManualMatchupMap
    upload_decisions: UploadDecisionMap
    vt_event_dates: Mapping[str, pd.Timestamp]
    title_exclusion_rules: tuple[ExclusionRule, ...]
    event_exclusion_rules: tuple[ExclusionRule, ...]
    event_location_overrides: Mapping[str, str]
    event_location_patterns: tuple[tuple[str, str], ...]
    location_aliases: Mapping[str, str]
    event_date_overrides: Mapping[str, str]
    results: pd.DataFrame
    source_files: tuple[Path, ...]


def load_pipeline_inputs(
    raw_dir: PathLike,
    *,
    data_dir: PathLike = DATA_DIR,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: Mapping[str, str] | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
    results: pd.DataFrame | None = None,
) -> PipelineInputs:
    """Load every file-backed dependency once and return one run snapshot."""
    raw_dir = Path(raw_dir)
    data_dir = Path(data_dir)
    overrides_dir = data_dir / "overrides"
    rules_dir = data_dir / "rules"
    annotations_dir = data_dir / "annotations"

    youtube_path = raw_dir / youtube_json_name
    events_path = raw_dir / events_csv_name
    versetracker_path = raw_dir / versetracker_csv_name
    aliases_path = data_dir / "emcee_aliases.csv"
    manual_path = overrides_dir / "manual_matchups.csv"
    decisions_path = overrides_dir / "upload_decisions.csv"
    event_locations_path = overrides_dir / "event_locations.csv"
    event_location_patterns_path = overrides_dir / "event_location_patterns.csv"
    location_aliases_path = overrides_dir / "location_aliases.csv"
    event_dates_path = overrides_dir / "event_dates.csv"
    title_rules_path = rules_dir / "title_exclusions.csv"
    event_rules_path = rules_dir / "event_exclusions.csv"
    results_path = annotations_dir / "battle_results.csv"

    return PipelineInputs(
        raw_dir=raw_dir,
        data_dir=data_dir,
        raw_uploads=load_youtube_uploads(youtube_path),
        raw_events=load_event_metadata(events_path),
        rename_map=rename_map if rename_map is not None else load_rename_map(aliases_path),
        manual_matchups=(
            manual_matchups
            if manual_matchups is not None
            else load_manual_matchups(manual_path)
        ),
        upload_decisions=(
            upload_decisions
            if upload_decisions is not None
            else load_upload_decisions(decisions_path)
        ),
        vt_event_dates=(
            vt_event_dates
            if vt_event_dates is not None
            else load_versetracker_event_dates(versetracker_path)
        ),
        title_exclusion_rules=tuple(load_title_exclusion_rules(title_rules_path)),
        event_exclusion_rules=tuple(load_event_exclusion_rules(event_rules_path)),
        event_location_overrides=load_event_location_overrides(event_locations_path),
        event_location_patterns=tuple(
            load_event_location_patterns(event_location_patterns_path)
        ),
        location_aliases=load_location_aliases(location_aliases_path),
        event_date_overrides=load_event_date_overrides(event_dates_path),
        results=results if results is not None else load_results(results_path),
        source_files=tuple(
            path
            for path in (
                youtube_path,
                events_path,
                versetracker_path if vt_event_dates is None else None,
                aliases_path if rename_map is None else None,
                manual_path if manual_matchups is None else None,
                decisions_path if upload_decisions is None else None,
                event_locations_path,
                event_location_patterns_path,
                location_aliases_path,
                event_dates_path,
                title_rules_path,
                event_rules_path,
                results_path if results is None else None,
            )
            if path is not None and path.exists()
        ),
    )
