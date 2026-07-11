"""
fliptop.publish

Final-output publishing helpers for ``ft_battles``.

This module owns the stable final analysis table: joining validated annotation
results onto rich battle metadata, enforcing the public column set, and writing
the table to disk.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .io import atomic_output_path

PathLike = str | Path

# The columns build_ft_battles() emits, in order. This is the final wrangling
# artifact: battle metadata plus the core result fields, with only the columns
# needed for downstream analysis.
FINAL_COLUMNS = [
    "id",
    "title",
    "upload_date",
    "duration_seconds",
    "emcee1",
    "emcee2",
    "matchup",
    "event_name",
    "event_date",
    "event_location",
    "url",
    "battle_type",
    "winner",
    "votes_winner",
    "votes_loser",
]

# Columns that may appear in rich metadata or debug/audit surfaces, but should
# never be published in ft_battles.json. The final table is intended to stand as
# a clean analysis output, not a provenance dump.
FINAL_OUTPUT_FORBIDDEN_COLUMNS = [
    "description",
    "duration_hms",
    "event_date_source",
    "yt_raw_title",
    "event_description",
    "video_id",
    "matchup_clean",
    "event_location_clean",
    "source_part_number",
    "pipeline_status",
    "stage",
    "excluded_reason",
    "exit_category",
    "matched_keyword",
    "rule_id",
    "rule_note",
    "upload_decision",
    "upload_decision_reason",
    "upload_decision_note",
    "manual_note",
    "annotation_status",
    "helper_emcee",
    "emcee1_status",
    "emcee2_status",
    "helper_status",
]


def build_ft_battles_from_metadata(
    battle_metadata: pd.DataFrame,
    results: pd.DataFrame | None = None,
    *,
    require_results: bool = True,
) -> pd.DataFrame:
    """
    Publish the final result-enriched ``ft_battles`` table from metadata.

    This is the final data-wrangling artifact. It joins the id-keyed battle
    results onto the metadata table, converts multi-part ``id`` values to their
    scalar battle key, and selects the stable analysis columns in
    :data:`FINAL_COLUMNS`.
    """
    from .annotations import battle_key, load_results, merge_results, validate_results_store

    if results is None:
        results = load_results()

    problems = validate_results_store(
        results,
        battle_metadata,
        require_complete=require_results,
    )
    if require_results and problems:
        raise ValueError(
            "battle results failed validation; refusing to build ft_battles:\n"
            + "\n".join(f"  - {p}" for p in problems)
        )

    merged = merge_results(battle_metadata, results)
    merged["id"] = merged["id"].map(battle_key)

    existing_cols = [c for c in FINAL_COLUMNS if c in merged.columns]
    return merged[existing_cols]


def build_ft_battles(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map=None,
    manual_matchups=None,
    upload_decisions=None,
    vt_event_dates=None,
    results: pd.DataFrame | None = None,
    require_results: bool = True,
) -> pd.DataFrame:
    """
    Build the final result-enriched ``ft_battles`` table from raw files.

    The output keeps only the project-level analysis columns and joins
    ``battle_type``, ``winner``, ``votes_winner``, and ``votes_loser`` from the
    annotations store. Use ``build_battle_metadata`` when you need the rich
    intermediate metadata with description/provenance columns.
    """
    from .battles import build_battle_metadata

    battle_metadata = build_battle_metadata(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        versetracker_csv_name=versetracker_csv_name,
        rename_map=rename_map,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
        vt_event_dates=vt_event_dates,
    )
    return build_ft_battles_from_metadata(
        battle_metadata,
        results=results,
        require_results=require_results,
    )


def write_ft_battles(
    out_path: PathLike,
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    rename_map=None,
    manual_matchups=None,
    upload_decisions=None,
    fmt: str = "json",
) -> Path:
    """Build the final ``ft_battles`` table and save it to disk."""
    ft_battles = build_ft_battles(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        rename_map=rename_map,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    return save_ft_battles(ft_battles, out_path, fmt=fmt)


def save_ft_battles(
    ft_battles: pd.DataFrame,
    out_path: PathLike,
    fmt: str = "json",
) -> Path:
    """Serialize an already-built ``ft_battles`` table to disk."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = fmt.lower()
    if fmt == "csv":
        with atomic_output_path(out_path) as temporary:
            ft_battles.to_csv(temporary, index=False)
    elif fmt == "json":
        with atomic_output_path(out_path) as temporary:
            ft_battles.to_json(
                temporary,
                orient="records",
                lines=True,
                date_format="epoch",
                date_unit="ms",
                force_ascii=False,
            )
    else:
        raise ValueError(f"Unsupported fmt {fmt!r}; use 'csv' or 'json'.")

    return out_path
