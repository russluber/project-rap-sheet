"""
fliptop.battles

Reproducible pipeline to go from raw FlipTop data to a clean
one-row-per-battle metadata table, then publish the result-enriched
``ft_battles`` table used for analysis.

Typical notebook usage:

    from fliptop import RAW_DATA_DIR, PROCESSED_DATA_DIR
    from fliptop import build_ft_battles, build_battle_metadata
    from fliptop.publish import write_ft_battles

    battle_metadata = build_battle_metadata(raw_dir=RAW_DATA_DIR)
    ft_battles = build_ft_battles(raw_dir=RAW_DATA_DIR)

    write_ft_battles(
        out_path=PROCESSED_DATA_DIR / "ft_battles.json",
        raw_dir=RAW_DATA_DIR,
        fmt="json",
    )

The metadata pipeline has three main stages:

1. From raw YouTube uploads to clean 1v1 battle uploads.
2. Attach event metadata and remove excluded event categories.
3. Consolidate multi part uploads into one row per battle.

The final ``ft_battles`` output then joins ``data/annotations/battle_results.csv``
onto that metadata, keeps the project-level analysis columns, and scalarizes
multi-part ``id`` values to their battle key.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from . import events as _events
from . import publish as _publish
from . import uploads as _uploads
from .contracts import BATTLE_METADATA_COLUMNS, RAW_EVENT_METADATA, RAW_YOUTUBE_UPLOADS

# ---------------------------------------------------------------------------
# I. Types and simple aliases
# ---------------------------------------------------------------------------

PathLike = str | Path
RenameMap = Mapping[str, str]
ManualMatchupMap = Mapping[str, Mapping[str, str | None]]
UploadDecisionMap = Mapping[str, Mapping[str, str]]


# ---------------------------------------------------------------------------
# II. File loading helpers
# ---------------------------------------------------------------------------

def load_youtube_uploads(path: PathLike) -> pd.DataFrame:
    """
    Load raw YouTube uploads data from a JSON file.

    Parameters
    ----------
    path:
        Path to youtube_videos.json as produced by fetch_youtube_channel_uploads.py.

    Returns
    -------
    pd.DataFrame
        Raw uploads table, one row per video.
    """
    path = Path(path)
    # This mirrors what you did in the notebook: pd.read_json on the exported file.
    df = pd.read_json(path)
    return RAW_YOUTUBE_UPLOADS.require(df, source=path)


def load_event_metadata(path: PathLike) -> pd.DataFrame:
    """
    Load raw event and matchup metadata scraped from the FlipTop site.

    Parameters
    ----------
    path:
        Path to matchup_events_metadata.csv as produced
        by fetch_events_metadata_from_fliptop_web.py.

    Returns
    -------
    pd.DataFrame
        Raw event metadata, likely one row per video id.
    """
    path = Path(path)
    # This mirrors your notebook: pd.read_csv on the scraped CSV.
    df = pd.read_csv(path)
    return RAW_EVENT_METADATA.require(df, source=path)


# ---------------------------------------------------------------------------
# III. Upload transforms
# ---------------------------------------------------------------------------

# Re-export upload helpers so older imports from ``fliptop.battles`` keep working.
EXCLUDE_KEYWORDS = _uploads.EXCLUDE_KEYWORDS
EXCLUDE_RE = _uploads.EXCLUDE_RE
TITLE_EXCLUSION_RULES = _uploads.TITLE_EXCLUSION_RULES
_PT_SUFFIX = _uploads._PT_SUFFIX
_base_raw_title = _uploads._base_raw_title
_keep_upload_decision_includes = _uploads._keep_upload_decision_includes
_part_num = _uploads._part_num
add_duration_columns = _uploads.add_duration_columns
add_matchup_and_split = _uploads.add_matchup_and_split
add_matchup_clean = _uploads.add_matchup_clean
apply_emcee_rename = _uploads.apply_emcee_rename
apply_manual_matchup_overrides = _uploads.apply_manual_matchup_overrides
clean_titles = _uploads.clean_titles
convert_video_metrics_to_numeric = _uploads.convert_video_metrics_to_numeric
copy_yt_title = _uploads.copy_yt_title
drop_non_battles = _uploads.drop_non_battles
extract_matchup_from_title = _uploads.extract_matchup_from_title
filter_titles_with_vs = _uploads.filter_titles_with_vs
keep_1v1 = _uploads.keep_1v1
keep_1v1_or_manual_matchup = _uploads.keep_1v1_or_manual_matchup
make_df_1v1_uploads = _uploads.make_df_1v1_uploads
parse_upload_date = _uploads.parse_upload_date
prepare_uploads = _uploads.prepare_uploads
strip_pt_suffix_from_title = _uploads.strip_pt_suffix_from_title

# ---------------------------------------------------------------------------
# IV. Event transforms
# ---------------------------------------------------------------------------

# Re-export event helpers so older imports from ``fliptop.battles`` keep working.
EVENT_EXCLUSION_RULES = _events.EVENT_EXCLUSION_RULES
EXCLUDE_EVENT_KEYWORDS = _events.EXCLUDE_EVENT_KEYWORDS
EVENT_EXCLUSION_RE = _events.EVENT_EXCLUSION_RE
EXCLUDE_EVENT_RE = _events.EXCLUDE_EVENT_RE
_parse_event_date_range = _events._parse_event_date_range
_split_event_day = _events._split_event_day
apply_manual_event_date_overrides = _events.apply_manual_event_date_overrides
apply_manual_event_location_overrides = _events.apply_manual_event_location_overrides
attach_event_metadata = _events.attach_event_metadata
clean_event_location = _events.clean_event_location
drop_excluded_events = _events.drop_excluded_events
extract_event_name_from_description = _events.extract_event_name_from_description
fill_metadata_from_yt_description = _events.fill_metadata_from_yt_description
impute_event_dates_from_versetracker = _events.impute_event_dates_from_versetracker
load_versetracker_event_dates = _events.load_versetracker_event_dates
normalize_event_day = _events.normalize_event_day
parse_event_date = _events.parse_event_date
split_event_description = _events.split_event_description


# ---------------------------------------------------------------------------
# V. Audit compatibility wrappers
# ---------------------------------------------------------------------------


def build_excluded_uploads(*args, **kwargs) -> pd.DataFrame:
    """Compatibility wrapper for :func:`fliptop.lineage.build_excluded_uploads`."""
    from .lineage import build_excluded_uploads as _impl

    return _impl(*args, **kwargs)


def build_upload_lineage(*args, **kwargs) -> pd.DataFrame:
    """Compatibility wrapper for :func:`fliptop.lineage.build_upload_lineage`."""
    from .lineage import build_upload_lineage as _impl

    return _impl(*args, **kwargs)


def build_manual_matchup_review_uploads(*args, **kwargs) -> pd.DataFrame:
    """Compatibility wrapper for lineage manual-matchup review uploads."""
    from .lineage import build_manual_matchup_review_uploads as _impl

    return _impl(*args, **kwargs)


def build_pipeline_stage_summary(*args, **kwargs) -> pd.DataFrame:
    """Compatibility wrapper for :func:`fliptop.lineage.build_pipeline_stage_summary`."""
    from .lineage import build_pipeline_stage_summary as _impl

    return _impl(*args, **kwargs)


def build_pipeline_stage_drops(*args, **kwargs) -> pd.DataFrame:
    """Compatibility wrapper for :func:`fliptop.lineage.build_pipeline_stage_drops`."""
    from .lineage import build_pipeline_stage_drops as _impl

    return _impl(*args, **kwargs)


def write_audit_outputs(*args, **kwargs):
    """Compatibility wrapper for :func:`fliptop.lineage.write_audit_outputs`."""
    from .lineage import write_audit_outputs as _impl

    return _impl(*args, **kwargs)


def consolidate_battle_parts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse multi-part YouTube battles (pt. 1, pt. 2, ...) into a single row.

    Uses `yt_raw_title` to detect parts, e.g.:
      - 'FlipTop - Dello vs Batas pt. 1'
      - 'FlipTop - Dello vs Batas pt. 2'

    For each base battle:
      - `id` and `url` become lists [part1, part2, ...]
      - `upload_date` becomes the earliest upload
      - `duration_seconds` is summed across parts
      - `duration_hms` is recomputed from `duration_seconds` for ALL rows
      - Metadata columns (description, title, emcee1, emcee2, matchup,
        event_name, event_date, event_location) are taken from the first part
        (they should match).
    """
    if "yt_raw_title" not in df.columns:
        # Nothing to do; we rely on yt_raw_title to see the 'pt. N' suffix.
        return df

    work = df.copy()

    # Make sure duration_seconds is numeric if present
    if "duration_seconds" in work.columns:
        work["duration_seconds"] = pd.to_numeric(
            work["duration_seconds"], errors="coerce"
        )

    # Identify rows that are clearly "pt. N"
    titles = work["yt_raw_title"].fillna("")
    is_part = titles.str.contains(_PT_SUFFIX, na=False)
    parts = work.loc[is_part].copy()

    if parts.empty:
        # No parts to consolidate; just recompute duration_hms (if desired) and return
        final_df = work
        if "duration_seconds" in final_df.columns:
            final_df["duration_hms"] = (
                pd.to_datetime(final_df["duration_seconds"], unit="s")
                .dt.strftime("%H:%M:%S")
            )
        return final_df

    # Derive base key and part number for ordering
    parts["base_raw_title"] = parts["yt_raw_title"].map(_base_raw_title)
    parts["part_num"] = parts["yt_raw_title"].map(_part_num)

    # Sort so lists are ordered [pt1, pt2, ...]
    parts = parts.sort_values(
        ["base_raw_title", "part_num", "yt_raw_title"],
        na_position="last",
    )

    # Build an aggregation map tuned to your columns
    agg: dict[str, object] = {}

    def add(col: str, rule):
        if col in parts.columns:
            agg[col] = rule

    # List fields
    add("id", list)
    add("url", list)

    # Date / time
    add("upload_date", "min")
    add("duration_seconds", "sum")

    # Text + metadata (should be identical across parts)
    add("description", "first")
    add("title", "first")          # cleaned title (no 'pt.')
    add("yt_raw_title", "first")   # keep one raw raw title as representative
    add("emcee1", "first")
    add("emcee2", "first")
    add("matchup", "first")
    add("event_name", "first")
    add("event_date", "first")
    add("event_date_source", "first")
    add("event_location", "first")

    # Group by the base title (battle identity)
    grouped = parts.groupby("base_raw_title", as_index=False).agg(agg)

    # Decide what to use as the final title:
    # - Prefer existing 'title' if present
    # - Otherwise fall back to base_raw_title
    if "title" in grouped.columns:
        grouped["title"] = grouped["title"].fillna(grouped["base_raw_title"])
    else:
        grouped["title"] = grouped["base_raw_title"]

    # Drop helper key used for grouping
    grouped = grouped.drop(columns=["base_raw_title"], errors="ignore")

    # All non-part rows pass through unchanged
    remaining = work.loc[~is_part].copy()

    # Stitch back together
    final_df = pd.concat([remaining, grouped], ignore_index=True)

    # Recompute duration_hms for ALL rows that have duration_seconds
    if "duration_seconds" in final_df.columns:
        final_df["duration_hms"] = (
            pd.to_datetime(final_df["duration_seconds"], unit="s")
            .dt.strftime("%H:%M:%S")
        )

    return final_df


# The rich intermediate columns emitted by build_battle_metadata(), in order.
# This table keeps provenance/debug columns that are useful inside the pipeline
# but are not part of the final public analysis table.
METADATA_COLUMNS = list(BATTLE_METADATA_COLUMNS)

FINAL_COLUMNS = _publish.FINAL_COLUMNS
FINAL_OUTPUT_FORBIDDEN_COLUMNS = _publish.FINAL_OUTPUT_FORBIDDEN_COLUMNS


def finalize_battles(
    df_with_meta: pd.DataFrame,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
) -> pd.DataFrame:
    """
    Final tidy up step to produce ft_battles.

    Mirrors the final notebook steps conceptually:

      - drop helper / raw columns not needed downstream
      - rename matchup_clean -> matchup, event_location_clean -> event_location
      - consolidate multi-part uploads
      - sort by upload_date (newest first)
      - drop yt_raw_title helper
      - apply a couple of manual location fixes
      - impute missing (COVID-masked) event_dates from VerseTracker
      - normalize multi-day event names + resolve per-day dates
      - pin event_date for battles whose YouTube description mis-dates them
      - select and order the final columns

    ``vt_event_dates`` is an optional ``{event_name: first-day date}`` map (see
    :func:`load_versetracker_event_dates`); when given, it fills NaT event_dates
    before the day suffix is stripped.
    """
    work = df_with_meta.copy()

    # 0) Ensure the provenance column exists even on edge paths (e.g. an empty
    #    events table) where attach_event_metadata returned before creating it.
    if "event_date_source" not in work.columns and "event_date" in work.columns:
        work["event_date_source"] = pd.Series(pd.NA, index=work.index, dtype="object")
        work.loc[work["event_date"].notna(), "event_date_source"] = "website"

    # 1) Drop raw / helper columns you don't want in ft_battles
    # (these are from your notebook; safe to ignore if not present)
    cols_to_drop = [
        "view_count",
        "likeCount",
        "commentCount",
        "tags",
        "matchup",           # raw matchup; we will keep matchup_clean instead
        "event_description", # if present
        "video_id",          # from events
    ]
    work = work.drop(columns=[c for c in cols_to_drop if c in work.columns],
                     errors="ignore")

    # 2) Rename cleaned columns to their final names
    rename_cols = {}
    if "matchup_clean" in work.columns:
        rename_cols["matchup_clean"] = "matchup"
    if "event_location_clean" in work.columns:
        rename_cols["event_location_clean"] = "event_location"

    work = work.rename(columns=rename_cols)

    # 3) Drop the original ISO duration string; we use duration_seconds / duration_hms
    work = work.drop(columns=["duration"], errors="ignore")

    # 4) Consolidate multi-part battles (pt. 1, pt. 2, ...)
    battles = consolidate_battle_parts(work)

    # 5) Sort by upload_date (newest first) if present
    if "upload_date" in battles.columns:
        battles = battles.sort_values("upload_date", ascending=False).reset_index(drop=True)

    # 6) Drop yt_raw_title helper
    battles = battles.drop(columns=["yt_raw_title"], errors="ignore")

    # 7) Apply manual event_location fixes you had in the notebook
    #    (must precede normalize_event_day, which strips the '(Day N)' suffix
    #    these overrides key on).
    battles = apply_manual_event_location_overrides(battles)

    # 7b) Impute COVID-masked event_dates from VerseTracker. Runs before
    #     normalize_event_day so the '(Day N)' suffix is still available to
    #     offset multi-day events; only fills NaT, never overwrites a date.
    battles = impute_event_dates_from_versetracker(battles, vt_event_dates)

    # 8) Standardize multi-day event names ("Ahon 16 (Day 2)" -> "Ahon 16") and
    #    resolve the per-day event_date from the description's date range.
    battles = normalize_event_day(battles)

    # 9) Pin event_date for battles whose YouTube description mis-dates them
    #    (website-authoritative hand fixes).
    battles = apply_manual_event_date_overrides(battles)

    # 10) Select and order metadata columns (keep only those that exist)
    existing_cols = [c for c in METADATA_COLUMNS if c in battles.columns]
    battles = battles[existing_cols]

    return battles


# ---------------------------------------------------------------------------
# VI. Top level pipeline functions
# These are what you will usually call from notebooks and scripts.
# ---------------------------------------------------------------------------

def build_battle_metadata(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
) -> pd.DataFrame:
    """
    Build the rich one-row-per-battle metadata table from raw files.

    Parameters
    ----------
    raw_dir:
        Directory that contains the raw data files under data/raw.
    youtube_json_name:
        File name of the YouTube uploads JSON.
    events_csv_name:
        File name of the scraped events CSV.
    versetracker_csv_name:
        File name of the VerseTracker event-date reference CSV used to impute
        COVID-masked event_dates.
    rename_map:
        Optional emcee rename map for canonicalization.
    manual_matchups:
        Optional manual matchup overrides for no-show/ambiguous titles. ``None``
        loads ``data/overrides/manual_matchups.csv``; pass ``{}`` to disable.
    upload_decisions:
        Optional exact include/exclude/review decisions. ``None`` loads
        ``data/overrides/upload_decisions.csv``; pass ``{}`` to disable.
    vt_event_dates:
        Optional ``{event_name: first-day date}`` map for date imputation. If
        ``None`` (default) it is loaded from ``raw_dir / versetracker_csv_name``;
        pass an explicit dict (e.g. ``{}``) to override - ``{}`` disables the
        imputation entirely.

    Returns
    -------
    pd.DataFrame
        Metadata table with one row per battle. It includes provenance/debug
        columns such as ``description``, ``duration_hms``, and
        ``event_date_source``; use :func:`build_ft_battles` for the final
        result-enriched analysis table.
    """
    from .pipeline import build_pipeline_run

    run = build_pipeline_run(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        versetracker_csv_name=versetracker_csv_name,
        rename_map=rename_map,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
        vt_event_dates=vt_event_dates,
    )
    return run.battle_metadata


def build_ft_battles_from_metadata(
    *args,
    **kwargs,
) -> pd.DataFrame:
    """Compatibility wrapper for :func:`fliptop.publish.build_ft_battles_from_metadata`."""
    return _publish.build_ft_battles_from_metadata(*args, **kwargs)


def build_ft_battles(
    *args,
    **kwargs,
) -> pd.DataFrame:
    """Compatibility wrapper for :func:`fliptop.publish.build_ft_battles`."""
    return _publish.build_ft_battles(*args, **kwargs)


def write_ft_battles(
    *args,
    **kwargs,
) -> Path:
    """Compatibility wrapper for :func:`fliptop.publish.write_ft_battles`."""
    return _publish.write_ft_battles(*args, **kwargs)


def save_ft_battles(
    *args,
    **kwargs,
) -> Path:
    """Compatibility wrapper for :func:`fliptop.publish.save_ft_battles`."""
    return _publish.save_ft_battles(*args, **kwargs)
