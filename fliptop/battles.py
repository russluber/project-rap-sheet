"""
fliptop.battles

Reproducible pipeline to go from raw FlipTop data to a clean
one-row-per-battle metadata table, then publish the result-enriched
``ft_battles`` table used for analysis.

Typical notebook usage:

    from fliptop import RAW_DATA_DIR, PROCESSED_DATA_DIR
    from fliptop.battles import build_ft_battles, build_battle_metadata, write_ft_battles

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

import re
from collections.abc import Iterable, Mapping
from datetime import timedelta
from pathlib import Path

import isodate
import pandas as pd
from dateutil import parser as dateparse

from .overrides import (
    load_event_date_overrides,
    load_event_location_overrides,
    load_event_location_patterns,
    load_location_aliases,
    load_manual_matchups,
    load_upload_decisions,
)
from .rename_map import load_rename_map
from .rules import (
    compile_exclusion_pattern,
    first_matching_rule,
    load_event_exclusion_rules,
    load_title_exclusion_rules,
)

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
    return df


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
    return df


# ---------------------------------------------------------------------------
# III. Atomic transforms on uploads (per step functions)
# These should be small, focused, and easy to test.
# Each one takes a DataFrame and returns a new DataFrame.
# ---------------------------------------------------------------------------

def clean_titles(df: pd.DataFrame, title_col: str = "title") -> pd.DataFrame:
    """
    Trim whitespace and remove wrapping double quotes in the title column.
    """
    if title_col not in df:
        return df

    return df.assign(
        **{
            title_col: df[title_col]
            .astype("string")
            .str.strip()
            .str.replace(r'^"(.*)"$', r"\1", regex=True)
        }
    )


def parse_upload_date(
    df: pd.DataFrame,
    upload_date_col: str = "upload_date",
    new_col: str = "upload_date",
) -> pd.DataFrame:
    """
    Parse 'upload_date' into timezone-naive datetime64[ns].

    Assumes YouTube API returns UTC timestamps like '2026-02-19T12:40:15Z'.
    We parse as UTC then drop tz info to keep things simple downstream.
    """
    if upload_date_col not in df:
        return df

    return df.assign(
        **{
            new_col: pd.to_datetime(
                df[upload_date_col],
                errors="coerce",
                utc=True,
            ).dt.tz_localize(None)
        }
    )


def add_duration_columns(
    df: pd.DataFrame,
    duration_col: str = "duration",
) -> pd.DataFrame:
    """
    From ISO-8601 'duration' strings create:

    - 'duration_seconds' (numeric, used for aggregation)
    - 'duration_hms' (string 'HH:MM:SS' for display)
    """
    if duration_col not in df:
        return df

    def to_seconds(x):
        if pd.isna(x):
            return pd.NA
        try:
            d = isodate.parse_duration(x)
            if isinstance(d, timedelta):
                return d.total_seconds()
            return d.totimedelta().total_seconds()
        except Exception:
            return pd.NA

    seconds = df[duration_col].map(to_seconds)

    hms = pd.to_datetime(seconds, unit="s", errors="coerce").dt.strftime("%H:%M:%S")
    hms = hms.where(seconds.notna(), other=pd.NA)

    return df.assign(
        duration_seconds=seconds,
        duration_hms=hms,
    )


def convert_video_metrics_to_numeric(
    df: pd.DataFrame,
    cols: Iterable[str] = ("view_count", "likeCount", "commentCount"),
) -> pd.DataFrame:
    """
    Convert view/like/comment count columns from strings to numeric.

    Any missing or non numeric values are coerced to NaN.
    """
    target_cols = list(cols)
    present_cols = [col for col in target_cols if col in df.columns]
    if not present_cols:
        return df

    return df.assign(
        **{col: pd.to_numeric(df[col], errors="coerce") for col in present_cols}
    )


TITLE_EXCLUSION_RULES = load_title_exclusion_rules()
EXCLUDE_KEYWORDS = [rule.pattern for rule in TITLE_EXCLUSION_RULES]
EXCLUDE_RE = compile_exclusion_pattern(TITLE_EXCLUSION_RULES)

# Event names are unavailable during the title filters above. These domain-level
# exclusions run after event metadata is attached so they also catch uploads
# whose YouTube titles do not identify them as tryouts / POI.
EVENT_EXCLUSION_RULES = load_event_exclusion_rules()
EXCLUDE_EVENT_KEYWORDS = [rule.pattern for rule in EVENT_EXCLUSION_RULES]
EXCLUDE_EVENT_RE = compile_exclusion_pattern(EVENT_EXCLUSION_RULES)


def filter_titles_with_vs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Only keep rows whose 'title' contains the token 'vs' (case-insensitive).
    """
    if "title" not in df:
        return df
    return df[df["title"].str.contains(r"\bvs\b", case=False, regex=True, na=False)]


def drop_non_battles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows whose 'title' matches active title exclusion rules.
    """
    if "title" not in df:
        return df
    return df[~df["title"].str.contains(EXCLUDE_RE, na=False)]


def drop_excluded_events(
    df: pd.DataFrame,
    event_col: str = "event_name",
) -> pd.DataFrame:
    """Drop rows whose event name matches active event exclusion rules."""
    if event_col not in df:
        return df
    return df[~df[event_col].astype("string").str.contains(EXCLUDE_EVENT_RE, na=False)]


def keep_1v1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep rows that look like 1v1 battles based on simple heuristics.
    """
    if "title" not in df:
        return df

    # Defensive: mark non-strings
    is_str = df["title"].apply(lambda x: isinstance(x, str))

    # Lowercase only the string titles
    s = df["title"].where(is_str, None).fillna("").str.lower()

    vs_count = s.str.count(r"\bvs\b")
    and_count = s.str.count(r"\band\b")
    has_slash = s.str.contains(r"/", na=False)
    has_plus = s.str.contains(r"\+", na=False)
    and_both = s.str.contains(r"\band\b.*\bvs\b.*\band\b", na=False)
    n_on_m = s.str.contains(r"\b\d+\s*on\s*\d+\b", na=False)

    not_1v1 = (
        (~is_str)
        | (vs_count > 1)
        | (and_count > 1)
        | has_slash
        | has_plus
        | and_both
        | n_on_m
    )

    return df.loc[~not_1v1]


def _resolved_manual_matchup_ids(
    manual_matchups: ManualMatchupMap | None,
) -> set[str]:
    if not manual_matchups:
        return set()
    return {
        str(battle_id)
        for battle_id, row in manual_matchups.items()
        if row.get("emcee1") and row.get("emcee2")
    }


def _pending_manual_matchup_ids(
    manual_matchups: ManualMatchupMap | None,
) -> set[str]:
    if not manual_matchups:
        return set()
    return {
        str(battle_id)
        for battle_id, row in manual_matchups.items()
        if not (row.get("emcee1") and row.get("emcee2"))
    }


def _manual_matchup_notes(manual_matchups: ManualMatchupMap | None) -> dict[str, object]:
    if not manual_matchups:
        return {}
    return {
        str(battle_id): row.get("note") or pd.NA
        for battle_id, row in manual_matchups.items()
    }


def _manual_matchup_audit_fields(
    manual_matchups: ManualMatchupMap | None,
    rename_map: RenameMap | None = None,
) -> pd.DataFrame:
    if not manual_matchups:
        return pd.DataFrame(
            columns=[
                "battle_key",
                "helper_emcee",
                "emcee1_status",
                "emcee2_status",
                "helper_status",
                "manual_note",
            ]
        )
    if rename_map is None:
        rename_map = load_rename_map()

    rows = []
    for battle_id, row in manual_matchups.items():
        helper = row.get("helper_emcee")
        if helper:
            helper = rename_map.get(str(helper).strip(), str(helper).strip())
        rows.append(
            {
                "battle_key": str(battle_id),
                "helper_emcee": helper or pd.NA,
                "emcee1_status": row.get("emcee1_status") or pd.NA,
                "emcee2_status": row.get("emcee2_status") or pd.NA,
                "helper_status": row.get("helper_status") or pd.NA,
                "manual_note": row.get("note") or pd.NA,
            }
        )
    return pd.DataFrame(rows)


def _upload_decision_ids(
    upload_decisions: UploadDecisionMap | None,
    decision: str,
) -> set[str]:
    if not upload_decisions:
        return set()
    return {
        str(upload_id)
        for upload_id, row in upload_decisions.items()
        if row.get("decision") == decision
    }


def _upload_decision_audit_fields(
    upload_decisions: UploadDecisionMap | None,
) -> pd.DataFrame:
    if not upload_decisions:
        return pd.DataFrame(
            columns=[
                "id",
                "upload_decision",
                "upload_decision_reason",
                "upload_decision_note",
            ]
        )

    rows = []
    for upload_id, row in upload_decisions.items():
        rows.append(
            {
                "id": str(upload_id),
                "upload_decision": row.get("decision") or pd.NA,
                "upload_decision_reason": row.get("reason") or pd.NA,
                "upload_decision_note": row.get("note") or pd.NA,
            }
        )
    return pd.DataFrame(rows)


def _apply_upload_decision_fields(
    df: pd.DataFrame,
    upload_decisions: UploadDecisionMap | None,
) -> pd.DataFrame:
    if df.empty or "id" not in df.columns or not upload_decisions:
        return df

    fields = _upload_decision_audit_fields(upload_decisions)
    if fields.empty:
        return df

    out = df.copy()
    if any(col in out.columns for col in fields.columns if col != "id"):
        out = out.drop(columns=[c for c in fields.columns if c != "id" and c in out.columns])
    out["id"] = out["id"].astype(str)
    return out.merge(fields, on="id", how="left")


def _hold_upload_decision_rows(
    df: pd.DataFrame,
    upload_decisions: UploadDecisionMap | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Remove exact exclude/review decisions from the flow and return their rows.

    Returns ``(kept, excluded, review)``. Exact ``include`` decisions do not exit
    here; they protect rows from broad filters later in the pipeline.
    """
    if "id" not in df.columns or not upload_decisions:
        empty = df.iloc[0:0].copy()
        return df, empty, empty

    ids = df["id"].astype(str)
    exclude_ids = _upload_decision_ids(upload_decisions, "exclude")
    review_ids = _upload_decision_ids(upload_decisions, "review")

    excluded = df.loc[ids.isin(exclude_ids)].copy()
    if not excluded.empty:
        excluded = _apply_upload_decision_fields(excluded, upload_decisions)
        excluded["pipeline_status"] = "excluded"
        excluded["stage"] = "upload_decision_override"
        excluded["excluded_reason"] = "manual upload decision"
        excluded["exit_category"] = excluded["upload_decision_reason"]

    review = df.loc[ids.isin(review_ids)].copy()
    if not review.empty:
        review = _apply_upload_decision_fields(review, upload_decisions)
        review["pipeline_status"] = "needs_upload_review"
        review["stage"] = "upload_decision_review"
        review["exit_category"] = review["upload_decision_reason"]

    held_ids = exclude_ids | review_ids
    kept = df.loc[~ids.isin(held_ids)].copy()
    return kept, excluded, review


def _keep_upload_decision_includes(
    before: pd.DataFrame,
    after: pd.DataFrame,
    upload_decisions: UploadDecisionMap | None,
) -> pd.DataFrame:
    """Add exact ``include`` ids back after a broad filter removes them."""
    if "id" not in before.columns or not upload_decisions:
        return after

    include_ids = _upload_decision_ids(upload_decisions, "include")
    if not include_ids:
        return after

    after_ids = set(after["id"].astype(str)) if "id" in after.columns else set()
    restore_ids = include_ids - after_ids
    if not restore_ids:
        return after

    restored = before.loc[before["id"].astype(str).isin(restore_ids)].copy()
    if restored.empty:
        return after
    return pd.concat([after, restored], ignore_index=True)


def keep_1v1_or_manual_matchup(
    df: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
) -> pd.DataFrame:
    """
    Keep normal 1v1-looking titles plus explicitly resolved manual matchups.

    This lets a hand-reviewed no-show title such as ``A + B vs C`` enter the
    pipeline once ``manual_matchups.csv`` records the actual 1v1 matchup, while
    keeping the broad ``+`` heuristic conservative for everything else.
    """
    kept = keep_1v1(df)
    resolved_ids = _resolved_manual_matchup_ids(manual_matchups)
    if not resolved_ids or "id" not in df.columns:
        return kept

    already_kept = set(kept["id"].astype(str)) if "id" in kept.columns else set()
    manual = df[
        df["id"].astype(str).isin(resolved_ids - already_kept)
    ]
    if manual.empty:
        return kept
    return pd.concat([kept, manual], ignore_index=True)


def copy_yt_title(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve the cleaned YouTube title (including any 'pt. N' suffix)
    in a new column 'yt_raw_title'.

    Assumes this is called AFTER clean_titles().
    """
    if "title" not in df:
        return df
    return df.assign(yt_raw_title=df["title"])


_PT_RE = re.compile(r"\s*pt\.?\s*(\d+)$", flags=re.IGNORECASE)
_PT_SUFFIX = re.compile(r"\s*pt\.?\s*\d+$", flags=re.IGNORECASE)


def _base_title(s: str):
    """Strip a trailing 'pt. N' / 'pt N' suffix from a title, if present."""
    if not isinstance(s, str):
        return s
    return _PT_RE.sub("", s.strip()).strip()


def strip_pt_suffix_from_title(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy with 'title' cleaned so trailing 'pt. N' / 'pt N'
    is removed. Does NOT collapse multi-part battles.
    """
    if "title" not in df:
        return df
    return df.assign(title=df["title"].map(_base_title))


def _base_raw_title(s: str) -> str:
    """
    Strip trailing 'pt. N' from a raw title.

    Reuses _base_title; non string inputs become empty string, but in practice
    we only call this on rows already known to be 'pt. N' strings.
    """
    cleaned = _base_title(s)
    return "" if not isinstance(cleaned, str) else cleaned


def _part_num(s: str):
    """Extract the part number N from '... pt. N', or None if not present."""
    if not isinstance(s, str):
        return None
    m = _PT_RE.search(s)
    return int(m.group(1)) if m else None

_VS_SPLIT = re.compile(r"\s+vs\s+", flags=re.IGNORECASE)

_PREFIX = re.compile(r"^FlipTop(?: [^–-]+)?\s*[-–]\s*", flags=re.IGNORECASE)
_POST2 = re.compile(r"\s*[@|(*].*$")  # trims " @channel", " | whatever", " (desc", "*whatever"
_TRAIL_NUM = re.compile(r"\s+\d+$")   # trailing stand-alone numbers


def extract_matchup_from_title(title: str) -> str | None:
    """
    Extract a clean 'Emcee A vs Emcee B' string from a FlipTop video title.

    1) Trim whitespace.
    2) Remove optional 'FlipTop … – ' prefix.
    3) Split on a single 'vs' (case-insensitive).
    4) Left side -> emcee1.
    5) Right side -> emcee2, trimming trailing clutter ('@…', '|…', '(…', '*…')
       and any trailing standalone number.
    6) If either side ends up empty, return None; else return 'A vs B'.
    """
    if not isinstance(title, str):
        return None

    t = title.strip()
    t = _PREFIX.sub("", t)

    parts = _VS_SPLIT.split(t, maxsplit=1)
    if len(parts) != 2:
        return None

    emcee1 = parts[0].strip()
    emcee2 = _POST2.sub("", parts[1].strip())
    emcee2 = _TRAIL_NUM.sub("", emcee2).strip()

    if not emcee1 or not emcee2:
        return None

    return f"{emcee1} vs {emcee2}"

def add_matchup_and_split(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'matchup', 'emcee1', and 'emcee2' columns inferred from 'title'.

    - Builds 'matchup' by applying `extract_matchup_from_title`.
    - Removes any trailing ' - …' annotation from 'matchup'.
    - Splits 'matchup' at 'vs' into 'emcee1' and 'emcee2'.
    """
    if "title" not in df:
        return df

    out = df.copy()
    out["matchup"] = out["title"].map(extract_matchup_from_title)

    # Drop trailing " - Finals" etc
    out["matchup"] = out["matchup"].str.replace(r"\s-\s.*$", "", regex=True)

    split = out["matchup"].str.split(_VS_SPLIT, n=1, expand=True)
    out[["emcee1", "emcee2"]] = split

    out["emcee1"] = out["emcee1"].str.strip()
    out["emcee2"] = out["emcee2"].str.strip()

    return out


def apply_manual_matchup_overrides(
    df: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
) -> pd.DataFrame:
    """
    Override ``matchup``, ``emcee1``, and ``emcee2`` for resolved manual rows.

    Pending rows in ``manual_matchups.csv`` use ``NA`` for both names and are
    ignored here; they are only surfaced in audit output.
    """
    if not manual_matchups or "id" not in df.columns:
        return df

    out = df.copy()
    ids = out["id"].astype(str)
    for battle_id, row in manual_matchups.items():
        emcee1 = row.get("emcee1")
        emcee2 = row.get("emcee2")
        if not emcee1 or not emcee2:
            continue

        mask = ids == str(battle_id)
        if not bool(mask.any()):
            continue
        out.loc[mask, "matchup"] = f"{emcee1} vs {emcee2}"
        out.loc[mask, "emcee1"] = emcee1
        out.loc[mask, "emcee2"] = emcee2
    return out


def apply_emcee_rename(
    df: pd.DataFrame,
    rename_map: RenameMap | None = None,
) -> pd.DataFrame:
    """
    Canonicalize emcee names using an alias to canonical mapping.

    Applies to both emcee1 and emcee2.
    """
    if rename_map is None:
        return df

    if not {"emcee1", "emcee2"} <= set(df.columns):
        return df

    out = df.copy()
    out["emcee1"] = out["emcee1"].astype("string").str.strip().replace(rename_map)
    out["emcee2"] = out["emcee2"].astype("string").str.strip().replace(rename_map)
    return out


def add_matchup_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build 'matchup_clean' from canonicalized emcee columns.
    """
    if not {"emcee1", "emcee2"} <= set(df.columns):
        return df

    return df.assign(
        matchup_clean=(
            df["emcee1"].astype("string").str.strip()
            + " vs "
            + df["emcee2"].astype("string").str.strip()
        )
    )




# ---------------------------------------------------------------------------
# IV. Atomic transforms on event metadata
# ---------------------------------------------------------------------------

# Month token: full or abbr, optional trailing period (incl. Sept.)
_MONTH = (
    r"(Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May\.?|"
    r"Jun(?:e)?\.?|Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:t\.?|tember)\.?|"
    r"Oct(?:ober)?\.?|Nov(?:ember)?\.?|Dec(?:ember)?\.?)"
)

# <Month> <day or day-range>[,] <year>
# examples: "Oct. 29, 2010" | "Feb 6, 2010" | "Dec. 20-21, 2024"
# Groups: 1=month, 2=start day, 3=end day (optional, same-month range), 4=year.
_DATE_RANGE = re.compile(
    rf"{_MONTH}\s+(\d{{1,2}})(?:\s*-\s*(\d{{1,2}}))?\s*,\s*(\d{{4}})",
    re.I,
)


def _parse_event_date_range(text) -> tuple[str | None, str | None]:
    """
    Find the first ``Month D[-D2], YYYY`` in ``text`` and return
    ``(start_iso, end_iso)`` as ISO date strings, or ``(None, None)`` if none.

    For a single day, ``end == start``. Only same-month ranges are recognized
    (e.g. "December 13-14, 2025"); a cross-month range would yield just the
    start day. If the end day parses before the start (bad data), it is clamped
    to the start.
    """
    if not isinstance(text, str):
        return (None, None)

    m = _DATE_RANGE.search(text)
    if not m:
        return (None, None)

    month_tok = m.group(1).replace(".", "")
    day_start, day_end, year = m.group(2), m.group(3), m.group(4)

    try:
        start = dateparse.parse(f"{month_tok} {day_start} {year}").date()
    except Exception:
        return (None, None)

    if day_end:
        try:
            end = dateparse.parse(f"{month_tok} {day_end} {year}").date()
        except Exception:
            end = start
    else:
        end = start

    if end < start:
        end = start

    return (start.isoformat(), end.isoformat())

def split_event_description(
    df: pd.DataFrame,
    desc_col: str = "event_description",
) -> pd.DataFrame:
    """
    Split the event_description into:
      - event_date  (ISO string, e.g. '2010-10-29')
      - event_location (text before the date, after the last colon)

    Mirrors the notebook logic:
      - find a month/day(/day-range)/year pattern
      - parse that to an ISO date string
      - treat the left side as a location-ish string
    """
    if desc_col not in df:
        return df

    def extract(desc: str):
        if not isinstance(desc, str) or not desc.strip():
            return (pd.NA, pd.NA)

        m = _DATE_RANGE.search(desc)
        if not m:
            # no recognizable date; treat entire string as location
            return (pd.NA, desc.strip())

        # normalize month (remove trailing dot), take FIRST day if range is given
        month_tok = m.group(1).replace(".", "")
        day_first = m.group(2)
        year = m.group(4)  # group 3 is the optional range end day

        date_text = (
            f"{month_tok} {day_first} {year}"
            if year
            else f"{month_tok} {day_first}"
        )

        try:
            event_date = dateparse.parse(date_text).date().isoformat()
        except Exception:
            # fallback: keep the raw string if parsing fails
            event_date = date_text

        # location: substring before the date, after the last colon
        # (drop "FlipTop presents:" etc.)
        pre = desc[:m.start()]
        loc = pre.split(":")[-1].strip().strip(" .")

        return (event_date, loc or pd.NA)

    pairs = df[desc_col].map(extract)

    return df.assign(
        event_date=pairs.map(lambda x: x[0]),
        event_location=pairs.map(lambda x: x[1]),
    )


def parse_event_date(
    df: pd.DataFrame,
    date_col: str = "event_date",
) -> pd.DataFrame:
    """
    Parse the event_date string into a timezone-naive datetime64[ns].

    Converts ISO-like strings or other parsed date strings into
    pandas datetimes. Invalid parses become NaT.
    """
    if date_col not in df:
        return df

    return df.assign(
        **{
            date_col: pd.to_datetime(df[date_col], errors="coerce")
        }
    )


def clean_event_location(
    df: pd.DataFrame,
    raw_loc_col: str = "event_location",
    new_col: str = "event_location_clean",
    aliases: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """
    Clean up event location strings.

    Heuristics (mirrors what you were doing in the notebook):

      - If '@' is present, keep the part after the LAST '@'
        e.g. "FlipTop presents: Ahon 12 @ B-Side, Makati City"
             -> "B-Side, Makati City"

      - Otherwise, try to grab the part after the last sentence boundary
        ('.', '!', '?') as a crude location-ish suffix.

      - Strip out obvious "FlipTop ..." prefixes if they survive.

      - Normalize whitespace and strip trailing punctuation.

      - Finally, canonicalize known location values (e.g. Davao variants) via the
        ``aliases`` map, loaded from data/overrides/location_aliases.csv when not
        supplied.
    """
    if raw_loc_col not in df:
        return df

    if aliases is None:
        aliases = load_location_aliases()

    def _clean_loc(val):
        if not isinstance(val, str):
            return pd.NA

        txt = val.strip()
        if not txt:
            return pd.NA

        # 1) Prefer text after the last '@'
        if "@" in txt:
            txt = txt.rsplit("@", 1)[-1].strip()

        else:
            # 2) Otherwise, try after the last sentence boundary
            # e.g. "Some event. Quezon City" -> "Quezon City"
            for sep in [".", "!", "?"]:
                if sep in txt:
                    txt = txt.split(sep)[-1].strip()

        # 3) Strip leading FlipTop-style prefixes if any remain
        txt = re.sub(
            r"^(FlipTop(?: Battle League)?(?: presents)?[:\-]?\s*)",
            "",
            txt,
            flags=re.IGNORECASE,
        )

        # 4) Normalize spaces and strip trailing punctuation
        txt = re.sub(r"\s+", " ", txt).strip(" \t\n\r-–,.;:")

        if not txt:
            return pd.NA

        # 5) Collapse an accidentally repeated word, e.g. a source typo like
        # "Makati City City" -> "Makati City".
        txt = re.sub(r"\b(\w+)(?:\s+\1\b)+", r"\1", txt, flags=re.IGNORECASE)

        # 6) Ensure the country name is preceded by a comma. Some source
        # descriptions write "..., City. Philippines" or "..., Metro Manila
        # Philippines" (period, or no separator at all) instead of
        # "..., Philippines". Targets "Philippines" specifically, so legitimate
        # abbreviation periods (St., Dr., J.P., ...) are left untouched.
        txt = re.sub(r"(?<=\w)[ .]+Philippines\b", ", Philippines", txt)

        # Canonicalize known location values (e.g. Davao variants). Whitespace is
        # already normalized above, so an exact-match lookup suffices.
        return aliases.get(txt, txt)

    return df.assign(
        **{new_col: df[raw_loc_col].map(_clean_loc)}
    )


def extract_event_name_from_description(
    df: pd.DataFrame,
    desc_col: str = "description",
    new_col: str = "event_name_from_desc",
) -> pd.DataFrame:
    """
    Extract event name from FlipTop YouTube description.

    Handles patterns like:
      - 'FlipTop presents: Ahon 16 @ The Tent, ...'
      - 'FlipTop presents Ahon 16 @ The Tent, ...'
      - 'FlipTop presents: Gubat 12, Day 1 @ ...'

    Writes the result into `new_col`.
    """
    if desc_col not in df:
        return df

    pattern = re.compile(
        r"fliptop\s+presents\s*:?\s*(.+?)\s*@",
        flags=re.IGNORECASE,
    )

    def _extract(desc: str):
        if not isinstance(desc, str):
            return pd.NA
        # collapse whitespace
        text = re.sub(r"\s+", " ", desc)
        m = pattern.search(text)
        if not m:
            return pd.NA
        raw = m.group(1).strip()
        raw = re.sub(r"\s+", " ", raw)
        raw = raw.strip(" -–—")
        return raw if raw else pd.NA

    return df.assign(**{new_col: df[desc_col].map(_extract)})


def fill_metadata_from_yt_description(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use YouTube 'description' to fill event_name, event_date, and
    event_location_clean where they are missing.

    This is especially useful for newer battles whose metadata did not
    appear on the FlipTop website scrape.
    """
    df = df.copy()

    if "description" not in df.columns:
        return df

    # 1) Fill event_name from description where it is missing
    tmp = extract_event_name_from_description(df, desc_col="description",
                                              new_col="_event_name_from_desc")
    if "event_name" in df.columns:
        df["event_name"] = df["event_name"].fillna(tmp["_event_name_from_desc"])
    else:
        df["event_name"] = tmp["_event_name_from_desc"]

    # 2) Derive event_date and event_location_clean from description
    # Treat description as an event_description surrogate
    tmp2 = df.assign(event_description=df["description"])
    tmp2 = split_event_description(tmp2)        # adds event_date (string), event_location
    tmp2["event_date"] = pd.to_datetime(tmp2["event_date"], errors="coerce")
    tmp2 = clean_event_location(tmp2)           # adds event_location_clean

    # 3) Only fill where original values are missing
    if "event_date" in df.columns:
        missing_date_mask = df["event_date"].isna()
        df.loc[missing_date_mask, "event_date"] = tmp2.loc[missing_date_mask, "event_date"]
    else:
        df["event_date"] = tmp2["event_date"]

    if "event_location_clean" in df.columns:
        missing_loc_mask = df["event_location_clean"].isna()
        df.loc[missing_loc_mask, "event_location_clean"] = tmp2.loc[
            missing_loc_mask, "event_location_clean"
        ]
    else:
        df["event_location_clean"] = tmp2["event_location_clean"]

    # Drop helper column
    df = df.drop(columns=["_event_name_from_desc"], errors="ignore")

    return df


def apply_manual_event_location_overrides(
    df: pd.DataFrame,
    event_name_col: str = "event_name",
    event_location_col: str = "event_location",
    overrides: Mapping[str, str] | None = None,
    patterns: Iterable[tuple[str, str]] | None = None,
) -> pd.DataFrame:
    """
    Apply hand-maintained event location overrides after final cleanup.

    Two hand-maintained tables (loaded from data/overrides/ when not supplied):

      - ``patterns`` (event_location_patterns.csv): any event_location *containing*
        the substring is replaced wholesale (e.g. "D' mention ..." -> the FlipTop
        Baraks venue).
      - ``overrides`` (event_locations.csv): per-event fixes keyed by exact
        event_name, for battles whose location could not be extracted correctly
        from the source description (COVID-era obfuscation, or a no-'@' description
        that leaked the event name into the location).

    See the ``note`` column in each CSV for the per-row rationale.
    """
    if overrides is None:
        overrides = load_event_location_overrides()
    if patterns is None:
        patterns = load_event_location_patterns()

    out = df.copy()

    if event_location_col in out.columns:
        for substring, location in patterns:
            mask = out[event_location_col].astype("string").str.contains(
                substring,
                regex=False,
                na=False,
            )
            out.loc[mask, event_location_col] = location

    if {event_name_col, event_location_col} <= set(out.columns):
        for event_name, location in overrides.items():
            out.loc[out[event_name_col] == event_name, event_location_col] = location

    return out


def apply_manual_event_date_overrides(
    df: pd.DataFrame,
    id_col: str = "id",
    date_col: str = "event_date",
    source_col: str = "event_date_source",
    overrides: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """
    Pin event_date for specific battles whose YouTube description is wrong.

    Per-battle corrections keyed by YouTube video id (matching either a scalar id
    or any id within a consolidated multi-part battle's id list), loaded from
    data/overrides/event_dates.csv when not supplied. Used for the rare case where
    a battle's own description mis-dates it and the FlipTop website is authoritative;
    the date parsed from the description cannot be trusted for these. Pinned rows
    are tagged ``"manual"`` in ``source_col``.
    """
    if id_col not in df.columns or date_col not in df.columns:
        return df

    if overrides is None:
        overrides = load_event_date_overrides()

    out = df.copy()
    for battle_id, iso in overrides.items():
        mask = out[id_col].map(
            lambda x: x == battle_id or (isinstance(x, list) and battle_id in x)
        )
        out.loc[mask, date_col] = pd.Timestamp(iso)
        if mask.any():
            if source_col not in out.columns:
                out[source_col] = pd.Series(pd.NA, index=out.index, dtype="object")
            out.loc[mask, source_col] = "manual"

    return out


# Trailing day label on an event name: "(Day 2)", ", Day 2", or " Day 2".
_EVENT_DAY_RE = re.compile(r"\s*[,(]?\s*\bday\s*(\d+)\b\s*\)?\s*$", re.IGNORECASE)


def _split_event_day(name) -> tuple[object, int | None]:
    """
    Split a trailing 'Day N' label off an event name.

    Returns ``(clean_name, day)``. Handles the parenthesized website form
    ("Ahon 16 (Day 2)") and the comma form from YouTube descriptions
    ("Gubat 12, Day 1"). If there is no day label, ``day`` is None and the name
    is returned trimmed (non-strings pass through untouched).
    """
    if not isinstance(name, str):
        return (name, None)
    m = _EVENT_DAY_RE.search(name)
    if not m:
        return (name.strip(), None)
    clean = name[: m.start()].strip()
    return (clean, int(m.group(1)))


def normalize_event_day(
    df: pd.DataFrame,
    name_col: str = "event_name",
    desc_col: str = "description",
    date_col: str = "event_date",
) -> pd.DataFrame:
    """
    Standardize multi-day event names and resolve per-day event dates.

    Two things happen:

      1. A trailing 'Day N' label is stripped from ``event_name`` (so
         "Ahon 16 (Day 1)" and "Ahon 16 (Day 2)" both become "Ahon 16").

      2. The date is corrected for the common source bug where a multi-day event
         page carries a *range* ("December 13-14, 2025") on every day's entry, so
         both Day 1 and Day 2 ended up pinned to the range's first day. When an
         event's date currently equals the range start, it is moved to the N-th
         day of the range (``start + (N-1)`` days, clamped to the range end).

    The day number (N) is used only internally here, to resolve the date and
    strip the suffix; it is not kept as a column. ``event_name`` + ``event_date``
    already specify the battle, and re-deriving the ordinal (rank of the date
    within its event) is trivial if ever needed.

    Only dates that are currently pinned to the range start are touched, so rows
    that already carry a correct per-day date (a single date in the source) and
    rows with no date (e.g. COVID-era ``NaT``) are left exactly as they are.

    Must run *after* ``apply_manual_event_location_overrides``, whose keys still
    reference the day-suffixed event names.
    """
    if name_col not in df.columns:
        return df

    out = df.copy()

    split = out[name_col].map(_split_event_day)
    out[name_col] = split.map(lambda pair: pair[0])
    days = split.map(lambda pair: pair[1])  # transient; never stored as a column

    if desc_col not in out.columns or date_col not in out.columns:
        return out

    def _resolve_date(day, current, desc):
        if pd.isna(day) or pd.isna(current):
            return current

        start_iso, end_iso = _parse_event_date_range(desc)
        if not start_iso:
            return current

        start = pd.Timestamp(start_iso)
        # Only adjust dates still pinned to the range start (the first-day bug);
        # a date that already differs has been correctly disambiguated.
        if current != start:
            return current

        span = max((pd.Timestamp(end_iso) - start).days, 0)
        offset = min(int(day) - 1, span)
        return start + pd.Timedelta(days=offset)

    out[date_col] = [
        _resolve_date(day, current, desc)
        for day, current, desc in zip(days, out[date_col], out[desc_col])
    ]
    return out


def load_versetracker_event_dates(path: PathLike) -> dict[str, pd.Timestamp]:
    """
    Load the VerseTracker event-date reference file as a ``{event_name: date}`` map.

    The file is produced by ``scripts/fetch_versetracker_event_dates.py`` and has
    one row per event keyed by the base name (no "(Day N)" suffix) with the ISO
    first-day date. A missing or malformed file yields ``{}`` so the pipeline
    still runs without it.
    """
    path = Path(path)
    if not path.exists():
        return {}

    df = pd.read_csv(path)
    if "event_name" not in df.columns or "event_date" not in df.columns:
        return {}

    dates = pd.to_datetime(df["event_date"], errors="coerce")
    return {
        str(name).strip(): date
        for name, date in zip(df["event_name"], dates)
        if pd.notna(date) and isinstance(name, str)
    }


def impute_event_dates_from_versetracker(
    df: pd.DataFrame,
    vt_dates: Mapping[str, pd.Timestamp] | None = None,
    name_col: str = "event_name",
    date_col: str = "event_date",
    source_col: str = "event_date_source",
) -> pd.DataFrame:
    """
    Fill ``NaT`` ``event_date`` values from the VerseTracker reference map.

    For each row whose date is missing, strip any trailing "Day N" off the event
    name (reusing :func:`_split_event_day`); if the clean name is in ``vt_dates``,
    set the date to the mapped first-day date plus ``(N - 1)`` days. Single-day
    events (no day label) use the mapped date unchanged.

    VerseTracker lists only the first day of a multi-day event, so the per-day
    offset (consecutive days) is applied here from the "(Day N)" suffix. Only
    ``NaT`` rows are touched - an existing date is never overwritten - and events
    absent from the map are left ``NaT``. Rows actually filled are tagged
    ``"versetracker"`` in ``source_col`` (see :func:`attach_event_metadata`).

    Must run *before* :func:`normalize_event_day`, which strips the "(Day N)"
    suffix this relies on.
    """
    if not vt_dates or name_col not in df.columns or date_col not in df.columns:
        return df

    out = df.copy()
    missing = out[date_col].isna()
    if not missing.any():
        return out

    def _imputed(name):
        if not isinstance(name, str):
            return pd.NaT
        clean, day = _split_event_day(name)
        base = vt_dates.get(clean)
        if base is None:
            return pd.NaT
        offset = (int(day) - 1) if day else 0
        return base + pd.Timedelta(days=offset)

    imputed = out.loc[missing, name_col].map(_imputed)
    out.loc[missing, date_col] = imputed

    if source_col not in out.columns:
        out[source_col] = pd.Series(pd.NA, index=out.index, dtype="object")
    out.loc[imputed.index[imputed.notna()], source_col] = "versetracker"
    return out


# ---------------------------------------------------------------------------
# V. Mid level stage functions
# These correspond to the big conceptual chunks of the pipeline.
# ---------------------------------------------------------------------------

def make_df_1v1_uploads(
    df_yt: pd.DataFrame,
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    From raw YouTube uploads to a clean table of 1v1 battle uploads.

    Pipeline:

      df_yt
        -> clean_titles
        -> parse_upload_date
        -> add_duration_columns
        -> convert_video_metrics_to_numeric
        -> copy_yt_title                (preserve original cleaned YouTube title)
        -> strip_pt_suffix_from_title   (remove 'pt. N' from working title)
        -> apply upload decisions       (exact include/exclude/review ids)
        -> filter_titles_with_vs        (keep only titles containing 'vs')
        -> drop_non_battles             (remove flyers/trailers/etc)
        -> keep_1v1_or_manual_matchup   (heuristics + resolved manual no-shows)
        -> add_matchup_and_split        (matchup, emcee1, emcee2)
        -> apply_manual_matchup_overrides
        -> apply_emcee_rename           (canonicalize emcee names)
        -> add_matchup_clean            ('emcee1 vs emcee2' using canonical names)

    Parameters
    ----------
    df_yt:
        Raw uploads DataFrame as loaded from youtube_videos.json.
    rename_map:
        Optional alias->canonical mapping for emcee names. If None, it is loaded
        from data/emcee_aliases.csv via fliptop.rename_map.load_rename_map().
    manual_matchups:
        Optional manual matchup overrides for no-show/ambiguous titles. ``None``
        loads ``data/overrides/manual_matchups.csv``; pass ``{}`` to disable.
    upload_decisions:
        Optional exact include/exclude/review decisions. ``None`` loads
        ``data/overrides/upload_decisions.csv``; pass ``{}`` to disable.

    Returns
    -------
    pd.DataFrame
        Clean 1v1 uploads with canonical emcee names and matchup_clean.
    """
    if rename_map is None:
        rename_map = load_rename_map()
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    df = prepare_uploads(df_yt)
    df, _, _ = _hold_upload_decision_rows(df, upload_decisions)
    df = _keep_upload_decision_includes(
        df,
        filter_titles_with_vs(df),
        upload_decisions,
    )
    df = _keep_upload_decision_includes(df, drop_non_battles(df), upload_decisions)
    df = _keep_upload_decision_includes(
        df,
        keep_1v1_or_manual_matchup(df, manual_matchups=manual_matchups),
        upload_decisions,
    )
    df = (
        df.pipe(add_matchup_and_split)
        .pipe(apply_manual_matchup_overrides, manual_matchups=manual_matchups)
        .pipe(apply_emcee_rename, rename_map=rename_map)
        .pipe(add_matchup_clean)
    )

    # Optional: sort and reindex for nicer downstream usage
    if "upload_date" in df.columns:
        df = df.sort_values("upload_date").reset_index(drop=True)

    return df


def prepare_uploads(df_yt: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the pre-filter transforms shared by the pipeline and the exclusion
    audit: clean titles, parse dates/durations, numeric metrics, preserve the
    raw title, and strip the 'pt. N' suffix from the working title.

    Splitting this out keeps `build_excluded_uploads` in lock-step with
    `make_df_1v1_uploads`, so the audit can never drift from what the pipeline
    actually feeds into the filters.
    """
    return (
        df_yt
        .pipe(clean_titles)
        .pipe(parse_upload_date)
        .pipe(add_duration_columns)
        .pipe(convert_video_metrics_to_numeric)
        .pipe(copy_yt_title)
        .pipe(strip_pt_suffix_from_title)
    )


def _event_name_lookup(df_events: pd.DataFrame) -> pd.DataFrame:
    """Return a small ``id -> event_name`` lookup from raw event metadata."""
    event_key = "video_id" if "video_id" in df_events.columns else "id"
    if event_key not in df_events.columns or "event_name" not in df_events.columns:
        return pd.DataFrame(columns=["id", "_event_name_lookup"])
    return (
        df_events[[event_key, "event_name"]]
        .drop_duplicates(subset=[event_key])
        .rename(columns={event_key: "id", "event_name": "_event_name_lookup"})
    )


def _rule_audit_fields(row) -> dict[str, object]:
    """Structured rule metadata responsible for a row exit, if any."""
    if row["excluded_reason"] == "non-battle keyword":
        match = first_matching_rule(row.get("title"), TITLE_EXCLUSION_RULES)
    elif row["excluded_reason"] == "excluded event":
        match = first_matching_rule(row.get("event_name"), EVENT_EXCLUSION_RULES)
    else:
        match = None

    if match is None:
        return {
            "matched_keyword": pd.NA,
            "rule_id": pd.NA,
            "rule_note": pd.NA,
            "exit_category": row.get("exit_category", pd.NA),
        }

    rule, matched_keyword = match
    return {
        "matched_keyword": matched_keyword,
        "rule_id": rule.rule_id,
        "rule_note": rule.note,
        "exit_category": rule.exit_category,
    }


def _upload_stage_trace(
    df_yt: pd.DataFrame,
    df_events: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    """
    Run the upload filters once and return stage frames plus row exits.

    ``excluded`` records the first filter that removed each upload. ``pending``
    records known battle uploads held for manual matchup resolution. This is
    the shared spine for the exclusion audit, lineage audit, and stage summary.
    """
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    pre = prepare_uploads(df_yt)
    after_decisions, decision_excluded, needs_upload_review = _hold_upload_decision_rows(
        pre,
        upload_decisions,
    )

    def _dropped(
        before: pd.DataFrame,
        after: pd.DataFrame,
        reason: str,
        stage: str,
        exit_category: str,
    ) -> pd.DataFrame:
        out = before[~before["id"].isin(after["id"])].copy()
        out["pipeline_status"] = "excluded"
        out["stage"] = stage
        out["excluded_reason"] = reason
        out["exit_category"] = exit_category
        return out

    after_vs = _keep_upload_decision_includes(
        after_decisions,
        filter_titles_with_vs(after_decisions),
        upload_decisions,
    )
    after_nonbattle = _keep_upload_decision_includes(
        after_vs,
        drop_non_battles(after_vs),
        upload_decisions,
    )

    pending_ids = _pending_manual_matchup_ids(manual_matchups)
    is_pending_manual = (
        after_nonbattle["id"].astype(str).isin(pending_ids)
        if "id" in after_nonbattle.columns
        else pd.Series(False, index=after_nonbattle.index)
    )
    pending_manual = after_nonbattle.loc[is_pending_manual].copy()
    if not pending_manual.empty:
        event_lookup = _event_name_lookup(df_events)
        if not event_lookup.empty:
            pending_manual = pending_manual.merge(event_lookup, on="id", how="left")
            pending_manual["event_name"] = pending_manual["_event_name_lookup"]
            pending_manual = pending_manual.drop(columns=["_event_name_lookup"])

    if "event_name" in pending_manual.columns:
        pending_excluded_event = pending_manual["event_name"].astype("string").str.contains(
            EXCLUDE_EVENT_RE,
            na=False,
        )
    else:
        pending_excluded_event = pd.Series(False, index=pending_manual.index)

    needs_manual = pending_manual.loc[~pending_excluded_event].copy()
    if not needs_manual.empty:
        needs_manual["pipeline_status"] = "needs_manual_matchup"
        needs_manual["stage"] = "manual_matchup_override"
        needs_manual["exit_category"] = "manual_review_required"
        needs_manual["manual_note"] = needs_manual["id"].astype(str).map(
            _manual_matchup_notes(manual_matchups)
        )
    pending_event_excluded = pending_manual.loc[pending_excluded_event].copy()
    if not pending_event_excluded.empty:
        pending_event_excluded["pipeline_status"] = "excluded"
        pending_event_excluded["stage"] = "drop_excluded_events"
        pending_event_excluded["excluded_reason"] = "excluded event"
        pending_event_excluded["exit_category"] = "out_of_scope_event"
        pending_event_excluded["manual_note"] = pending_event_excluded["id"].astype(str).map(
            _manual_matchup_notes(manual_matchups)
        )

    not_pending = after_nonbattle.loc[~is_pending_manual].copy()
    after_1v1 = _keep_upload_decision_includes(
        not_pending,
        keep_1v1_or_manual_matchup(
            not_pending,
            manual_matchups=manual_matchups,
        ),
        upload_decisions,
    )
    with_event_meta = attach_event_metadata(after_1v1, df_events)
    after_event_filter = _keep_upload_decision_includes(
        with_event_meta,
        drop_excluded_events(with_event_meta),
        upload_decisions,
    )

    excluded = pd.concat(
        [
            decision_excluded,
            _dropped(
                after_decisions,
                after_vs,
                "no 'vs' token",
                "filter_titles_with_vs",
                "not_battle",
            ),
            _dropped(
                after_vs,
                after_nonbattle,
                "non-battle keyword",
                "drop_non_battles",
                pd.NA,
            ),
            _dropped(
                not_pending,
                after_1v1,
                "not 1v1",
                "keep_1v1",
                "format_not_supported",
            ),
            _dropped(
                with_event_meta,
                after_event_filter,
                "excluded event",
                "drop_excluded_events",
                "out_of_scope_event",
            ),
            pending_event_excluded,
        ],
        ignore_index=True,
    )

    # Add event names to early-stage drops too, without changing which filter
    # receives credit for excluding them.
    event_lookup = _event_name_lookup(df_events)
    if not event_lookup.empty:
        excluded = excluded.merge(event_lookup, on="id", how="left")
        if "event_name" in excluded.columns:
            excluded["event_name"] = excluded["event_name"].fillna(
                excluded["_event_name_lookup"]
            )
        else:
            excluded["event_name"] = excluded["_event_name_lookup"]
        excluded = excluded.drop(columns=["_event_name_lookup"])

        if not needs_manual.empty:
            needs_manual = needs_manual.merge(event_lookup, on="id", how="left")
            if "event_name" in needs_manual.columns:
                needs_manual["event_name"] = needs_manual["event_name"].fillna(
                    needs_manual["_event_name_lookup"]
                )
            else:
                needs_manual["event_name"] = needs_manual["_event_name_lookup"]
            needs_manual = needs_manual.drop(columns=["_event_name_lookup"])

        if not needs_upload_review.empty:
            needs_upload_review = needs_upload_review.merge(event_lookup, on="id", how="left")
            if "event_name" in needs_upload_review.columns:
                needs_upload_review["event_name"] = needs_upload_review[
                    "event_name"
                ].fillna(needs_upload_review["_event_name_lookup"])
            else:
                needs_upload_review["event_name"] = needs_upload_review[
                    "_event_name_lookup"
                ]
            needs_upload_review = needs_upload_review.drop(columns=["_event_name_lookup"])

    needs_review = pd.concat([needs_upload_review, needs_manual], ignore_index=True)

    rule_cols = ["matched_keyword", "rule_id", "rule_note", "exit_category"]
    if excluded.empty:
        for col in rule_cols:
            excluded[col] = pd.Series(dtype="object")
    else:
        rule_fields = excluded.apply(_rule_audit_fields, axis=1, result_type="expand")
        for col in rule_cols:
            if col not in excluded.columns:
                excluded[col] = pd.NA
            if col in rule_fields.columns:
                excluded[col] = excluded[col].combine_first(rule_fields[col])
    trace = {
        "raw_youtube": df_yt.copy(),
        "prepare_uploads": pre,
        "apply_upload_decisions": after_decisions,
        "filter_titles_with_vs": after_vs,
        "drop_non_battles": after_nonbattle,
        "manual_matchup_review_split": not_pending,
        "keep_1v1_or_manual_matchup": after_1v1,
        "attach_event_metadata": with_event_meta,
        "drop_excluded_events": after_event_filter,
    }
    return trace, excluded, needs_review


def _filter_upload_stages(
    df_yt: pd.DataFrame,
    df_events: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run the upload filters once and return ``(prepared, kept, excluded, pending)``.

    ``excluded`` records the first stage that removed each upload. This is the
    compatibility API shared by the older audit helpers.
    """
    trace, excluded, needs_manual = _upload_stage_trace(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    return (
        trace["prepare_uploads"],
        trace["drop_excluded_events"],
        excluded,
        needs_manual,
    )


def build_excluded_uploads(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    Return the raw uploads that the pipeline drops, tagged with the reason.

    Audit helper: lets you eyeball everything the pipeline excludes so real
    battles are not silently filtered out. It reruns the title/format filters
    in the same order, then attaches event metadata and applies the event-name
    exclusions, recording the first stage that removed each video:

        - "no 'vs' token"      (filter_titles_with_vs)
        - "non-battle keyword" (drop_non_battles; rule metadata is recorded)
        - "not 1v1"            (keep_1v1)
        - "excluded event"     (drop_excluded_events; event keyword recorded)
        - "manual upload decision" (exact exclude row in upload_decisions.csv)

    Returns
    -------
    pd.DataFrame
        One row per excluded upload, with id, both titles, upload_date, url,
        `excluded_reason`, `exit_category`, and rule metadata where applicable.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    _, _, excluded, _ = _filter_upload_stages(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    cols = [
        "id",
        "yt_raw_title",
        "title",
        "event_name",
        "upload_date",
        "url",
        "excluded_reason",
        "exit_category",
        "matched_keyword",
        "rule_id",
        "rule_note",
        "upload_decision",
        "upload_decision_reason",
        "upload_decision_note",
    ]
    excluded = excluded[[c for c in cols if c in excluded.columns]]
    if "upload_date" in excluded.columns:
        excluded = excluded.sort_values("upload_date").reset_index(drop=True)
    return excluded


UPLOAD_LINEAGE_COLUMNS = [
    "id",
    "yt_raw_title",
    "title",
    "upload_date",
    "url",
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
    "event_name",
    "event_date",
    "event_date_source",
    "battle_key",
    "final_title",
    "final_matchup",
    "emcee1",
    "emcee2",
    "helper_emcee",
    "emcee1_status",
    "emcee2_status",
    "helper_status",
    "source_part_number",
    "annotation_status",
    "battle_type",
    "winner",
    "votes_winner",
    "votes_loser",
]

PIPELINE_SUMMARY_COLUMNS = [
    "stage_order",
    "stage",
    "input_rows",
    "output_rows",
    "delta_rows",
    "exit_rows",
    "exit_status",
    "note",
]

PIPELINE_STAGE_DROP_COLUMNS = [
    "stage_order",
    "id",
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
    "yt_raw_title",
    "title",
    "event_name",
    "upload_date",
    "url",
]


def build_upload_lineage(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
    battle_metadata: pd.DataFrame | None = None,
    results: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build a one-row-per-YouTube-upload audit table for the wrangling pipeline.

    The lineage table answers "what happened to every raw upload?" Each raw
    ``youtube_videos.json`` row is tagged as:

    - ``excluded``: removed by the first recorded filter stage;
    - ``needs_manual_matchup``: explicitly known battle whose title needs a
      hand-entered 1v1 matchup before it can enter the final output;
    - ``included``: source upload is the published battle key;
    - ``consolidated_part``: source upload was folded into a multi-part battle
      whose key is another source id.

    For included rows it also records the final battle key, canonical matchup,
    event/date provenance, and annotation status. This is an audit surface only;
    it does not change the published ``ft_battles`` build.
    """
    from .annotations import battle_key, load_results

    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()
    prepared, _kept, excluded, needs_manual = _filter_upload_stages(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    base_cols = ["id", "yt_raw_title", "title", "upload_date", "url"]
    lineage = prepared[[c for c in base_cols if c in prepared.columns]].copy()
    lineage["id"] = lineage["id"].astype(str)

    event_lookup = _event_name_lookup(df_events)
    if not event_lookup.empty:
        lineage = lineage.merge(event_lookup, on="id", how="left")
        lineage["event_name"] = lineage["_event_name_lookup"]
        lineage = lineage.drop(columns=["_event_name_lookup"])

    for col in UPLOAD_LINEAGE_COLUMNS:
        if col not in lineage.columns:
            lineage[col] = pd.NA

    lineage["source_part_number"] = lineage["yt_raw_title"].map(_part_num)

    upload_decision_fields = _upload_decision_audit_fields(upload_decisions)
    if not upload_decision_fields.empty:
        decision_lookup = upload_decision_fields.set_index("id")
        is_decision = lineage["id"].isin(decision_lookup.index)
        for col in [
            "upload_decision",
            "upload_decision_reason",
            "upload_decision_note",
        ]:
            lineage.loc[is_decision, col] = lineage.loc[is_decision, "id"].map(
                decision_lookup[col]
            )

    if not excluded.empty:
        excluded_lookup = excluded.drop_duplicates(subset=["id"]).copy()
        excluded_lookup["id"] = excluded_lookup["id"].astype(str)
        excluded_lookup = excluded_lookup.set_index("id")
        is_excluded = lineage["id"].isin(excluded_lookup.index)
        for col in [
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
        ]:
            if col not in excluded_lookup.columns:
                continue
            lineage.loc[is_excluded, col] = lineage.loc[is_excluded, "id"].map(
                excluded_lookup[col]
            )
        if "event_name" in excluded_lookup.columns:
            lineage.loc[is_excluded, "event_name"] = lineage.loc[is_excluded, "id"].map(
                excluded_lookup["event_name"]
            )

    if not needs_manual.empty:
        manual_lookup = needs_manual.drop_duplicates(subset=["id"]).copy()
        manual_lookup["id"] = manual_lookup["id"].astype(str)
        manual_lookup = manual_lookup.set_index("id")
        is_manual = lineage["id"].isin(manual_lookup.index)
        for col in [
            "pipeline_status",
            "stage",
            "exit_category",
            "upload_decision",
            "upload_decision_reason",
            "upload_decision_note",
            "manual_note",
        ]:
            if col not in manual_lookup.columns:
                continue
            lineage.loc[is_manual, col] = lineage.loc[is_manual, "id"].map(
                manual_lookup[col]
            )
        if "event_name" in manual_lookup.columns:
            lineage.loc[is_manual, "event_name"] = lineage.loc[is_manual, "id"].map(
                manual_lookup["event_name"]
            )

    if battle_metadata is None:
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

    final_rows: list[dict[str, object]] = []
    for _, battle in battle_metadata.iterrows():
        key = battle_key(battle["id"])
        if key is None:
            continue
        source_ids = battle["id"] if isinstance(battle["id"], list) else [battle["id"]]
        for source_id in source_ids:
            source_id = str(source_id)
            final_rows.append(
                {
                    "id": source_id,
                    "pipeline_status": (
                        "included" if source_id == str(key) else "consolidated_part"
                    ),
                    "stage": (
                        "final" if source_id == str(key) else "consolidate_battle_parts"
                    ),
                    "event_name": battle.get("event_name", pd.NA),
                    "event_date": battle.get("event_date", pd.NaT),
                    "event_date_source": battle.get("event_date_source", pd.NA),
                    "battle_key": str(key),
                    "final_title": battle.get("title", pd.NA),
                    "final_matchup": battle.get("matchup", pd.NA),
                    "emcee1": battle.get("emcee1", pd.NA),
                    "emcee2": battle.get("emcee2", pd.NA),
                }
            )

    if final_rows:
        final_lookup = pd.DataFrame(final_rows).drop_duplicates(subset=["id"]).set_index("id")
        is_final_source = lineage["id"].isin(final_lookup.index)
        for col in [
            "pipeline_status",
            "stage",
            "event_name",
            "event_date",
            "event_date_source",
            "battle_key",
            "final_title",
            "final_matchup",
            "emcee1",
            "emcee2",
        ]:
            lineage.loc[is_final_source, col] = lineage.loc[is_final_source, "id"].map(
                final_lookup[col]
            )

    manual_fields = _manual_matchup_audit_fields(manual_matchups, rename_map=rename_map)
    if not manual_fields.empty:
        manual_lookup = manual_fields.set_index("battle_key")
        lineage_keys = lineage["battle_key"].where(lineage["battle_key"].notna(), lineage["id"])
        is_manual = lineage_keys.isin(manual_lookup.index)
        for col in [
            "helper_emcee",
            "emcee1_status",
            "emcee2_status",
            "helper_status",
            "manual_note",
        ]:
            lineage.loc[is_manual, col] = lineage_keys.loc[is_manual].map(
                manual_lookup[col]
            )

    if results is None:
        results = load_results()

    if not results.empty and "battle_key" in lineage.columns:
        result_lookup = results.copy()
        result_lookup["id"] = result_lookup["id"].astype(str)
        result_lookup = result_lookup.drop_duplicates(subset=["id"]).set_index("id")
        has_battle_key = lineage["battle_key"].notna()
        lineage.loc[has_battle_key, "annotation_status"] = "missing"
        annotated = has_battle_key & lineage["battle_key"].isin(result_lookup.index)
        lineage.loc[annotated, "annotation_status"] = "annotated"
        for col in ["battle_type", "winner", "votes_winner", "votes_loser"]:
            lineage.loc[has_battle_key, col] = lineage.loc[has_battle_key, "battle_key"].map(
                result_lookup[col]
            )
    else:
        lineage.loc[lineage["battle_key"].notna(), "annotation_status"] = "missing"

    unclassified = lineage["pipeline_status"].isna()
    lineage.loc[unclassified, "pipeline_status"] = "unclassified"

    lineage = lineage[UPLOAD_LINEAGE_COLUMNS]
    if "upload_date" in lineage.columns:
        lineage = lineage.sort_values("upload_date").reset_index(drop=True)
    return lineage


def build_manual_matchup_review_uploads(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    Return known battle uploads awaiting a hand-entered 1v1 matchup.

    These rows are listed in ``data/overrides/manual_matchups.csv`` with
    ``emcee1=NA`` and ``emcee2=NA``. They are intentionally removed from the
    generic ``filtered_out.csv`` audit, but are not included in final battle
    metadata until the two emcee columns are resolved.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    _, _, _, needs_manual = _filter_upload_stages(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    if not needs_manual.empty and "stage" in needs_manual.columns:
        needs_manual = needs_manual[needs_manual["stage"] == "manual_matchup_override"].copy()

    cols = [
        "id",
        "yt_raw_title",
        "title",
        "event_name",
        "upload_date",
        "url",
        "stage",
        "exit_category",
        "manual_note",
    ]
    needs_manual = needs_manual[[c for c in cols if c in needs_manual.columns]]
    if "upload_date" in needs_manual.columns:
        needs_manual = needs_manual.sort_values("upload_date").reset_index(drop=True)
    return needs_manual


def _summary_delta(input_rows: object, output_rows: object) -> object:
    if input_rows is pd.NA or output_rows is pd.NA:
        return pd.NA
    return int(output_rows) - int(input_rows)


def _pipeline_summary_row(
    *,
    stage_order: int,
    stage: str,
    input_rows: object,
    output_rows: object,
    exit_rows: int = 0,
    exit_status: str = "",
    note: str = "",
) -> dict[str, object]:
    return {
        "stage_order": stage_order,
        "stage": stage,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "delta_rows": _summary_delta(input_rows, output_rows),
        "exit_rows": int(exit_rows),
        "exit_status": exit_status,
        "note": note,
    }


def build_pipeline_stage_summary(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    versetracker_csv_name: str = "versetracker_event_dates.csv",
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
    battle_metadata: pd.DataFrame | None = None,
    ft_battles: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Summarize raw-to-output row counts at each major wrangling stage.

    This is the compact companion to ``upload_lineage.csv``: it explains the
    row-count changes step by step, while ``build_pipeline_stage_drops`` lists
    the exact upload ids that exited at filter/manual-review stages.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    trace, excluded, needs_manual = _upload_stage_trace(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    needs_manual_matchup = (
        needs_manual[needs_manual["stage"] == "manual_matchup_override"]
        if "stage" in needs_manual.columns
        else needs_manual.iloc[0:0]
    )
    needs_upload_review = (
        needs_manual[needs_manual["stage"] == "upload_decision_review"]
        if "stage" in needs_manual.columns
        else needs_manual.iloc[0:0]
    )

    event_drops = excluded[excluded["stage"] == "drop_excluded_events"]
    event_stage_ids = (
        set(trace["attach_event_metadata"]["id"].astype(str))
        if "id" in trace["attach_event_metadata"].columns
        else set()
    )
    pending_event_drops = event_drops[
        ~event_drops["id"].astype(str).isin(event_stage_ids)
    ]

    if battle_metadata is None:
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
    if ft_battles is None:
        ft_battles = build_ft_battles_from_metadata(
            battle_metadata,
            require_results=False,
        )

    def drop_count(stage: str) -> int:
        return int((excluded["stage"] == stage).sum())

    raw_n = len(trace["raw_youtube"])
    prepared_n = len(trace["prepare_uploads"])
    decision_n = len(trace["apply_upload_decisions"])
    with_vs_n = len(trace["filter_titles_with_vs"])
    nonbattle_n = len(trace["drop_non_battles"])
    manual_flow_n = len(trace["manual_matchup_review_split"])
    manual_output_n = manual_flow_n + len(pending_event_drops)
    one_v_one_n = len(trace["keep_1v1_or_manual_matchup"])
    with_event_n = len(trace["attach_event_metadata"])
    event_input_n = with_event_n + len(pending_event_drops)
    event_output_n = len(trace["drop_excluded_events"])
    metadata_n = len(battle_metadata)
    final_n = len(ft_battles)

    rows = [
        _pipeline_summary_row(
            stage_order=1,
            stage="raw_youtube",
            input_rows=pd.NA,
            output_rows=raw_n,
            note="Rows loaded from youtube_videos.json.",
        ),
        _pipeline_summary_row(
            stage_order=2,
            stage="prepare_uploads",
            input_rows=raw_n,
            output_rows=prepared_n,
            note="Clean titles, parse dates/durations, numeric metrics, and preserve yt_raw_title.",
        ),
        _pipeline_summary_row(
            stage_order=3,
            stage="apply_upload_decisions",
            input_rows=prepared_n,
            output_rows=decision_n,
            exit_rows=(
                drop_count("upload_decision_override") + len(needs_upload_review)
            ),
            exit_status="manual_upload_decision",
            note="Apply exact include/exclude/review decisions from data/overrides/upload_decisions.csv.",
        ),
        _pipeline_summary_row(
            stage_order=4,
            stage="filter_titles_with_vs",
            input_rows=decision_n,
            output_rows=with_vs_n,
            exit_rows=drop_count("filter_titles_with_vs"),
            exit_status="excluded",
            note="Keep uploads whose working title contains a standalone 'vs' token.",
        ),
        _pipeline_summary_row(
            stage_order=5,
            stage="drop_non_battles",
            input_rows=with_vs_n,
            output_rows=nonbattle_n,
            exit_rows=drop_count("drop_non_battles"),
            exit_status="excluded",
            note="Remove title-keyword matches such as flyers, trailers, interviews, and other non-battles.",
        ),
        _pipeline_summary_row(
            stage_order=6,
            stage="manual_matchup_review_split",
            input_rows=nonbattle_n,
            output_rows=manual_output_n,
            exit_rows=len(needs_manual_matchup),
            exit_status="needs_manual_matchup",
            note=(
                "Hold unresolved manual matchups for review; resolved manual rows continue. "
                "Pending rows in excluded event categories are credited to drop_excluded_events."
            ),
        ),
        _pipeline_summary_row(
            stage_order=7,
            stage="keep_1v1_or_manual_matchup",
            input_rows=manual_flow_n,
            output_rows=one_v_one_n,
            exit_rows=drop_count("keep_1v1"),
            exit_status="excluded",
            note="Keep normal 1v1-looking titles plus explicitly resolved manual matchups.",
        ),
        _pipeline_summary_row(
            stage_order=8,
            stage="attach_event_metadata",
            input_rows=one_v_one_n,
            output_rows=with_event_n,
            note="Merge scraped event name, date, and location metadata by upload id.",
        ),
        _pipeline_summary_row(
            stage_order=9,
            stage="drop_excluded_events",
            input_rows=event_input_n,
            output_rows=event_output_n,
            exit_rows=drop_count("drop_excluded_events"),
            exit_status="excluded",
            note="Remove excluded event categories such as Process of Illumination and tryouts.",
        ),
        _pipeline_summary_row(
            stage_order=10,
            stage="finalize_battle_metadata",
            input_rows=event_output_n,
            output_rows=metadata_n,
            exit_rows=max(event_output_n - metadata_n, 0),
            exit_status="row_count_change",
            note="Consolidate multi-part uploads, apply date/location fixes, and select metadata columns.",
        ),
        _pipeline_summary_row(
            stage_order=11,
            stage="publish_ft_battles",
            input_rows=metadata_n,
            output_rows=final_n,
            exit_rows=max(metadata_n - final_n, 0),
            exit_status="row_count_change",
            note="Join annotation results by battle key and select the final analysis columns.",
        ),
    ]
    return pd.DataFrame(rows, columns=PIPELINE_SUMMARY_COLUMNS)


def build_pipeline_stage_drops(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> pd.DataFrame:
    """
    Return the exact raw upload rows that exit at filter/manual-review stages.

    ``filtered_out.csv`` remains the narrower compatibility view of excluded
    rows. This table keeps the stage/status columns, so manual-review holds are
    visible alongside true exclusions.
    """
    raw_dir = Path(raw_dir)
    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    _, excluded, needs_manual = _upload_stage_trace(
        df_yt,
        df_events,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    exits = pd.concat([excluded, needs_manual], ignore_index=True)
    if exits.empty:
        return pd.DataFrame(columns=PIPELINE_STAGE_DROP_COLUMNS)

    stage_order = {
        "upload_decision_override": 3,
        "upload_decision_review": 3,
        "filter_titles_with_vs": 4,
        "drop_non_battles": 5,
        "manual_matchup_override": 6,
        "keep_1v1": 7,
        "drop_excluded_events": 9,
    }
    exits["stage_order"] = exits["stage"].map(stage_order).fillna(99).astype(int)

    for col in PIPELINE_STAGE_DROP_COLUMNS:
        if col not in exits.columns:
            exits[col] = pd.NA

    exits = exits[PIPELINE_STAGE_DROP_COLUMNS]
    sort_cols = ["stage_order"]
    if "upload_date" in exits.columns:
        sort_cols.append("upload_date")
    sort_cols.append("id")
    return exits.sort_values(sort_cols, na_position="last").reset_index(drop=True)


def write_audit_outputs(
    raw_dir: PathLike,
    debug_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
) -> tuple[Path, Path, Path, Path, Path]:
    """
    Write the reproducible debug audit files and return their paths.

    Outputs:
    - ``filtered_out.csv``: compatibility view from ``build_excluded_uploads``;
    - ``upload_lineage.csv``: one row per raw YouTube upload.
    - ``manual_matchup_needed.csv``: known battles awaiting manual matchup rows.
    - ``pipeline_summary.csv``: row counts at each major pipeline stage.
    - ``pipeline_stage_drops.csv``: exact ids exiting at filter/manual stages.
    """
    debug_dir = Path(debug_dir)
    debug_dir.mkdir(parents=True, exist_ok=True)

    excluded = build_excluded_uploads(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    lineage = build_upload_lineage(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    manual_needed = build_manual_matchup_review_uploads(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    pipeline_summary = build_pipeline_stage_summary(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    pipeline_drops = build_pipeline_stage_drops(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )

    excluded_path = debug_dir / "filtered_out.csv"
    lineage_path = debug_dir / "upload_lineage.csv"
    manual_path = debug_dir / "manual_matchup_needed.csv"
    summary_path = debug_dir / "pipeline_summary.csv"
    drops_path = debug_dir / "pipeline_stage_drops.csv"
    excluded.to_csv(excluded_path, index=False)
    lineage.to_csv(lineage_path, index=False)
    manual_needed.to_csv(manual_path, index=False)
    pipeline_summary.to_csv(summary_path, index=False)
    pipeline_drops.to_csv(drops_path, index=False)
    return excluded_path, lineage_path, manual_path, summary_path, drops_path


def attach_event_metadata(
    df_1v1: pd.DataFrame,
    df_events_raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Attach event metadata to 1v1 uploads.

    Event-side pipeline:

      df_events_raw
        -> split_event_description   (event_date string + event_location)
        -> parse_event_date          (event_date -> datetime64[ns])
        -> clean_event_location      (event_location_clean)
        -> select relevant columns

    Then we merge df_1v1 with cleaned event metadata on YouTube video id.

    Returns
    -------
    pd.DataFrame
        1v1 uploads with event_name, event_date, and event_location_clean
        where available.
    """
    if df_events_raw is None or df_events_raw.empty:
        return df_1v1.copy()

    # 1) Clean the event metadata
    df_events = (
        df_events_raw
        .pipe(split_event_description)
        .pipe(parse_event_date)
        .pipe(clean_event_location)
    )

    # 2) Decide join keys
    # Left side: df_1v1 almost certainly has 'id' as the YouTube video id column
    if "id" in df_1v1.columns:
        left_key = "id"
    else:
        # fail soft if schema changes
        return df_1v1.copy()

    # Right side: prefer 'video_id' if present, else fall back to 'id'
    if "video_id" in df_events.columns:
        right_key = "video_id"
    elif "id" in df_events.columns:
        right_key = "id"
    else:
        # cannot join without a video id column
        return df_1v1.copy()

    # 3) Pick the columns we actually need from events
    event_cols = [right_key]
    for col in ["event_name", "event_date", "event_location_clean"]:
        if col in df_events.columns:
            event_cols.append(col)

    df_events_small = df_events[event_cols].drop_duplicates(subset=[right_key])

    # 4) Merge onto the 1v1 uploads
    out = df_1v1.merge(
        df_events_small,
        how="left",
        left_on=left_key,
        right_on=right_key,
        suffixes=("", "_event"),
    )

    # If right_key was 'video_id', we do not need it in the final table
    if right_key in out.columns and right_key != left_key:
        out = out.drop(columns=[right_key])

    # 4b) Track where each event_date came from. The website scrape is the
    # baseline; later stages overwrite this tag as they clear/fill/override.
    if "event_date" in out.columns:
        out["event_date_source"] = pd.Series(pd.NA, index=out.index, dtype="object")
        out.loc[out["event_date"].notna(), "event_date_source"] = "website"

    # 5) Apply COVID window mask and post-COVID description-based fill
    if "upload_date" in out.columns and "event_date" in out.columns:
        # a) Clear event_date during the COVID window (its source is now stale)
        start = pd.Timestamp("2020-05-01")
        end = pd.Timestamp("2022-04-27")
        covid_mask = out["upload_date"].between(start, end)
        out.loc[covid_mask, "event_date"] = pd.NaT
        out.loc[covid_mask, "event_date_source"] = pd.NA

        # b) For rows after 2022-05-01 with missing event_date, use descriptions
        post_covid_mask = out["event_date"].isna() & (out["upload_date"] > "2022-05-01")
        if post_covid_mask.any():
            # Work on that subset with the helper, then update back
            subset = fill_metadata_from_yt_description(out.loc[post_covid_mask])
            cols_to_update = ["event_name", "event_date", "event_location_clean"]
            cols_to_update = [c for c in cols_to_update if c in subset.columns]
            out.loc[post_covid_mask, cols_to_update] = subset[cols_to_update].values
            # Tag the rows the description fill actually dated.
            filled = post_covid_mask & out["event_date"].notna()
            out.loc[filled, "event_date_source"] = "description"

    return out


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
METADATA_COLUMNS = [
    "id",
    "title",
    "description",
    "upload_date",
    "duration_seconds",
    "duration_hms",
    "emcee1",
    "emcee2",
    "matchup",
    "event_name",
    "event_date",
    "event_date_source",
    "event_location",
    "url",
]

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
    raw_dir = Path(raw_dir)

    if vt_event_dates is None:
        vt_event_dates = load_versetracker_event_dates(raw_dir / versetracker_csv_name)

    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if upload_decisions is None:
        upload_decisions = load_upload_decisions()

    df_1v1 = make_df_1v1_uploads(
        df_yt,
        rename_map=rename_map,
        manual_matchups=manual_matchups,
        upload_decisions=upload_decisions,
    )
    df_with_meta = attach_event_metadata(df_1v1, df_events)
    df_with_meta = _keep_upload_decision_includes(
        df_with_meta,
        drop_excluded_events(df_with_meta),
        upload_decisions,
    )
    battle_metadata = finalize_battles(df_with_meta, vt_event_dates=vt_event_dates)

    return battle_metadata


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

    Parameters
    ----------
    battle_metadata:
        Rich one-row-per-battle metadata from :func:`build_battle_metadata`.
    results:
        Optional battle-results DataFrame. If omitted,
        ``data/annotations/battle_results.csv`` is loaded.
    require_results:
        When true, every battle must have a valid result row and every result row
        must point to a battle in ``battle_metadata``. This should stay true for
        the published output; pass false only for exploratory workflows.
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
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    vt_event_dates: Mapping[str, pd.Timestamp] | None = None,
    results: pd.DataFrame | None = None,
    require_results: bool = True,
) -> pd.DataFrame:
    """
    Build the final result-enriched ``ft_battles`` table from raw files.

    The output keeps only the project-level analysis columns and joins
    ``battle_type``, ``winner``, ``votes_winner``, and ``votes_loser`` from the
    annotations store. Use :func:`build_battle_metadata` when you need the rich
    intermediate metadata with description/provenance columns.
    """
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
    rename_map: RenameMap | None = None,
    manual_matchups: ManualMatchupMap | None = None,
    upload_decisions: UploadDecisionMap | None = None,
    fmt: str = "json",
) -> Path:
    """
    Convenience helper to build the final result-enriched ft_battles table and
    save it to disk.

    Parameters
    ----------
    out_path:
        Where to write the file, for example:
          - data/processed/ft_battles.csv
          - data/processed/ft_battles.json
    raw_dir:
        Directory that contains the raw data files under data/raw.
    youtube_json_name:
        File name of the YouTube uploads JSON.
    events_csv_name:
        File name of the scraped events CSV.
    rename_map:
        Optional emcee rename map for canonicalization.
    manual_matchups:
        Optional manual matchup overrides for ambiguous/no-show titles.
    upload_decisions:
        Optional exact include/exclude/review decisions for upload ids.
    fmt:
        "json" (default) or "csv". JSON is the default because consolidated
        multi-part battles may store list-valued `url` values, which CSV cannot
        represent or round-trip cleanly. The JSON output is newline-delimited
        (one battle per line); reload it with `pd.read_json(path, lines=True)`.

    Returns
    -------
    Path
        The path that was written.
    """
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
    """
    Serialize an already-built ft_battles table to disk.

    Split out from `write_ft_battles` so callers that already hold a built
    ft_battles (for example the refresh CLI, which also writes the emcees
    table from the same frame) do not have to rebuild it.

    Parameters
    ----------
    ft_battles:
        The battle-level table to write.
    out_path:
        Destination path, e.g. data/processed/ft_battles.json.
    fmt:
        "json" (default) or "csv". See `write_ft_battles` for why JSON is the
        default (nested values do not round-trip through CSV cleanly).

    Returns
    -------
    Path
        The path that was written.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = fmt.lower()
    if fmt == "csv":
        ft_battles.to_csv(out_path, index=False)
    elif fmt == "json":
        # newline-delimited JSON, one battle per line, UTF-8 friendly
        ft_battles.to_json(
            out_path,
            orient="records",
            lines=True,
            date_format="epoch",
            date_unit="ms",
            force_ascii=False,
        )
    else:
        raise ValueError(f"Unsupported fmt {fmt!r}; use 'csv' or 'json'.")

    return out_path
