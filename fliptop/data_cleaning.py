"""
fliptop.data_cleaning

Reproducible pipeline to go from raw FlipTop data to a clean
one row per battle table (df_battles).

The pipeline has three main stages:

1. From raw YouTube uploads to clean 1v1 battle uploads.
2. Attach event metadata from the scraped event file.
3. Consolidate multi part uploads into one row per battle.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, Optional

import pandas as pd
import isodate
import re
from datetime import timedelta
from .rename_map import RENAME_MAP

from dateutil import parser as dateparse


# ---------------------------------------------------------------------------
# I. Types and simple aliases
# ---------------------------------------------------------------------------

PathLike = str | Path
RenameMap = Mapping[str, str]


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
        by scrape_fliptop_event_matchups_with_ids.py.

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


EXCLUDE_KEYWORDS = [
    "tryout", "tryouts", "beatbox", "beat box", "flyer", "promo", "promos",
    "anygma machine", "unggoyan", "pre-battle interviews", "interview", "interviews",
    "salitang ugat", "trailer", "video flyer", "[live]", "silip", "sound check",
    "tribute", "anniversary party", "tutok", "review", "abangan",
]

EXCLUDE_RE = re.compile("|".join(re.escape(w) for w in EXCLUDE_KEYWORDS), flags=re.IGNORECASE)


def filter_titles_with_vs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Only keep rows whose 'title' contains the token 'vs' (case-insensitive).
    """
    if "title" not in df:
        return df
    return df[df["title"].str.contains(r"\bvs\b", case=False, regex=True, na=False)]


def drop_non_battles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows whose 'title' matches known non-battle keywords.
    """
    if "title" not in df:
        return df
    return df[~df["title"].str.contains(EXCLUDE_RE, na=False)]


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


def apply_emcee_rename(
    df: pd.DataFrame,
    rename_map: Optional[RenameMap] = None,
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
_DATE_RANGE = re.compile(
    rf"{_MONTH}\s+(\d{{1,2}})(?:\s*-\s*\d{{1,2}})?\s*,\s*(\d{{4}})",
    re.I,
)

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
        year = m.group(3) if m.lastindex and m.lastindex >= 3 else None

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
    """
    if raw_loc_col not in df:
        return df

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

        return txt

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

# ---------------------------------------------------------------------------
# V. Mid level stage functions
# These correspond to the big conceptual chunks of the pipeline.
# ---------------------------------------------------------------------------

def make_df_1v1_uploads(
    df_yt: pd.DataFrame,
    rename_map: Optional[RenameMap] = None,
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
        -> filter_titles_with_vs        (keep only titles containing 'vs')
        -> drop_non_battles             (remove flyers/promos/etc)
        -> keep_1v1                     (heuristics to keep likely 1v1 battles)
        -> add_matchup_and_split        (matchup, emcee1, emcee2)
        -> apply_emcee_rename           (canonicalize emcee names)
        -> add_matchup_clean            ('emcee1 vs emcee2' using canonical names)

    Parameters
    ----------
    df_yt:
        Raw uploads DataFrame as loaded from youtube_videos.json.
    rename_map:
        Optional alias->canonical mapping for emcee names. If None,
        defaults to fliptop.rename_map.RENAME_MAP.

    Returns
    -------
    pd.DataFrame
        Clean 1v1 uploads with canonical emcee names and matchup_clean.
    """
    if rename_map is None:
        rename_map = RENAME_MAP

    df = (
        df_yt
        .pipe(clean_titles)
        .pipe(parse_upload_date)
        .pipe(add_duration_columns)
        .pipe(convert_video_metrics_to_numeric)
        .pipe(copy_yt_title)
        .pipe(strip_pt_suffix_from_title)
        .pipe(filter_titles_with_vs)
        .pipe(drop_non_battles)
        .pipe(keep_1v1)
        .pipe(add_matchup_and_split)
        .pipe(apply_emcee_rename, rename_map=rename_map)
        .pipe(add_matchup_clean)
    )

    # Optional: sort and reindex for nicer downstream usage
    if "upload_date" in df.columns:
        df = df.sort_values("upload_date").reset_index(drop=True)

    return df


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
    
    # 5) Apply COVID window mask and post-COVID description-based fill
    if "upload_date" in out.columns and "event_date" in out.columns:
        # a) Clear event_date during the COVID window
        start = pd.Timestamp("2020-05-01")
        end = pd.Timestamp("2022-04-27")
        covid_mask = out["upload_date"].between(start, end)
        out.loc[covid_mask, "event_date"] = pd.NaT

        # b) For rows after 2022-05-01 with missing event_date, use descriptions
        post_covid_mask = out["event_date"].isna() & (out["upload_date"] > "2022-05-01")
        if post_covid_mask.any():
            # Work on that subset with the helper, then update back
            subset = fill_metadata_from_yt_description(out.loc[post_covid_mask])
            cols_to_update = ["event_name", "event_date", "event_location_clean"]
            cols_to_update = [c for c in cols_to_update if c in subset.columns]
            out.loc[post_covid_mask, cols_to_update] = subset[cols_to_update].values

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


def finalize_battles(
    df_with_meta: pd.DataFrame,
) -> pd.DataFrame:
    """
    Final tidy up step to produce df_battles.

    Mirrors the final notebook steps conceptually:

      - drop helper / raw columns not needed downstream
      - rename matchup_clean -> matchup, event_location_clean -> event_location
      - consolidate multi-part uploads
      - sort by upload_date (newest first)
      - drop yt_raw_title helper
      - apply a couple of manual location fixes
      - select and order the final columns
    """
    work = df_with_meta.copy()

    # 1) Drop raw / helper columns you don't want in df_battles
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
    if "event_location" in battles.columns:
        battles["event_location"] = (
            battles["event_location"]
            # Fix incorrect 'Davao City, Metro Manila, Philippines'
            .str.replace(
                r"^Davao City,\s*Metro Manila,\s*Philippines$",
                "Davao City, Philippines",
                regex=True,
            )
            # Normalize plain 'Davao City'
            .str.replace(
                r"^Davao City$",
                "Davao City, Philippines",
                regex=True,
            )
        )

    # 8) Select and order final columns (keep only those that exist)
    final_cols = [
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
        "event_location",
        "url",
    ]
    existing_cols = [c for c in final_cols if c in battles.columns]
    battles = battles[existing_cols]

    return battles


# ---------------------------------------------------------------------------
# VI. Top level pipeline functions
# These are what you will usually call from notebooks and scripts.
# ---------------------------------------------------------------------------

def build_df_battles(
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    rename_map: Optional[RenameMap] = None,
) -> pd.DataFrame:
    """
    Build the complete df_battles table from raw files.

    Parameters
    ----------
    raw_dir:
        Directory that contains the raw data files under data/raw.
    youtube_json_name:
        File name of the YouTube uploads JSON.
    events_csv_name:
        File name of the scraped events CSV.
    rename_map:
        Optional emcee rename map for canonicalization.

    Returns
    -------
    pd.DataFrame
        Final df_battles table with one row per battle.
    """
    raw_dir = Path(raw_dir)

    df_yt = load_youtube_uploads(raw_dir / youtube_json_name)
    df_events = load_event_metadata(raw_dir / events_csv_name)

    df_1v1 = make_df_1v1_uploads(df_yt, rename_map=rename_map)
    df_with_meta = attach_event_metadata(df_1v1, df_events)
    df_battles = finalize_battles(df_with_meta)

    return df_battles


def write_df_battles(
    out_path: PathLike,
    raw_dir: PathLike,
    youtube_json_name: str = "youtube_videos.json",
    events_csv_name: str = "matchup_events_metadata.csv",
    rename_map: Optional[RenameMap] = None,
    fmt: str = "csv",
) -> Path:
    """
    Convenience helper to build df_battles and save it to disk.

    Parameters
    ----------
    out_path:
        Where to write the file, for example:
          - data/processed/df_battles.csv
          - data/processed/df_battles.json
    raw_dir:
        Directory that contains the raw data files under data/raw.
    youtube_json_name:
        File name of the YouTube uploads JSON.
    events_csv_name:
        File name of the scraped events CSV.
    rename_map:
        Optional emcee rename map for canonicalization.
    fmt:
        "csv" (default) or "json".

    Returns
    -------
    Path
        The path that was written.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_battles = build_df_battles(
        raw_dir=raw_dir,
        youtube_json_name=youtube_json_name,
        events_csv_name=events_csv_name,
        rename_map=rename_map,
    )

    fmt = fmt.lower()
    if fmt == "csv":
        df_battles.to_csv(out_path, index=False)
    elif fmt == "json":
        # newline-delimited JSON, one battle per line, UTF-8 friendly
        df_battles.to_json(
            out_path,
            orient="records",
            lines=True,
            force_ascii=False,
        )
    else:
        raise ValueError(f"Unsupported fmt {fmt!r}; use 'csv' or 'json'.")

    return out_path