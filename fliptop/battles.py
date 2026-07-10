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
from pathlib import Path

import pandas as pd
from dateutil import parser as dateparse

from . import uploads as _uploads
from .overrides import (
    load_event_date_overrides,
    load_event_location_overrides,
    load_event_location_patterns,
    load_location_aliases,
    load_manual_matchups,
    load_upload_decisions,
)
from .rules import (
    compile_exclusion_pattern,
    load_event_exclusion_rules,
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

# Event names are unavailable during the upload title filters above. These
# domain-level exclusions run after event metadata is attached so they also
# catch uploads whose YouTube titles do not identify them as tryouts / POI.
EVENT_EXCLUSION_RULES = load_event_exclusion_rules()
EXCLUDE_EVENT_KEYWORDS = [rule.pattern for rule in EVENT_EXCLUSION_RULES]
EVENT_EXCLUSION_RE = compile_exclusion_pattern(EVENT_EXCLUSION_RULES)
EXCLUDE_EVENT_RE = EVENT_EXCLUSION_RE


def drop_excluded_events(
    df: pd.DataFrame,
    event_col: str = "event_name",
) -> pd.DataFrame:
    """Drop rows whose event name matches active event exclusion rules."""
    if event_col not in df:
        return df
    return df[~df[event_col].astype("string").str.contains(EVENT_EXCLUSION_RE, na=False)]


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
