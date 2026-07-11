"""
fliptop.events

Event-side cleaning and date/location helpers.

This module owns Stage 2 of the pipeline: scraped FlipTop event metadata is
parsed, normalized, joined onto clean upload rows, and corrected with manual
or VerseTracker date/location references.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from pathlib import Path

import pandas as pd
from dateutil import parser as dateparse

from .contracts import VERSETRACKER_EVENT_DATES
from .overrides import (
    load_event_date_overrides,
    load_event_location_overrides,
    load_event_location_patterns,
    load_location_aliases,
)
from .rules import compile_exclusion_pattern, load_event_exclusion_rules

PathLike = str | Path

# Event names are unavailable during upload title filtering. These domain-level
# exclusions run after event metadata is attached so they also catch uploads
# whose YouTube titles do not identify them as tryouts / POI.
def drop_excluded_events(
    df: pd.DataFrame,
    event_col: str = "event_name",
    exclusion_rules=None,
) -> pd.DataFrame:
    """Drop rows whose event name matches active event exclusion rules."""
    if event_col not in df:
        return df
    if exclusion_rules is None:
        exclusion_rules = load_event_exclusion_rules()
    pattern = compile_exclusion_pattern(list(exclusion_rules))
    return df[~df[event_col].astype("string").str.contains(pattern, na=False)]


# Month token: full or abbr, optional trailing period (incl. Sept.)
_MONTH = (
    r"(Jan(?:uary)?\.?|Feb(?:ruary)?\.?|Mar(?:ch)?\.?|Apr(?:il)?\.?|May\.?|"
    r"Jun(?:e)?\.?|Jul(?:y)?\.?|Aug(?:ust)?\.?|Sep(?:t\.?|tember)\.?|"
    r"Oct(?:ober)?\.?|Nov(?:ember)?\.?|Dec(?:ember)?\.?)"
)

# <Month> <day or day-range>[,] <year>
_DATE_RANGE = re.compile(
    rf"{_MONTH}\s+(\d{{1,2}})(?:\s*-\s*(\d{{1,2}}))?\s*,\s*(\d{{4}})",
    re.I,
)


def _parse_event_date_range(text) -> tuple[str | None, str | None]:
    """
    Find the first ``Month D[-D2], YYYY`` in ``text``.

    Returns ``(start_iso, end_iso)`` as ISO date strings, or ``(None, None)``
    when no same-month date/range can be parsed.
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
    Split event descriptions into ``event_date`` and ``event_location``.

    The date is the first recognizable month/day/year pattern. The location is
    the text before that date, after the final colon.
    """
    if desc_col not in df:
        return df

    def extract(desc: str):
        if not isinstance(desc, str) or not desc.strip():
            return (pd.NA, pd.NA)

        m = _DATE_RANGE.search(desc)
        if not m:
            return (pd.NA, desc.strip())

        month_tok = m.group(1).replace(".", "")
        day_first = m.group(2)
        year = m.group(4)
        date_text = f"{month_tok} {day_first} {year}" if year else f"{month_tok} {day_first}"

        try:
            event_date = dateparse.parse(date_text).date().isoformat()
        except Exception:
            event_date = date_text

        pre = desc[: m.start()]
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
    """Parse ``event_date`` into timezone-naive ``datetime64[ns]`` values."""
    if date_col not in df:
        return df

    return df.assign(**{date_col: pd.to_datetime(df[date_col], errors="coerce")})


def clean_event_location(
    df: pd.DataFrame,
    raw_loc_col: str = "event_location",
    new_col: str = "event_location_clean",
    aliases: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Clean and canonicalize event location strings."""
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

        if "@" in txt:
            txt = txt.rsplit("@", 1)[-1].strip()
        else:
            for sep in [".", "!", "?"]:
                if sep in txt:
                    txt = txt.split(sep)[-1].strip()

        txt = re.sub(
            r"^(FlipTop(?: Battle League)?(?: presents)?[:\-]?\s*)",
            "",
            txt,
            flags=re.IGNORECASE,
        )
        txt = re.sub(r"\s+", " ", txt).strip(" \t\n\r-\u2013,.;:")

        if not txt:
            return pd.NA

        txt = re.sub(r"\b(\w+)(?:\s+\1\b)+", r"\1", txt, flags=re.IGNORECASE)
        txt = re.sub(r"(?<=\w)[ .]+Philippines\b", ", Philippines", txt)
        return aliases.get(txt, txt)

    return df.assign(**{new_col: df[raw_loc_col].map(_clean_loc)})


def extract_event_name_from_description(
    df: pd.DataFrame,
    desc_col: str = "description",
    new_col: str = "event_name_from_desc",
) -> pd.DataFrame:
    """Extract event names from FlipTop YouTube descriptions."""
    if desc_col not in df:
        return df

    pattern = re.compile(
        r"fliptop\s+presents\s*:?\s*(.+?)\s*@",
        flags=re.IGNORECASE,
    )

    def _extract(desc: str):
        if not isinstance(desc, str):
            return pd.NA
        text = re.sub(r"\s+", " ", desc)
        m = pattern.search(text)
        if not m:
            return pd.NA
        raw = m.group(1).strip()
        raw = re.sub(r"\s+", " ", raw)
        raw = raw.strip(" -\u2013\u2014")
        return raw if raw else pd.NA

    return df.assign(**{new_col: df[desc_col].map(_extract)})


def fill_metadata_from_yt_description(
    df: pd.DataFrame,
    *,
    location_aliases: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Fill missing event name/date/location fields from YouTube descriptions."""
    df = df.copy()

    if "description" not in df.columns:
        return df

    tmp = extract_event_name_from_description(
        df,
        desc_col="description",
        new_col="_event_name_from_desc",
    )
    if "event_name" in df.columns:
        df["event_name"] = df["event_name"].fillna(tmp["_event_name_from_desc"])
    else:
        df["event_name"] = tmp["_event_name_from_desc"]

    tmp2 = df.assign(event_description=df["description"])
    tmp2 = split_event_description(tmp2)
    tmp2["event_date"] = pd.to_datetime(tmp2["event_date"], errors="coerce")
    tmp2 = clean_event_location(tmp2, aliases=location_aliases)

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

    return df.drop(columns=["_event_name_from_desc"], errors="ignore")


def apply_manual_event_location_overrides(
    df: pd.DataFrame,
    event_name_col: str = "event_name",
    event_location_col: str = "event_location",
    overrides: Mapping[str, str] | None = None,
    patterns: Iterable[tuple[str, str]] | None = None,
) -> pd.DataFrame:
    """Apply hand-maintained event location pattern and event-name overrides."""
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
    """Pin event dates for specific battles whose source description is wrong."""
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


_EVENT_DAY_RE = re.compile(r"\s*[,(]?\s*\bday\s*(\d+)\b\s*\)?\s*$", re.IGNORECASE)


def _split_event_day(name) -> tuple[object, int | None]:
    """Split a trailing ``Day N`` label off an event name."""
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
    """Strip day labels from event names and fix range-start day/date bugs."""
    if name_col not in df.columns:
        return df

    out = df.copy()
    split = out[name_col].map(_split_event_day)
    out[name_col] = split.map(lambda pair: pair[0])
    days = split.map(lambda pair: pair[1])

    if desc_col not in out.columns or date_col not in out.columns:
        return out

    def _resolve_date(day, current, desc):
        if pd.isna(day) or pd.isna(current):
            return current

        start_iso, end_iso = _parse_event_date_range(desc)
        if not start_iso:
            return current

        start = pd.Timestamp(start_iso)
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
    """Load the VerseTracker event-date reference file as an event-date map."""
    path = Path(path)
    if not path.exists():
        return {}

    df = pd.read_csv(path)
    VERSETRACKER_EVENT_DATES.require(df, source=path)

    dates = pd.to_datetime(df["event_date"])
    return {
        str(name).strip(): date
        for name, date in zip(df["event_name"], dates)
        if isinstance(name, str)
    }


def impute_event_dates_from_versetracker(
    df: pd.DataFrame,
    vt_dates: Mapping[str, pd.Timestamp] | None = None,
    name_col: str = "event_name",
    date_col: str = "event_date",
    source_col: str = "event_date_source",
) -> pd.DataFrame:
    """Fill missing event dates from the VerseTracker reference map."""
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


def attach_event_metadata(
    df_1v1: pd.DataFrame,
    df_events_raw: pd.DataFrame,
    *,
    location_aliases: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Attach event name, date, and location metadata to 1v1 upload rows."""
    if df_events_raw is None or df_events_raw.empty:
        out = df_1v1.copy()
        out["event_name"] = pd.Series(pd.NA, index=out.index, dtype="object")
        out["event_date"] = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")
        out["event_location_clean"] = pd.Series(pd.NA, index=out.index, dtype="object")
        out["event_date_source"] = pd.Series(pd.NA, index=out.index, dtype="object")
        return out

    df_events = (
        df_events_raw
        .pipe(split_event_description)
        .pipe(parse_event_date)
        .pipe(clean_event_location, aliases=location_aliases)
    )

    if "id" in df_1v1.columns:
        left_key = "id"
    else:
        return df_1v1.copy()

    if "video_id" in df_events.columns:
        right_key = "video_id"
    elif "id" in df_events.columns:
        right_key = "id"
    else:
        return df_1v1.copy()

    event_cols = [right_key]
    for col in ["event_name", "event_date", "event_location_clean"]:
        if col in df_events.columns:
            event_cols.append(col)

    df_events_small = df_events[event_cols].drop_duplicates(subset=[right_key])
    out = df_1v1.merge(
        df_events_small,
        how="left",
        left_on=left_key,
        right_on=right_key,
        suffixes=("", "_event"),
    )

    if right_key in out.columns and right_key != left_key:
        out = out.drop(columns=[right_key])

    if "event_date" in out.columns:
        out["event_date_source"] = pd.Series(pd.NA, index=out.index, dtype="object")
        out.loc[out["event_date"].notna(), "event_date_source"] = "website"

    if "upload_date" in out.columns and "event_date" in out.columns:
        start = pd.Timestamp("2020-05-01")
        end = pd.Timestamp("2022-04-27")
        covid_mask = out["upload_date"].between(start, end)
        out.loc[covid_mask, "event_date"] = pd.NaT
        out.loc[covid_mask, "event_date_source"] = pd.NA

        post_covid_mask = out["event_date"].isna() & (out["upload_date"] > "2022-05-01")
        if post_covid_mask.any():
            subset = fill_metadata_from_yt_description(
                out.loc[post_covid_mask],
                location_aliases=location_aliases,
            )
            cols_to_update = ["event_name", "event_date", "event_location_clean"]
            cols_to_update = [c for c in cols_to_update if c in subset.columns]
            out.loc[post_covid_mask, cols_to_update] = subset[cols_to_update].values
            filled = post_covid_mask & out["event_date"].notna()
            out.loc[filled, "event_date_source"] = "description"

    return out
