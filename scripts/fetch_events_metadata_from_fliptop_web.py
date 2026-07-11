#!/usr/bin/env python
"""
fetch_events_metadata_from_fliptop_web.py

Scrape FlipTop battle event pages over a range of years and build
a matchup level event metadata table that includes YouTube video IDs.

For each FlipTop event page, this script extracts:
  - the event name
  - the event description
  - every listed matchup like "Emcee A vs Emcee B" in the main matchup block
  - the YouTube video ID associated with each matchup

The output is a tidy CSV with one row per matchup:

    matchup, event_name, event_description, video_id

By default the output CSV is overwritten with exactly what the scrape found
(a clean, reproducible full rebuild). Pass --merge to instead upsert the scraped
rows into the existing CSV by video_id, and --skip-known to avoid re-fetching
event pages already recorded (past years only) - together these make a narrowed
year range a fast, safe incremental update.

Usage (from repo root):

    # full overwrite scrape
    python scripts/fetch_events_metadata_from_fliptop_web.py --start 2010 --end 2026

    # incremental: only recent years, merged into the existing CSV
    python scripts/fetch_events_metadata_from_fliptop_web.py \\
        --start 2025 --end 2026 --merge --skip-known
"""

import argparse
import json
import os
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from fliptop.io import atomic_output_path

# Project root is one level above this script's directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]


DEFAULT_BASE = "https://www.fliptop.com.ph"
DEFAULT_HEADERS = {
    "User-Agent": "fliptop-eda/0.2 (educational, contact: you@example.com)"
}
_VS = re.compile(r"\s+vs\s+", re.I)


class IncompleteScrapeError(RuntimeError):
    """Raised when a scrape cannot prove it collected a complete snapshot."""


# ---------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------

def _canon(name: str, rename_map: dict | None) -> str:
    """
    Canonicalize an emcee name using an optional rename map.

    rename_map: dict mapping lowercase variant -> canonical form.
    """
    if not isinstance(name, str):
        return ""
    s = name.strip()
    if not rename_map:
        return s
    lm = {k.lower(): v for k, v in rename_map.items()}
    return lm.get(s.lower(), s)


def _get_soup(
    url: str,
    session: requests.Session,
    *,
    headers: dict | None = None,
    retries: int = 2,
    sleep: float = 0.7,
    timeout: int = 30,
) -> BeautifulSoup:
    """
    Issue a GET request and return a BeautifulSoup document.

    Includes simple retry logic and polite backoff.
    """
    hdrs = headers or DEFAULT_HEADERS
    last_exc: Exception | None = None

    for i in range(retries + 1):
        try:
            r = session.get(url, headers=hdrs, timeout=timeout)
            if r.ok:
                return BeautifulSoup(r.text, "html.parser")
            time.sleep(sleep * (i + 1))
        except Exception as e:
            last_exc = e
            time.sleep(sleep * (i + 1))

    if last_exc is not None:
        raise RuntimeError(f"Failed to GET {url}: {last_exc}")

    raise RuntimeError(f"Failed to GET {url}: status={getattr(r, 'status_code', 'unknown')}")


def _ensure_parent_dir(path: str) -> None:
    """Ensure the parent directory for a file path exists."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


# ---------------------------------------------------------------------
# Year page: collect event links
# ---------------------------------------------------------------------

def event_links_for_year(
    year: int,
    session: requests.Session,
    *,
    base: str = DEFAULT_BASE,
    headers: dict | None = None,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
) -> list[tuple[str, str]]:
    """
    Return list of (event_name, event_url) from {base}/videos/battle?year=YYYY.

    Event names come from the ft-article card titles; URLs from the wrapping <a>.
    Only single slug battle pages (for example /videos/battle/ahon-16) are kept.
    """
    list_url = f"{base}/videos/battle?year={year}"
    soup = _get_soup(
        list_url,
        session,
        headers=headers,
        retries=retries,
        sleep=request_sleep,
        timeout=timeout,
    )

    events: list[tuple[str, str]] = []

    for a in soup.select('a[href^="/videos/battle/"]'):
        title_el = a.select_one(".ft-article h4")
        if not title_el:
            continue
        event_name = title_el.get_text(strip=True)
        href = a.get("href", "")
        # only keep single slug battle pages (avoid nested sections)
        if re.fullmatch(r"/videos/battle/[^/]+", href):
            events.append((event_name, urljoin(base, href)))

    # de dupe preserving order by URL
    seen, out = set(), []
    for name, link in events:
        if link not in seen:
            seen.add(link)
            out.append((name, link))
    return out


# ---------------------------------------------------------------------
# Event page: description, matchups, and video IDs
# ---------------------------------------------------------------------

def parse_event_live(
    event_url: str,
    session: requests.Session,
    *,
    rename_map: dict | None = None,
    headers: dict | None = None,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
) -> list[dict]:
    """
    Scrape a single event page and return matchup level rows.

    Returns a list of dicts with keys:
        - matchup            (for example "Hespero vs R-Zone")
        - event_name         (string)
        - event_description  (string)
        - video_id           (YouTube video ID if found, else None)

    Implementation details for FlipTop battle pages:
      - event_name is taken from the main page header (h2.display-7).
      - event_description is the text inside <div class="col-md-9"><small>…</small></div>.
      - The main event battles are in the first:
            <div class="container-xxl">
              <div class="row my-4"> ... </div>
        Inside that row:
          * Each matchup has:
              <div class="col-md-5 my-3">
                  <div class="youtube-player" data-id="VIDEO_ID"></div>
              </div>
              <div class="col-md-7 my-3">
                  <h4> Emcee1 vs Emcee2 </h4>
                  ...
              </div>
        We:
          * collect all youtube-player data-id values inside that row
          * collect all h4 matchup texts inside that row
          * pair them in order
    """
    soup = _get_soup(
        event_url,
        session,
        headers=headers,
        retries=retries,
        sleep=request_sleep,
        timeout=timeout,
    )

    # Event name from the page header (fallback to slug)
    name_el = soup.select_one("h2.display-7, h2.display-7.fw-bold")
    event_name = (
        name_el.get_text(strip=True)
        if name_el
        else event_url.rstrip("/").split("/")[-1].replace("-", " ").title()
    )

    # Description block
    desc_el = soup.select_one("div.col-md-9 small")
    event_description = desc_el.get_text(" ", strip=True) if desc_el else ""

    # Main event row with battles
    top_row = soup.select_one("div.container-xxl > div.row.my-4") or soup.select_one(
        "div.row.my-4"
    )

    if top_row is None:
        raise IncompleteScrapeError(f"{event_url}: main battle block was not found")

    # All YouTube player divs and matchup headings for this event block. A
    # mismatch indicates source-HTML drift; truncating with zip would silently
    # drop battles from a full overwrite.
    video_divs = top_row.select("div.col-md-5.my-3 div.youtube-player")
    matchup_els = top_row.select("div.col-md-7.my-3 h4")
    if not video_divs or not matchup_els:
        raise IncompleteScrapeError(f"{event_url}: no battle rows were found")
    if len(video_divs) != len(matchup_els):
        raise IncompleteScrapeError(
            f"{event_url}: found {len(video_divs)} video players but "
            f"{len(matchup_els)} matchup headings"
        )

    rows: list[dict] = []
    for vid_div, h4 in zip(video_divs, matchup_els):
        raw_id = vid_div.get("data-id") or None
        txt = h4.get_text(" ", strip=True)

        if not txt or not _VS.search(txt) or not (3 <= len(txt) <= 100):
            raise IncompleteScrapeError(
                f"{event_url}: invalid matchup heading {txt!r}"
            )

        left_right = _VS.split(txt, maxsplit=1)
        if len(left_right) != 2:
            raise IncompleteScrapeError(
                f"{event_url}: could not split matchup heading {txt!r}"
            )

        em1 = _canon(left_right[0], rename_map)
        em2 = _canon(left_right[1], rename_map)

        # trim common postfixes from the right emcee
        em2 = re.split(r"\s*[@|(*]", em2)[0].strip()
        em2 = re.sub(r"\s+\d+$", "", em2).strip()

        rows.append(
            {
                "matchup": f"{em1} vs {em2}",
                "event_name": event_name,
                "event_description": event_description,
                "video_id": raw_id,
            }
        )

    return rows


# ---------------------------------------------------------------------
# Public helpers: scrape years and write CSV
# ---------------------------------------------------------------------

def scrape_year(
    year: int,
    *,
    rename_map: dict | None = None,
    skip_event_names: set | None = None,
    sleep: float = 0.6,
    base: str = DEFAULT_BASE,
    headers: dict | None = DEFAULT_HEADERS,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
    verbose: bool = True,
    strict: bool = False,
) -> pd.DataFrame:
    """
    Scrape a single year of FlipTop battle events.

    Returns a DataFrame with columns:
        matchup, event_name, event_description, video_id

    If ``skip_event_names`` is given, event pages whose listing-card name is
    already in that set are not fetched (incremental scraping). If nothing is
    found, returns an empty DataFrame with the schema above.
    """
    session = requests.Session()
    out_rows: list[dict] = []

    links = event_links_for_year(
        year,
        session,
        base=base,
        headers=headers,
        retries=retries,
        request_sleep=request_sleep,
        timeout=timeout,
    )

    found = len(links)
    if strict and found == 0:
        raise IncompleteScrapeError(f"{year}: no event pages were discovered")
    links = _filter_known_links(links, skip_event_names)
    if verbose:
        skipped = found - len(links)
        suffix = f" ({skipped} already known, skipped)" if skipped else ""
        print(f"{year}: found {found} event pages, scraping {len(links)}{suffix}")

    failures: list[str] = []
    for _event_name, event_url in links:
        try:
            out_rows.extend(
                parse_event_live(
                    event_url,
                    session,
                    rename_map=rename_map,
                    headers=headers,
                    retries=retries,
                    request_sleep=request_sleep,
                    timeout=timeout,
                )
            )
        except Exception as e:
            print(f"[warn] {year} {event_url} -> {e}")
            failures.append(f"{event_url}: {e}")
        time.sleep(sleep)

    if strict and failures:
        shown = "\n".join(f"  - {failure}" for failure in failures)
        raise IncompleteScrapeError(
            f"{year}: {len(failures)} event page(s) failed; refusing partial scrape:\n"
            + shown
        )

    return pd.DataFrame(
        out_rows,
        columns=["matchup", "event_name", "event_description", "video_id"],
    )


def scrape_years(
    year_start: int,
    year_end_inclusive: int,
    *,
    rename_map: dict | None = None,
    known_event_names: set | None = None,
    base: str = DEFAULT_BASE,
    headers: dict | None = DEFAULT_HEADERS,
    sleep: float = 0.6,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
    verbose: bool = True,
    strict: bool = False,
) -> pd.DataFrame:
    """
    Scrape a range of years and return one concatenated DataFrame.

    Schema is guaranteed to be:
        matchup, event_name, event_description, video_id

    If ``known_event_names`` is given, already-known events are skipped - but
    only for years strictly before the current calendar year. The current year
    is always fully re-scraped, so battles uploaded late to a recent event are
    still picked up.
    """
    frames: list[pd.DataFrame] = []
    current_year = date.today().year

    for y in range(year_start, year_end_inclusive + 1):
        if verbose:
            print(f"Scraping {y}...")
        skip = known_event_names if (known_event_names is not None and y < current_year) else None
        frames.append(
            scrape_year(
                y,
                rename_map=rename_map,
                skip_event_names=skip,
                sleep=sleep,
                base=base,
                headers=headers,
                retries=retries,
                request_sleep=request_sleep,
                timeout=timeout,
                verbose=verbose,
                strict=strict,
            )
        )

    if not frames:
        return pd.DataFrame(
            columns=["matchup", "event_name", "event_description", "video_id"]
        )

    return pd.concat(frames, ignore_index=True)


def write_events_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Write the scraped events DataFrame to CSV.

    Ensures the directory exists and uses UTF 8 encoding.
    """
    _ensure_parent_dir(output_path)
    cols = ["matchup", "event_name", "event_description", "video_id"]
    df = df.reindex(
        columns=[c for c in cols if c in df.columns]
        + [c for c in df.columns if c not in cols]
    )
    with atomic_output_path(output_path) as temporary:
        df.to_csv(temporary, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {output_path}")


# ---------------------------------------------------------------------
# Incremental helpers: merge into an existing CSV, skip already-known events
# ---------------------------------------------------------------------

EVENT_COLS = ["matchup", "event_name", "event_description", "video_id"]


def validate_full_snapshot(
    df: pd.DataFrame,
    output_path: str,
    *,
    minimum_existing_fraction: float = 0.9,
) -> None:
    """Reject an empty, malformed, duplicate, or suspiciously small overwrite."""
    missing = [column for column in EVENT_COLS if column not in df.columns]
    if missing:
        raise IncompleteScrapeError(
            "scraped event data is missing required columns: " + ", ".join(missing)
        )
    if df.empty:
        raise IncompleteScrapeError("scraped event data is empty; refusing full overwrite")

    ids = df.loc[_has_video_id(df["video_id"]), "video_id"].astype(str).str.strip()
    duplicates = ids[ids.duplicated()].unique().tolist()
    if duplicates:
        shown = ", ".join(duplicates[:5])
        raise IncompleteScrapeError(
            f"scraped event data has {len(duplicates)} duplicate video id(s): {shown}"
        )

    if not os.path.exists(output_path):
        return
    try:
        existing = pd.read_csv(output_path)
    except Exception as exc:
        raise IncompleteScrapeError(
            f"{output_path}: existing event data is unreadable; refusing overwrite"
        ) from exc

    minimum_rows = int(len(existing) * minimum_existing_fraction)
    if len(existing) and len(df) < minimum_rows:
        raise IncompleteScrapeError(
            f"scrape returned {len(df)} rows versus {len(existing)} existing rows; "
            "refusing a suspiciously smaller full overwrite (use --allow-shrink "
            "only when this reduction is intentional)"
        )


def _existing_event_names(path: str) -> set:
    """Event names already present in an events CSV (empty set if none/unreadable)."""
    if not os.path.exists(path):
        return set()
    try:
        existing = pd.read_csv(path)
    except Exception:
        return set()
    if "event_name" not in existing.columns:
        return set()
    return set(existing["event_name"].dropna().astype(str))


def _filter_known_links(links: list, skip_event_names: set | None) -> list:
    """Drop ``(event_name, url)`` links whose event name is already known."""
    if not skip_event_names:
        return links
    return [(name, url) for name, url in links if name not in skip_event_names]


def _has_video_id(series: pd.Series) -> pd.Series:
    """Boolean mask of rows whose video_id is a usable, non-empty value."""
    s = series.astype("string").str.strip()
    mask = s.notna() & (s != "") & (s.str.lower() != "nan")
    return mask.fillna(False).astype(bool)


def _merge_event_frames(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """
    Upsert ``new`` scraped rows into ``existing``, keyed by ``video_id``.

    Rows that carry a video_id replace any existing row with the same id (the
    newly scraped row wins); rows without a video_id (the scraper could not find
    the embed) are kept from both sides and exact-deduplicated. Events outside
    the scraped range therefore survive untouched - this is what makes a
    narrowed-range scrape safe.
    """
    existing = existing.reindex(columns=EVENT_COLS)
    new = new.reindex(columns=EVENT_COLS)

    e_mask = _has_video_id(existing["video_id"])
    n_mask = _has_video_id(new["video_id"])

    keyed = (
        pd.concat([existing[e_mask], new[n_mask]], ignore_index=True)
        .drop_duplicates(subset="video_id", keep="last")
    )
    unkeyed = pd.concat(
        [existing[~e_mask], new[~n_mask]], ignore_index=True
    ).drop_duplicates()

    return pd.concat([keyed, unkeyed], ignore_index=True)


def merge_events_into_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Merge scraped events into an existing CSV instead of overwriting it.

    Upserts by ``video_id`` (see ``_merge_event_frames``). If the file does not
    exist yet this is identical to ``write_events_to_csv``.
    """
    if not os.path.exists(output_path):
        write_events_to_csv(df, output_path)
        return
    existing = pd.read_csv(output_path)
    merged = _merge_event_frames(existing, df)
    write_events_to_csv(merged, output_path)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def _load_rename_map(path: str | None) -> dict | None:
    """Load a rename map JSON if provided."""
    if not path:
        return None
    with open(path, encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError("rename map JSON must be an object or dict")
    return obj


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape FlipTop battle event pages by year and write a CSV with one "
            "row per matchup, including event name, description, and video_id."
        )
    )

    parser.add_argument(
        "--start",
        type=int,
        required=True,
        help="Start year (inclusive).",
    )
    parser.add_argument(
        "--end",
        type=int,
        required=True,
        help="End year (inclusive).",
    )

    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data" / "raw" / "matchup_events_metadata.csv"),
        help="Output CSV path. Default: <project_root>/data/raw/matchup_events_metadata.csv",
    )

    parser.add_argument(
        "--rename-map",
        default=None,
        help="Optional path to rename map JSON (name variants -> canonical).",
    )

    parser.add_argument(
        "--base",
        default=DEFAULT_BASE,
        help=f"Base site URL. Default: {DEFAULT_BASE}",
    )

    parser.add_argument(
        "--user-agent",
        default=DEFAULT_HEADERS.get("User-Agent", ""),
        help="User Agent header string.",
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.6,
        help="Seconds to sleep between event page requests.",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Number of retries per request (in addition to the first attempt).",
    )

    parser.add_argument(
        "--request-sleep",
        type=float,
        default=0.7,
        help="Backoff base sleep used within request retry loop.",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout seconds.",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce logging.",
    )

    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge scraped rows into the existing output CSV (upsert by "
             "video_id) instead of overwriting it. Safe for narrowed year ranges.",
    )

    parser.add_argument(
        "--skip-known",
        action="store_true",
        help="Skip fetching event pages whose name is already in the output CSV "
             "(past years only; the current year is always re-scraped). Requires "
             "--merge.",
    )

    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Allow a full overwrite with substantially fewer rows than the existing "
             "CSV. Use only when the source dataset intentionally became smaller.",
    )

    args = parser.parse_args()

    if args.start > args.end:
        raise SystemExit("Error: --start must be <= --end")

    if args.skip_known and not args.merge:
        raise SystemExit(
            "Error: --skip-known requires --merge (otherwise the overwrite would "
            "drop every event that was skipped)."
        )

    rename_map = _load_rename_map(args.rename_map)

    headers = (
        {"User-Agent": args.user_agent}
        if args.user_agent
        else DEFAULT_HEADERS
    )

    known = _existing_event_names(args.output) if args.skip_known else None

    df = scrape_years(
        args.start,
        args.end,
        rename_map=rename_map,
        known_event_names=known,
        base=args.base,
        headers=headers,
        sleep=args.sleep,
        retries=args.retries,
        request_sleep=args.request_sleep,
        timeout=args.timeout,
        verbose=not args.quiet,
        strict=True,
    )

    if args.merge:
        merge_events_into_csv(df, args.output)
    else:
        validate_full_snapshot(
            df,
            args.output,
            minimum_existing_fraction=0.0 if args.allow_shrink else 0.9,
        )
        write_events_to_csv(df, args.output)


if __name__ == "__main__":
    main()
