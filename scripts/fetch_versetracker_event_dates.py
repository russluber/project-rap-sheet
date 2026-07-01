#!/usr/bin/env python
"""
fetch_versetracker_event_dates.py

Scrape per-event dates from VerseTracker for FlipTop events whose event_date the
pipeline leaves blank - the "quarantine era" COVID events (see the COVID-window
mask in fliptop.battles.attach_event_metadata).

VerseTracker has one page per event, e.g.

    https://versetracker.com/events/fliptop-ahon-12

which carries a single authoritative event date in:

    <div class="event-date"><img .../>December 8, 2021</div>

This script turns each target event name into that slug, fetches the page,
extracts the date, and writes a small reference CSV:

    data/raw/versetracker_event_dates.csv
    columns: event_name, event_date, source_url

`event_name` is the base name with no "(Day N)" suffix; `event_date` is the ISO
first-day date (VerseTracker lists only the first day for multi-day events). The
pipeline's `impute_event_dates_from_versetracker` consumes this file and applies
the per-day offset for two-day events from the "(Day N)" suffix still present in
the raw scrape.

By default the target events are derived dynamically: build df_battles WITHOUT
this imputation and take every event whose event_date is NaT. Pass --events to
scrape an explicit list instead.

Usage (from repo root):

    python scripts/fetch_versetracker_event_dates.py
    python scripts/fetch_versetracker_event_dates.py --events "Ahon 12" "Zoning 10"
"""

import argparse
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparse

# Project root is one level above this script's directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BASE = "https://versetracker.com"
DEFAULT_HEADERS = {
    "User-Agent": "fliptop-eda/0.2 (educational, contact: you@example.com)"
}

OUTPUT_COLS = ["event_name", "event_date", "source_url"]


# ---------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------

def event_slug(name: str) -> str:
    """
    Turn an event name into a VerseTracker slug.

    "Ahon 12" -> "fliptop-ahon-12", "Bwelta Balentong 7" ->
    "fliptop-bwelta-balentong-7". Lowercases, maps any run of non-alphanumeric
    characters to a single hyphen, and trims stray hyphens.
    """
    body = re.sub(r"[^a-z0-9]+", "-", str(name).lower()).strip("-")
    return f"fliptop-{body}"


def event_url(name: str, *, base: str = DEFAULT_BASE) -> str:
    """Full VerseTracker event-page URL for an event name."""
    return f"{base}/events/{event_slug(name)}"


def parse_event_date(soup: BeautifulSoup) -> str | None:
    """
    Pull the ISO event date out of a VerseTracker event page.

    Reads ``div.event-date`` (text only; the leading calendar <img> carries no
    text) and parses e.g. "December 8, 2021" into "2021-12-08". Returns None if
    the element is missing or the text does not parse as a date.
    """
    el = soup.select_one("div.event-date")
    if el is None:
        return None
    text = el.get_text(" ", strip=True)
    if not text:
        return None
    try:
        return dateparse.parse(text).date().isoformat()
    except (ValueError, OverflowError):
        return None


def _get_soup(
    url: str,
    session: requests.Session,
    *,
    headers: dict | None = None,
    retries: int = 2,
    sleep: float = 0.7,
    timeout: int = 30,
) -> BeautifulSoup | None:
    """
    GET a URL and return a BeautifulSoup document, or None on a 404.

    Mirrors the polite retry/backoff style of the FlipTop web scraper. A 404 is
    treated as "no such event page" and returns None so the caller can warn and
    skip; other failures retry and then raise.
    """
    hdrs = headers or DEFAULT_HEADERS
    last_exc: Exception | None = None

    for i in range(retries + 1):
        try:
            r = session.get(url, headers=hdrs, timeout=timeout)
            if r.status_code == 404:
                return None
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
# Target selection
# ---------------------------------------------------------------------

def quarantine_event_names() -> list[str]:
    """
    Base event names whose event_date is NaT in df_battles.

    Builds df_battles WITHOUT the VerseTracker imputation (so this stays the
    stable set of COVID-masked events even after the reference CSV exists), then
    returns the distinct event names, in newest-event-first order.
    """
    from fliptop import RAW_DATA_DIR
    from fliptop.battles import build_df_battles

    df = build_df_battles(raw_dir=RAW_DATA_DIR, vt_event_dates={})
    missing = df[df["event_date"].isna()]
    # event_name is already day-suffix-stripped in df_battles; dedupe, keep order.
    names = missing["event_name"].dropna().astype(str)
    seen, out = set(), []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


# ---------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------

def fetch_event_date(
    name: str,
    session: requests.Session,
    *,
    base: str = DEFAULT_BASE,
    headers: dict | None = None,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
) -> dict | None:
    """
    Fetch one event's date row, or None if the page or date is missing.

    Returns ``{"event_name", "event_date", "source_url"}`` on success.
    """
    url = event_url(name, base=base)
    soup = _get_soup(
        url,
        session,
        headers=headers,
        retries=retries,
        sleep=request_sleep,
        timeout=timeout,
    )
    if soup is None:
        return None
    iso = parse_event_date(soup)
    if iso is None:
        return None
    return {"event_name": name, "event_date": iso, "source_url": url}


def scrape_event_dates(
    names: list[str],
    *,
    base: str = DEFAULT_BASE,
    headers: dict | None = DEFAULT_HEADERS,
    sleep: float = 0.6,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Scrape VerseTracker event dates for a list of event names.

    Events whose page 404s or has no parseable date are warned about and skipped;
    the returned frame has the OUTPUT_COLS schema (possibly with fewer rows).
    """
    session = requests.Session()
    rows: list[dict] = []

    for name in names:
        try:
            row = fetch_event_date(
                name,
                session,
                base=base,
                headers=headers,
                retries=retries,
                request_sleep=request_sleep,
                timeout=timeout,
            )
        except Exception as e:  # network error after retries
            print(f"[warn] {name!r} ({event_url(name, base=base)}) -> {e}")
            row = None

        if row is None:
            print(f"[warn] no date for {name!r} -> {event_url(name, base=base)} (skipped)")
        else:
            if verbose:
                print(f"{name:24s} {row['event_date']}")
            rows.append(row)
        time.sleep(sleep)

    return pd.DataFrame(rows, columns=OUTPUT_COLS)


def write_event_dates_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """Write the scraped event-date frame to CSV (UTF-8), sorted by event_name."""
    _ensure_parent_dir(output_path)
    out = df.reindex(columns=OUTPUT_COLS).sort_values("event_name")
    out.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(out)} rows to {output_path}")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape VerseTracker per-event dates for FlipTop quarantine-era "
            "events and write data/raw/versetracker_event_dates.csv."
        )
    )
    parser.add_argument(
        "--events",
        nargs="+",
        default=None,
        metavar="NAME",
        help="Explicit event names to scrape (e.g. --events \"Ahon 12\" \"Zoning 10\"). "
             "Default: every event whose event_date is currently NaT in df_battles.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data" / "raw" / "versetracker_event_dates.csv"),
        help="Output CSV path. Default: <project_root>/data/raw/versetracker_event_dates.csv",
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
        help="Backoff base sleep used within the request retry loop.",
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

    args = parser.parse_args()

    names = args.events if args.events else quarantine_event_names()
    if not names:
        print("No target events (nothing is missing an event_date). Nothing to do.")
        return

    if not args.quiet:
        print(f"Scraping {len(names)} event(s) from {args.base} ...")

    headers = {"User-Agent": args.user_agent} if args.user_agent else DEFAULT_HEADERS

    df = scrape_event_dates(
        names,
        base=args.base,
        headers=headers,
        sleep=args.sleep,
        retries=args.retries,
        request_sleep=args.request_sleep,
        timeout=args.timeout,
        verbose=not args.quiet,
    )

    write_event_dates_to_csv(df, args.output)


if __name__ == "__main__":
    main()
