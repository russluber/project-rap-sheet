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

Usage (from repo root):

    python scripts/fetch_events_metadata_from_fliptop_web.py
"""

import time
import os
import re
import json
import argparse
from urllib.parse import urljoin
from typing import Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup

from pathlib import Path

# Project root is one level above this script's directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]


DEFAULT_BASE = "https://www.fliptop.com.ph"
DEFAULT_HEADERS = {
    "User-Agent": "fliptop-eda/0.2 (educational, contact: you@example.com)"
}
_VS = re.compile(r"\s+vs\s+", re.I)


# ---------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------

def _canon(name: str, rename_map: Optional[dict]) -> str:
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
    headers: Optional[dict] = None,
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
    headers: Optional[dict] = None,
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
    rename_map: Optional[dict] = None,
    headers: Optional[dict] = None,
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

    rows: list[dict] = []

    if top_row:
        # All YouTube player divs for this event block
        video_divs = top_row.select("div.col-md-5.my-3 div.youtube-player")
        # All matchup h4s for this event block
        matchup_els = top_row.select("div.col-md-7.my-3 h4")

        # Protect against length mismatches by zipping
        for vid_div, h4 in zip(video_divs, matchup_els):
            raw_id = vid_div.get("data-id") or None
            txt = h4.get_text(" ", strip=True)

            if not txt or not _VS.search(txt) or not (3 <= len(txt) <= 100):
                continue

            left_right = _VS.split(txt, maxsplit=1)
            if len(left_right) != 2:
                continue

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
    rename_map: Optional[dict] = None,
    sleep: float = 0.6,
    base: str = DEFAULT_BASE,
    headers: Optional[dict] = DEFAULT_HEADERS,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Scrape a single year of FlipTop battle events.

    Returns a DataFrame with columns:
        matchup, event_name, event_description, video_id

    If nothing is found, returns an empty DataFrame with that schema.
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

    if verbose:
        print(f"{year}: found {len(links)} event pages")

    for _, event_url in links:
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
        time.sleep(sleep)

    return pd.DataFrame(
        out_rows,
        columns=["matchup", "event_name", "event_description", "video_id"],
    )


def scrape_years(
    year_start: int,
    year_end_inclusive: int,
    *,
    rename_map: Optional[dict] = None,
    base: str = DEFAULT_BASE,
    headers: Optional[dict] = DEFAULT_HEADERS,
    sleep: float = 0.6,
    retries: int = 2,
    request_sleep: float = 0.7,
    timeout: int = 30,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Scrape a range of years and return one concatenated DataFrame.

    Schema is guaranteed to be:
        matchup, event_name, event_description, video_id
    """
    frames: list[pd.DataFrame] = []

    for y in range(year_start, year_end_inclusive + 1):
        if verbose:
            print(f"Scraping {y}...")
        frames.append(
            scrape_year(
                y,
                rename_map=rename_map,
                sleep=sleep,
                base=base,
                headers=headers,
                retries=retries,
                request_sleep=request_sleep,
                timeout=timeout,
                verbose=verbose,
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
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {output_path}")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def _load_rename_map(path: Optional[str]) -> Optional[dict]:
    """Load a rename map JSON if provided."""
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as f:
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

    args = parser.parse_args()

    if args.start > args.end:
        raise SystemExit("Error: --start must be <= --end")

    rename_map = _load_rename_map(args.rename_map)

    headers = (
        {"User-Agent": args.user_agent}
        if args.user_agent
        else DEFAULT_HEADERS
    )

    df = scrape_years(
        args.start,
        args.end,
        rename_map=rename_map,
        base=args.base,
        headers=headers,
        sleep=args.sleep,
        retries=args.retries,
        request_sleep=args.request_sleep,
        timeout=args.timeout,
        verbose=not args.quiet,
    )

    write_events_to_csv(df, args.output)


if __name__ == "__main__":
    main()