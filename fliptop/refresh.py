"""
fliptop.refresh

One-command refresh of the project's processed datasets.

By default this *rebuilds* the processed outputs from the raw data already on
disk (fast, deterministic, no network or API key required):

    fliptop-refresh
    # or: python -m fliptop.refresh

With ``--fetch`` it first pulls fresh raw data (YouTube uploads + scraped event
metadata) by running the two collection scripts, then rebuilds:

    fliptop-refresh --fetch

Outputs written (under data/processed by default):
    - df_battles.json   (newline-delimited JSON, one battle per line)
    - emcees.csv
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

from . import PROJECT_ROOT, PROCESSED_DATA_DIR, RAW_DATA_DIR
from .data_cleaning import build_df_battles, save_df_battles
from .emcee_table import write_emcees_table

# Default FlipTop YouTube channel (see scripts/fetch_youtube_channel_uploads.py).
DEFAULT_CHANNEL = "UCBdHwFIE4AJWSa3Wxdu7bAQ"

SCRIPTS_DIR = PROJECT_ROOT / "scripts"


# ---------------------------------------------------------------------------
# Stage 1 (optional): fetch raw data
# ---------------------------------------------------------------------------

def fetch_raw(
    raw_dir: Path,
    *,
    channel: str = DEFAULT_CHANNEL,
    start_year: int = 2010,
    end_year: int | None = None,
) -> None:
    """
    Refresh the raw data files by running the two collection scripts.

    Invoked as subprocesses (with the current interpreter) because the scripts
    live outside the importable package and are designed as standalone CLIs.
    Both scripts are incremental / idempotent, so re-running is safe.
    """
    end_year = end_year or date.today().year

    youtube_out = raw_dir / "youtube_videos.json"
    events_out = raw_dir / "matchup_events_metadata.csv"

    print(f"[fetch] YouTube uploads (channel={channel}) -> {youtube_out}")
    _run_script(
        SCRIPTS_DIR / "fetch_youtube_channel_uploads.py",
        ["--channel", channel, "--output", str(youtube_out)],
    )

    print(f"[fetch] FlipTop web events ({start_year}-{end_year}) -> {events_out}")
    _run_script(
        SCRIPTS_DIR / "fetch_events_metadata_from_fliptop_web.py",
        ["--start", str(start_year), "--end", str(end_year), "--output", str(events_out)],
    )


def _run_script(script_path: Path, args: list[str]) -> None:
    """Run a collection script with the current Python interpreter."""
    cmd = [sys.executable, str(script_path), *args]
    subprocess.run(cmd, check=True)


# ---------------------------------------------------------------------------
# Stage 2: rebuild processed outputs from raw
# ---------------------------------------------------------------------------

def rebuild_processed(
    raw_dir: Path = RAW_DATA_DIR,
    processed_dir: Path = PROCESSED_DATA_DIR,
) -> tuple[Path, Path]:
    """
    Build df_battles once and write both processed outputs from it.

    Returns
    -------
    (battles_path, emcees_path)
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    df_battles = build_df_battles(raw_dir=raw_dir)

    battles_path = save_df_battles(
        df_battles, processed_dir / "df_battles.json", fmt="json"
    )
    print(f"[build] wrote {len(df_battles)} battles -> {battles_path}")

    emcees_path = processed_dir / "emcees.csv"
    write_emcees_table(df_battles, emcees_path)
    print(f"[build] wrote emcees table -> {emcees_path}")

    return battles_path, emcees_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh the FlipTop processed datasets. Rebuilds from existing raw "
            "data by default; use --fetch to pull fresh raw data first."
        )
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch fresh raw data (YouTube + web) before rebuilding.",
    )
    parser.add_argument(
        "--channel",
        default=DEFAULT_CHANNEL,
        help=f"YouTube channel ID for --fetch (default: {DEFAULT_CHANNEL}).",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=2010,
        help="Start year for the web scrape when --fetch is used (default: 2010).",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="End year for the web scrape when --fetch is used (default: current year).",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=RAW_DATA_DIR,
        help="Directory holding the raw data files.",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=PROCESSED_DATA_DIR,
        help="Directory to write processed outputs into.",
    )

    args = parser.parse_args(argv)

    if args.fetch:
        fetch_raw(
            args.raw_dir,
            channel=args.channel,
            start_year=args.start,
            end_year=args.end,
        )

    rebuild_processed(raw_dir=args.raw_dir, processed_dir=args.processed_dir)
    print("[done] refresh complete.")


if __name__ == "__main__":
    main()
