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

The web events scrape is a full overwrite by default. ``--events-since YEAR``
makes it incremental instead - only events from YEAR onward are scraped and
merged into the existing CSV - which is much faster for routine updates:

    fliptop-refresh --fetch --events-since 2025

Outputs written (under data/processed by default):
    - ft_battles.json   (final result-enriched table, one battle per line)
    - battle_participants.csv
    - emcees.csv
    - release_manifest.json

By default it also writes reproducible debug files under ``data/debug``:
    - filtered_out.csv
    - upload_lineage.csv
    - manual_matchup_needed.csv
    - pipeline_summary.csv
    - pipeline_stage_drops.csv
    - missing_results.csv
    - release_blockers.txt
    - release_changes.csv
    - release_changes_summary.txt
    - run_manifest.json

Use ``--no-audit`` to skip the debug files. ``--audit`` is still accepted for
backwards compatibility, but audit output is now the default maintainer path.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

from . import DATA_DIR, PROCESSED_DATA_DIR, PROJECT_ROOT, RAW_DATA_DIR
from .inputs import load_pipeline_inputs
from .lineage import write_audit_outputs
from .pipeline import PipelineRun, build_pipeline_run
from .raw_snapshot import (
    publish_raw_snapshot,
    staged_raw_snapshot,
    validate_raw_snapshot,
)
from .release import (
    CandidateArtifacts,
    ReleaseBlockedError,
    build_candidate_artifacts,
    publish_candidate_bundle,
    require_releasable,
    write_candidate_review_outputs,
    write_release_change_report,
    write_run_manifest,
)
from .validate import (
    summarize_battle_metadata,
    summarize_ft_battles,
)

# Default FlipTop YouTube channel (see scripts/fetch_youtube_channel_uploads.py).
DEFAULT_CHANNEL = "UCBdHwFIE4AJWSa3Wxdu7bAQ"

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DEBUG_DATA_DIR = DATA_DIR / "debug"


# ---------------------------------------------------------------------------
# Stage 1 (optional): fetch raw data
# ---------------------------------------------------------------------------

def fetch_raw(
    raw_dir: Path,
    *,
    channel: str = DEFAULT_CHANNEL,
    start_year: int = 2010,
    end_year: int | None = None,
    merge_events: bool = False,
    skip_known_events: bool = False,
) -> None:
    """
    Refresh the raw data files by running the two collection scripts.

    Invoked as subprocesses (with the current interpreter) against a temporary
    copy of the current raw snapshot. The candidate is contract-validated and
    promoted as a rollback-safe bundle only after both collectors succeed.

    The YouTube fetch is always incremental (it skips ids already saved). The web
    events scrape overwrites the CSV by default (a clean full rebuild); with
    ``merge_events`` it instead upserts the scraped rows into the existing CSV,
    and ``skip_known_events`` additionally avoids re-fetching event pages already
    recorded - so a narrowed ``start_year`` becomes a fast, safe update.
    """
    end_year = end_year or date.today().year

    raw_dir = Path(raw_dir)
    with staged_raw_snapshot(raw_dir) as staging_dir:
        youtube_staged = staging_dir / "youtube_videos.json"
        events_staged = staging_dir / "matchup_events_metadata.csv"

        print(f"[fetch] YouTube uploads (channel={channel}) -> staged snapshot")
        _run_script(
            SCRIPTS_DIR / "fetch_youtube_channel_uploads.py",
            ["--channel", channel, "--output", str(youtube_staged)],
        )

        events_args = [
            "--start",
            str(start_year),
            "--end",
            str(end_year),
            "--output",
            str(events_staged),
        ]
        if merge_events:
            events_args.append("--merge")
        if skip_known_events:
            events_args.append("--skip-known")

        mode = "incremental" if merge_events else "full overwrite"
        print(f"[fetch] FlipTop web events ({start_year}-{end_year}, {mode}) -> staged snapshot")
        _run_script(
            SCRIPTS_DIR / "fetch_events_metadata_from_fliptop_web.py",
            events_args,
        )

        validate_raw_snapshot(staging_dir)
        published = publish_raw_snapshot(staging_dir, raw_dir)

    print(f"[fetch] published raw snapshot -> {raw_dir} ({len(published)} files)")


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
    validate: bool = True,
    pipeline_run: PipelineRun | None = None,
    candidate: CandidateArtifacts | None = None,
) -> tuple[Path, Path]:
    """
    Build battle metadata once, publish ft_battles from it, and write outputs.

    Data-quality gates run before anything is written: first on the rich battle
    metadata, then on the final result-enriched table. A regression or missing
    annotation therefore fails loudly instead of overwriting processed data.

    Returns
    -------
    (battles_path, emcees_path)
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    if candidate is None:
        if pipeline_run is None:
            pipeline_run = build_pipeline_run(raw_dir=raw_dir)
        candidate = build_candidate_artifacts(pipeline_run)

    print(
        f"[validate] metadata: "
        f"{summarize_battle_metadata(candidate.pipeline_run.battle_metadata)}"
    )
    print(f"[validate] final: {summarize_ft_battles(candidate.ft_battles)}")

    if validate:
        require_releasable(candidate)

    battles_path, participants_path, emcees_path = publish_candidate_bundle(
        candidate,
        processed_dir,
    )
    print(f"[build] wrote {len(candidate.ft_battles)} battles -> {battles_path}")
    print(
        f"[build] wrote {len(candidate.participants)} participant rows "
        f"-> {participants_path}"
    )
    print(f"[build] wrote emcees table -> {emcees_path}")
    print(f"[build] wrote release manifest -> {processed_dir / 'release_manifest.json'}")

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
        "--events-since",
        type=int,
        default=None,
        metavar="YEAR",
        help="Incremental events scrape (with --fetch): only scrape FlipTop "
             "events from YEAR onward and MERGE them into the existing CSV, "
             "skipping event pages already recorded. Fast and safe for recent "
             "updates; overrides --start. Run a plain --fetch periodically for a "
             "full reconcile.",
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
    audit_group = parser.add_mutually_exclusive_group()
    audit_group.add_argument(
        "--audit",
        dest="audit",
        action="store_true",
        help=(
            "Write debug audit files (filtered_out.csv, upload_lineage.csv, "
            "manual_matchup_needed.csv, pipeline_summary.csv, "
            "pipeline_stage_drops.csv). This is the default; the flag is kept "
            "for compatibility."
        ),
    )
    audit_group.add_argument(
        "--no-audit",
        dest="audit",
        action="store_false",
        help="Skip writing data/debug audit files.",
    )
    parser.set_defaults(audit=True)
    parser.add_argument(
        "--debug-dir",
        type=Path,
        default=DEBUG_DATA_DIR,
        help="Directory to write debug audit outputs into.",
    )

    args = parser.parse_args(argv)

    if args.fetch:
        incremental = args.events_since is not None
        fetch_raw(
            args.raw_dir,
            channel=args.channel,
            start_year=args.events_since if incremental else args.start,
            end_year=args.end,
            merge_events=incremental,
            skip_known_events=incremental,
        )

    inputs = load_pipeline_inputs(args.raw_dir)
    pipeline_run = build_pipeline_run(inputs=inputs)
    candidate = build_candidate_artifacts(pipeline_run)
    print(
        f"[candidate] built {len(candidate.ft_battles)} battles; "
        f"missing_results={len(candidate.missing_results)}; "
        f"review_uploads={len(pipeline_run.review_uploads)}"
    )

    if args.audit:
        (
            excluded_path,
            lineage_path,
            manual_path,
            summary_path,
            drops_path,
        ) = write_audit_outputs(
            raw_dir=args.raw_dir,
            debug_dir=args.debug_dir,
            pipeline_run=pipeline_run,
        )
        print(f"[audit] wrote filtered uploads -> {excluded_path}")
        print(f"[audit] wrote upload lineage -> {lineage_path}")
        print(f"[audit] wrote manual matchup queue -> {manual_path}")
        print(f"[audit] wrote pipeline summary -> {summary_path}")
        print(f"[audit] wrote pipeline stage drops -> {drops_path}")

        missing_path, blockers_path = write_candidate_review_outputs(
            candidate,
            args.debug_dir,
        )
        print(f"[review] wrote missing results queue -> {missing_path}")
        print(f"[review] wrote release blockers -> {blockers_path}")

        manifest_path = write_run_manifest(
            candidate,
            args.debug_dir,
            release_status="blocked" if candidate.release_problems else "ready",
        )
        changes_path, changes_summary_path = write_release_change_report(
            candidate,
            args.processed_dir,
            args.debug_dir,
        )
        print(f"[review] wrote run manifest -> {manifest_path}")
        print(f"[review] wrote release changes -> {changes_path}")
        print(f"[review] wrote release change summary -> {changes_summary_path}")

    try:
        battles_path, emcees_path = rebuild_processed(
            raw_dir=args.raw_dir,
            processed_dir=args.processed_dir,
            pipeline_run=pipeline_run,
            candidate=candidate,
        )
    except ReleaseBlockedError as exc:
        print(f"[release] blocked:\n{exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    if args.audit:
        write_run_manifest(
            candidate,
            args.debug_dir,
            release_status="published",
            published_files=[
                battles_path,
                args.processed_dir / "battle_participants.csv",
                emcees_path,
                args.processed_dir / "release_manifest.json",
            ],
        )

    print("[release] processed outputs updated.")
    print("[done] refresh complete.")


if __name__ == "__main__":
    main()
