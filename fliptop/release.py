"""Candidate construction and release-quality checks."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from . import DATA_DIR, PROJECT_ROOT
from .annotations import load_results, pending_battles, validate_results_store
from .io import atomic_output_path
from .pipeline import PipelineRun
from .publish import build_ft_battles_from_metadata
from .structures import build_battle_participants, build_emcees_table
from .validate import validate_battle_metadata, validate_ft_battles

PathLike = str | Path

CHANGE_COLUMNS = ["change_type", "id", "field", "old_value", "new_value"]
COMPARED_BATTLE_FIELDS = [
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


class ReleaseBlockedError(ValueError):
    """Raised when candidate artifacts fail the official release gate."""

MISSING_RESULTS_COLUMNS = [
    "battle_key",
    "title",
    "matchup",
    "event_name",
    "event_date",
    "upload_date",
    "emcee1",
    "emcee2",
    "url",
]


@dataclass
class CandidateArtifacts:
    """All derived tables and blockers produced before an official release."""

    pipeline_run: PipelineRun
    results: pd.DataFrame
    ft_battles: pd.DataFrame
    participants: pd.DataFrame
    emcees: pd.DataFrame
    missing_results: pd.DataFrame
    metadata_problems: list[str]
    results_problems: list[str]
    final_problems: list[str]

    @property
    def release_problems(self) -> list[str]:
        """Human-readable reasons this candidate cannot be released."""
        problems = [
            *(f"metadata: {problem}" for problem in self.metadata_problems),
            *(f"results: {problem}" for problem in self.results_problems),
            *(f"final: {problem}" for problem in self.final_problems),
        ]

        reviews = self.pipeline_run.review_uploads
        if not reviews.empty:
            if "pipeline_status" in reviews.columns:
                counts = reviews["pipeline_status"].value_counts()
                for status, count in counts.items():
                    problems.append(f"review: {count} upload(s) have status {status!r}")
            else:
                problems.append(f"review: {len(reviews)} upload(s) need manual review")
        return problems

    @property
    def releasable(self) -> bool:
        return not self.release_problems


def build_candidate_artifacts(
    pipeline_run: PipelineRun,
    results: pd.DataFrame | None = None,
) -> CandidateArtifacts:
    """Build candidate tables and collect, rather than raise on, release blockers."""
    if results is None:
        results = load_results()

    battle_metadata = pipeline_run.battle_metadata
    ft_battles = build_ft_battles_from_metadata(
        battle_metadata,
        results=results,
        require_results=False,
    )
    participants = build_battle_participants(ft_battles)
    emcees = build_emcees_table(ft_battles, participants=participants)

    missing_results = pending_battles(battle_metadata, results)
    missing_results = missing_results[
        [column for column in MISSING_RESULTS_COLUMNS if column in missing_results.columns]
    ]

    return CandidateArtifacts(
        pipeline_run=pipeline_run,
        results=results,
        ft_battles=ft_battles,
        participants=participants,
        emcees=emcees,
        missing_results=missing_results,
        metadata_problems=validate_battle_metadata(battle_metadata),
        results_problems=validate_results_store(
            results,
            battle_metadata,
            require_complete=True,
        ),
        final_problems=validate_ft_battles(ft_battles),
    )


def require_releasable(candidate: CandidateArtifacts) -> None:
    """Raise with all blockers when a candidate cannot be officially released."""
    problems = candidate.release_problems
    if not problems:
        return
    raise ReleaseBlockedError(
        "candidate failed the release gate; processed outputs were not changed:\n"
        + "\n".join(f"  - {problem}" for problem in problems)
    )


def write_candidate_review_outputs(
    candidate: CandidateArtifacts,
    debug_dir: PathLike,
) -> tuple[Path, Path]:
    """Write missing-result and release-blocker queues before release is attempted."""
    debug_dir = Path(debug_dir)
    debug_dir.mkdir(parents=True, exist_ok=True)
    missing_path = debug_dir / "missing_results.csv"
    blockers_path = debug_dir / "release_blockers.txt"

    with atomic_output_path(missing_path) as temporary:
        candidate.missing_results.to_csv(temporary, index=False)

    blocker_lines = candidate.release_problems or ["none"]
    with atomic_output_path(blockers_path) as temporary:
        temporary.write_text("\n".join(blocker_lines) + "\n", encoding="utf-8")

    return missing_path, blockers_path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_input_files(candidate: CandidateArtifacts) -> list[Path]:
    roots = [
        candidate.pipeline_run.raw_dir,
        DATA_DIR / "rules",
        DATA_DIR / "overrides",
        DATA_DIR / "annotations",
    ]
    files = [DATA_DIR / "emcee_aliases.csv"]
    for root in roots:
        if root.exists():
            files.extend(path for path in root.rglob("*") if path.is_file())
    return sorted(set(files))


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def build_run_manifest(
    candidate: CandidateArtifacts,
    *,
    release_status: str,
    published_files: list[Path] | None = None,
) -> dict[str, object]:
    """Build a JSON-serializable record of inputs, counts, blockers, and status."""
    inputs = _manifest_input_files(candidate)
    run = candidate.pipeline_run
    return {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "git_commit": _git_commit(),
        "release_status": release_status,
        "input_sha256": {_display_path(path): _sha256(path) for path in inputs},
        "counts": {
            "raw_uploads": len(run.raw_uploads),
            "raw_events": len(run.raw_events),
            "candidate_battles": len(candidate.ft_battles),
            "excluded_uploads": len(run.excluded_uploads),
            "review_uploads": len(run.review_uploads),
            "missing_results": len(candidate.missing_results),
            "participants": len(candidate.participants),
            "emcees": len(candidate.emcees),
        },
        "release_problems": candidate.release_problems,
        "published_files": [str(path) for path in (published_files or [])],
    }


def write_run_manifest(
    candidate: CandidateArtifacts,
    debug_dir: PathLike,
    *,
    release_status: str,
    published_files: list[Path] | None = None,
) -> Path:
    """Atomically write the current run manifest."""
    path = Path(debug_dir) / "run_manifest.json"
    manifest = build_run_manifest(
        candidate,
        release_status=release_status,
        published_files=published_files,
    )
    with atomic_output_path(path) as temporary:
        temporary.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return path


def _display_value(value) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return ""
    return str(value)


def build_release_changes(
    candidate: CandidateArtifacts,
    processed_dir: PathLike,
) -> pd.DataFrame:
    """Return added, removed, and field-level changes versus the current release."""
    current_path = Path(processed_dir) / "ft_battles.json"
    new = candidate.ft_battles.copy()
    new["id"] = new["id"].astype(str)

    if current_path.exists():
        current = pd.read_json(
            current_path,
            lines=True,
            convert_dates=["upload_date", "event_date"],
        )
        current["id"] = current["id"].astype(str)
    else:
        current = pd.DataFrame(columns=new.columns)

    current_by_id = current.set_index("id", drop=False)
    new_by_id = new.set_index("id", drop=False)
    current_ids = set(current_by_id.index)
    new_ids = set(new_by_id.index)
    rows: list[dict[str, str]] = []

    for battle_id in sorted(new_ids - current_ids):
        rows.append(
            {
                "change_type": "battle_added",
                "id": battle_id,
                "field": "",
                "old_value": "",
                "new_value": _display_value(new_by_id.loc[battle_id, "matchup"]),
            }
        )
    for battle_id in sorted(current_ids - new_ids):
        rows.append(
            {
                "change_type": "battle_removed",
                "id": battle_id,
                "field": "",
                "old_value": _display_value(current_by_id.loc[battle_id, "matchup"]),
                "new_value": "",
            }
        )

    shared_fields = [
        field
        for field in COMPARED_BATTLE_FIELDS
        if field in current.columns and field in new.columns
    ]
    for battle_id in sorted(current_ids & new_ids):
        for field in shared_fields:
            old_value = _display_value(current_by_id.loc[battle_id, field])
            new_value = _display_value(new_by_id.loc[battle_id, field])
            if old_value != new_value:
                rows.append(
                    {
                        "change_type": "field_changed",
                        "id": battle_id,
                        "field": field,
                        "old_value": old_value,
                        "new_value": new_value,
                    }
                )

    return pd.DataFrame(rows, columns=CHANGE_COLUMNS)


def write_release_change_report(
    candidate: CandidateArtifacts,
    processed_dir: PathLike,
    debug_dir: PathLike,
) -> tuple[Path, Path]:
    """Write detailed CSV changes plus a compact count summary."""
    changes = build_release_changes(candidate, processed_dir)
    debug_dir = Path(debug_dir)
    csv_path = debug_dir / "release_changes.csv"
    summary_path = debug_dir / "release_changes_summary.txt"

    with atomic_output_path(csv_path) as temporary:
        changes.to_csv(temporary, index=False)

    counts = changes["change_type"].value_counts().to_dict()
    lines = [f"total_changes={len(changes)}"]
    lines.extend(f"{kind}={count}" for kind, count in sorted(counts.items()))
    with atomic_output_path(summary_path) as temporary:
        temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return csv_path, summary_path
