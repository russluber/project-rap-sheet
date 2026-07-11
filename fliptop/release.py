"""Candidate construction and release-quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .annotations import load_results, pending_battles, validate_results_store
from .io import atomic_output_path
from .pipeline import PipelineRun
from .publish import build_ft_battles_from_metadata
from .structures import build_battle_participants, build_emcees_table
from .validate import validate_battle_metadata, validate_ft_battles

PathLike = str | Path


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
