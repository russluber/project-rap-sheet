"""Tests for candidate construction and release blockers."""

from dataclasses import replace

import pandas as pd
import pytest

import fliptop
from fliptop import RAW_DATA_DIR
from fliptop.annotations import RESULTS_COLUMNS, load_results
from fliptop.pipeline import build_pipeline_run
from fliptop.release import (
    CandidateArtifacts,
    ReleaseBlockedError,
    build_candidate_artifacts,
    require_releasable,
    write_candidate_review_outputs,
)


def test_package_exports_candidate_api():
    assert fliptop.CandidateArtifacts is CandidateArtifacts
    assert fliptop.build_candidate_artifacts is build_candidate_artifacts


def test_complete_current_candidate_is_releasable():
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = build_candidate_artifacts(run)

    assert candidate.releasable
    assert candidate.release_problems == []
    assert candidate.missing_results.empty
    assert len(candidate.ft_battles) == len(run.battle_metadata)


def test_missing_results_build_candidate_but_block_release():
    run = build_pipeline_run(RAW_DATA_DIR)
    empty_results = pd.DataFrame(columns=RESULTS_COLUMNS)
    candidate = build_candidate_artifacts(run, results=empty_results)

    assert len(candidate.ft_battles) == len(run.battle_metadata)
    assert len(candidate.missing_results) == len(run.battle_metadata)
    assert not candidate.releasable
    assert any("missing results" in problem for problem in candidate.release_problems)


def test_manual_review_rows_block_release():
    run = build_pipeline_run(RAW_DATA_DIR)
    review = pd.DataFrame(
        [{"id": "needs-review", "pipeline_status": "needs_upload_review"}]
    )
    candidate = build_candidate_artifacts(
        replace(run, review_uploads=review),
        results=load_results(),
    )

    assert not candidate.releasable
    assert any("needs_upload_review" in problem for problem in candidate.release_problems)


def test_release_gate_raises_with_all_blockers():
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = build_candidate_artifacts(
        run,
        results=pd.DataFrame(columns=RESULTS_COLUMNS),
    )

    with pytest.raises(ReleaseBlockedError, match="processed outputs were not changed"):
        require_releasable(candidate)


def test_candidate_review_outputs_are_written_before_release(tmp_path):
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = build_candidate_artifacts(
        run,
        results=pd.DataFrame(columns=RESULTS_COLUMNS),
    )

    missing_path, blockers_path = write_candidate_review_outputs(candidate, tmp_path)

    assert len(pd.read_csv(missing_path)) == len(run.battle_metadata)
    assert "missing results" in blockers_path.read_text(encoding="utf-8")
