"""Tests for candidate construction and release blockers."""

import json
from dataclasses import replace

import pandas as pd
import pytest

import fliptop
from fliptop import RAW_DATA_DIR
from fliptop import release as release_mod
from fliptop.annotations import RESULTS_COLUMNS, load_results
from fliptop.pipeline import build_pipeline_run
from fliptop.release import (
    CandidateArtifacts,
    ReleaseBlockedError,
    build_candidate_artifacts,
    build_release_changes,
    build_run_manifest,
    publish_candidate_bundle,
    require_releasable,
    write_candidate_review_outputs,
    write_release_change_report,
    write_run_manifest,
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


def test_run_manifest_records_inputs_counts_and_status(tmp_path):
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = build_candidate_artifacts(run)

    manifest = build_run_manifest(candidate, release_status="ready")
    path = write_run_manifest(candidate, tmp_path, release_status="ready")
    written = json.loads(path.read_text(encoding="utf-8"))

    assert manifest["counts"]["candidate_battles"] == len(candidate.ft_battles)
    assert manifest["contract_versions"]["raw.youtube_uploads"] == 1
    assert manifest["input_sha256"]
    assert written["release_status"] == "ready"
    assert written["git_commit"]


def test_release_change_report_detects_added_removed_and_changed(tmp_path):
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = build_candidate_artifacts(run)
    current = candidate.ft_battles.iloc[:2].copy()
    removed = current.iloc[[0]].copy()
    removed.loc[:, "id"] = "removed-id"
    current = pd.concat([current, removed], ignore_index=True)
    current.loc[0, "event_location"] = "Old Location"
    current.to_json(
        tmp_path / "ft_battles.json",
        orient="records",
        lines=True,
        date_format="epoch",
        date_unit="ms",
    )

    changes = build_release_changes(candidate, tmp_path)
    csv_path, summary_path = write_release_change_report(
        candidate,
        tmp_path,
        tmp_path / "debug",
    )

    assert {"battle_added", "battle_removed", "field_changed"} <= set(
        changes["change_type"]
    )
    assert csv_path.exists()
    assert "total_changes=" in summary_path.read_text(encoding="utf-8")


def test_candidate_bundle_restores_every_old_file_if_promotion_fails(
    tmp_path,
    monkeypatch,
):
    candidate = build_candidate_artifacts(build_pipeline_run(RAW_DATA_DIR))
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    old_contents = {
        "ft_battles.json": b"old battles\n",
        "battle_participants.csv": b"old participants\n",
        "emcees.csv": b"old emcees\n",
    }
    for filename, contents in old_contents.items():
        (processed_dir / filename).write_bytes(contents)

    real_replace = release_mod._replace_for_publish
    call_count = 0

    def fail_during_promotion(source, destination):
        nonlocal call_count
        call_count += 1
        if call_count == 5:
            raise OSError("forced promotion failure")
        real_replace(source, destination)

    monkeypatch.setattr(release_mod, "_replace_for_publish", fail_during_promotion)

    with pytest.raises(OSError, match="forced promotion failure"):
        publish_candidate_bundle(candidate, processed_dir)

    for filename, contents in old_contents.items():
        assert (processed_dir / filename).read_bytes() == contents
    assert not list(tmp_path.glob(".candidate-release-*"))
    assert not list(tmp_path.glob(".processed-backup-*"))
