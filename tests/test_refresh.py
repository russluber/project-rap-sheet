"""
Tests for the refresh orchestration (rebuild stage only).

The --fetch stage hits the network and is not exercised here; we test that
rebuild_processed writes both processed outputs from the committed raw data
into an isolated temp directory.
"""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pandas as pd
import pytest

from fliptop import PROCESSED_DATA_DIR, RAW_DATA_DIR
from fliptop import refresh as refresh_mod
from fliptop.pipeline import build_pipeline_run
from fliptop.refresh import rebuild_processed
from fliptop.release import ReleaseBlockedError, build_candidate_artifacts


def test_rebuild_processed_writes_both_outputs(tmp_path):
    battles_path, emcees_path = rebuild_processed(
        raw_dir=RAW_DATA_DIR, processed_dir=tmp_path
    )
    participants_path = tmp_path / "battle_participants.csv"

    assert battles_path.exists()
    assert emcees_path.exists()
    assert participants_path.exists()
    assert battles_path.name == "ft_battles.json"
    assert emcees_path.name == "emcees.csv"
    assert participants_path.name == "battle_participants.csv"


def test_rebuild_outputs_are_consistent(tmp_path):
    battles_path, emcees_path = rebuild_processed(
        raw_dir=RAW_DATA_DIR, processed_dir=tmp_path
    )

    battles = pd.read_json(battles_path, lines=True)
    emcees = pd.read_csv(emcees_path)
    participants = pd.read_csv(tmp_path / "battle_participants.csv")

    assert {"battle_type", "winner", "votes_winner", "votes_loser"} <= set(battles.columns)
    # Every emcee in the battles and participant table appears in emcees.csv.
    names_in_battles = (
        set(battles["emcee1"])
        | set(battles["emcee2"])
        | set(participants["emcee"])
    )
    assert set(emcees["emcee_name"]) == names_in_battles
    assert emcees["emcee_id"].is_unique


def test_committed_processed_outputs_match_current_pipeline(tmp_path):
    """Data-only commits must include exactly regenerated processed outputs."""
    rebuild_processed(raw_dir=RAW_DATA_DIR, processed_dir=tmp_path)

    for filename in ("ft_battles.json", "battle_participants.csv", "emcees.csv"):
        generated = (tmp_path / filename).read_bytes()
        committed = (PROCESSED_DATA_DIR / filename).read_bytes()
        assert generated == committed, (
            f"data/processed/{filename} is stale; run `uv run fliptop-refresh` "
            "and commit the regenerated output"
        )


def _bad_battles():
    # duplicate battle id + missing columns -> should fail the gate
    return pd.DataFrame({"id": ["dup", "dup"], "emcee1": ["A", "C"], "emcee2": ["B", "D"]})


def test_rebuild_refuses_to_write_on_validation_failure(tmp_path, monkeypatch):
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = replace(
        build_candidate_artifacts(run),
        metadata_problems=["forced validation failure"],
    )

    with pytest.raises(ReleaseBlockedError, match="release gate"):
        rebuild_processed(
            raw_dir=RAW_DATA_DIR,
            processed_dir=tmp_path,
            candidate=candidate,
        )

    # nothing was written - the gate ran before save
    assert not (tmp_path / "ft_battles.json").exists()
    assert not (tmp_path / "emcees.csv").exists()
    assert not (tmp_path / "battle_participants.csv").exists()


def test_rebuild_validate_false_bypasses_gate(tmp_path, monkeypatch):
    run = build_pipeline_run(RAW_DATA_DIR)
    candidate = replace(
        build_candidate_artifacts(run),
        metadata_problems=["forced validation failure"],
    )

    battles_path, emcees_path = rebuild_processed(
        raw_dir=RAW_DATA_DIR,
        processed_dir=tmp_path,
        validate=False,
        candidate=candidate,
    )
    assert battles_path.exists()
    assert emcees_path.exists()
    assert (tmp_path / "battle_participants.csv").exists()


def test_main_writes_audit_outputs_by_default(tmp_path, monkeypatch):
    calls = []
    pipeline_run = SimpleNamespace(review_uploads=pd.DataFrame())
    candidate = SimpleNamespace(
        ft_battles=pd.DataFrame({"id": ["battle"]}),
        missing_results=pd.DataFrame(),
        release_problems=[],
    )

    def fake_rebuild_processed(**kwargs):
        calls.append(("rebuild", kwargs))
        return tmp_path / "ft_battles.json", tmp_path / "emcees.csv"

    def fake_write_audit_outputs(**kwargs):
        calls.append(("audit", kwargs))
        return (
            tmp_path / "filtered_out.csv",
            tmp_path / "upload_lineage.csv",
            tmp_path / "manual_matchup_needed.csv",
            tmp_path / "pipeline_summary.csv",
            tmp_path / "pipeline_stage_drops.csv",
        )

    monkeypatch.setattr(refresh_mod, "rebuild_processed", fake_rebuild_processed)
    monkeypatch.setattr(refresh_mod, "write_audit_outputs", fake_write_audit_outputs)
    monkeypatch.setattr(
        refresh_mod,
        "build_candidate_artifacts",
        lambda run: calls.append(("candidate", run)) or candidate,
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_candidate_review_outputs",
        lambda *args: calls.append(("review", args))
        or (tmp_path / "missing_results.csv", tmp_path / "release_blockers.txt"),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_run_manifest",
        lambda *args, **kwargs: calls.append((f"manifest-{kwargs['release_status']}", args))
        or tmp_path / "run_manifest.json",
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_release_change_report",
        lambda *args: calls.append(("changes", args))
        or (tmp_path / "changes.csv", tmp_path / "changes.txt"),
    )
    monkeypatch.setattr(
        refresh_mod,
        "build_pipeline_run",
        lambda **kwargs: calls.append(("pipeline", kwargs)) or pipeline_run,
    )

    refresh_mod.main(["--processed-dir", str(tmp_path), "--debug-dir", str(tmp_path / "debug")])

    assert [name for name, _ in calls] == [
        "pipeline",
        "candidate",
        "audit",
        "review",
        "manifest-ready",
        "changes",
        "rebuild",
        "manifest-published",
    ]
    assert calls[2][1]["pipeline_run"] is pipeline_run
    assert calls[2][1]["debug_dir"] == tmp_path / "debug"
    assert calls[6][1]["candidate"] is candidate


def test_main_no_audit_skips_audit_outputs(tmp_path, monkeypatch):
    calls = []
    pipeline_run = SimpleNamespace(review_uploads=pd.DataFrame())
    candidate = SimpleNamespace(
        ft_battles=pd.DataFrame({"id": ["battle"]}),
        missing_results=pd.DataFrame(),
        release_problems=[],
    )

    monkeypatch.setattr(
        refresh_mod,
        "rebuild_processed",
        lambda **kwargs: calls.append(("rebuild", kwargs))
        or (tmp_path / "ft_battles.json", tmp_path / "emcees.csv"),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_audit_outputs",
        lambda **kwargs: calls.append(("audit", kwargs)),
    )
    monkeypatch.setattr(refresh_mod, "build_pipeline_run", lambda **kwargs: pipeline_run)
    monkeypatch.setattr(refresh_mod, "build_candidate_artifacts", lambda run: candidate)
    monkeypatch.setattr(
        refresh_mod,
        "write_candidate_review_outputs",
        lambda *args: calls.append(("review", args)),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_run_manifest",
        lambda *args, **kwargs: calls.append(("manifest", args)),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_release_change_report",
        lambda *args: calls.append(("changes", args)),
    )

    refresh_mod.main(["--processed-dir", str(tmp_path), "--no-audit"])

    assert [name for name, _ in calls] == ["rebuild"]
    assert calls[0][1]["pipeline_run"] is pipeline_run
    assert calls[0][1]["candidate"] is candidate


def test_main_audit_flag_remains_accepted(tmp_path, monkeypatch):
    calls = []
    pipeline_run = SimpleNamespace(review_uploads=pd.DataFrame())
    candidate = SimpleNamespace(
        ft_battles=pd.DataFrame({"id": ["battle"]}),
        missing_results=pd.DataFrame(),
        release_problems=[],
    )

    monkeypatch.setattr(
        refresh_mod,
        "rebuild_processed",
        lambda **kwargs: calls.append(("rebuild", kwargs))
        or (tmp_path / "ft_battles.json", tmp_path / "emcees.csv"),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_audit_outputs",
        lambda **kwargs: calls.append(("audit", kwargs))
        or (
            tmp_path / "filtered_out.csv",
            tmp_path / "upload_lineage.csv",
            tmp_path / "manual_matchup_needed.csv",
            tmp_path / "pipeline_summary.csv",
            tmp_path / "pipeline_stage_drops.csv",
        ),
    )
    monkeypatch.setattr(refresh_mod, "build_pipeline_run", lambda **kwargs: pipeline_run)
    monkeypatch.setattr(refresh_mod, "build_candidate_artifacts", lambda run: candidate)
    monkeypatch.setattr(
        refresh_mod,
        "write_candidate_review_outputs",
        lambda *args: calls.append(("review", args))
        or (tmp_path / "missing_results.csv", tmp_path / "release_blockers.txt"),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_run_manifest",
        lambda *args, **kwargs: calls.append((f"manifest-{kwargs['release_status']}", args))
        or tmp_path / "run_manifest.json",
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_release_change_report",
        lambda *args: calls.append(("changes", args))
        or (tmp_path / "changes.csv", tmp_path / "changes.txt"),
    )

    refresh_mod.main(["--processed-dir", str(tmp_path), "--audit"])

    assert [name for name, _ in calls] == [
        "audit",
        "review",
        "manifest-ready",
        "changes",
        "rebuild",
        "manifest-published",
    ]
    assert calls[0][1]["pipeline_run"] is pipeline_run
    assert calls[4][1]["pipeline_run"] is pipeline_run
    assert calls[4][1]["candidate"] is candidate


def test_main_writes_reviews_before_blocked_release(tmp_path, monkeypatch):
    calls = []
    pipeline_run = SimpleNamespace(review_uploads=pd.DataFrame())
    candidate = SimpleNamespace(
        ft_battles=pd.DataFrame({"id": ["battle"]}),
        missing_results=pd.DataFrame({"battle_key": ["battle"]}),
        release_problems=["results: missing"],
    )

    monkeypatch.setattr(refresh_mod, "build_pipeline_run", lambda **kwargs: pipeline_run)
    monkeypatch.setattr(refresh_mod, "build_candidate_artifacts", lambda run: candidate)
    monkeypatch.setattr(
        refresh_mod,
        "write_audit_outputs",
        lambda **kwargs: calls.append("audit")
        or tuple(tmp_path / f"audit-{i}.csv" for i in range(5)),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_candidate_review_outputs",
        lambda *args: calls.append("review")
        or (tmp_path / "missing.csv", tmp_path / "blockers.txt"),
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_run_manifest",
        lambda *args, **kwargs: calls.append(f"manifest-{kwargs['release_status']}")
        or tmp_path / "run_manifest.json",
    )
    monkeypatch.setattr(
        refresh_mod,
        "write_release_change_report",
        lambda *args: calls.append("changes")
        or (tmp_path / "changes.csv", tmp_path / "changes.txt"),
    )

    def block_release(**kwargs):
        calls.append("release")
        raise ReleaseBlockedError("blocked")

    monkeypatch.setattr(refresh_mod, "rebuild_processed", block_release)

    with pytest.raises(SystemExit) as exc:
        refresh_mod.main(
            ["--processed-dir", str(tmp_path), "--debug-dir", str(tmp_path / "debug")]
        )

    assert exc.value.code == 2
    assert calls == ["audit", "review", "manifest-blocked", "changes", "release"]
