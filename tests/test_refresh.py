"""
Tests for the refresh orchestration (rebuild stage only).

The --fetch stage hits the network and is not exercised here; we test that
rebuild_processed writes both processed outputs from the committed raw data
into an isolated temp directory.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fliptop import RAW_DATA_DIR
from fliptop import refresh as refresh_mod
from fliptop.refresh import rebuild_processed


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


def _bad_battles():
    # duplicate battle id + missing columns -> should fail the gate
    return pd.DataFrame({"id": ["dup", "dup"], "emcee1": ["A", "C"], "emcee2": ["B", "D"]})


def test_rebuild_refuses_to_write_on_validation_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(refresh_mod, "build_battle_metadata", lambda **kw: _bad_battles())

    with pytest.raises(ValueError, match="failed validation"):
        rebuild_processed(raw_dir=RAW_DATA_DIR, processed_dir=tmp_path)

    # nothing was written - the gate ran before save
    assert not (tmp_path / "ft_battles.json").exists()
    assert not (tmp_path / "emcees.csv").exists()
    assert not (tmp_path / "battle_participants.csv").exists()


def test_rebuild_validate_false_bypasses_gate(tmp_path, monkeypatch):
    monkeypatch.setattr(refresh_mod, "build_battle_metadata", lambda **kw: pd.DataFrame())
    monkeypatch.setattr(
        refresh_mod,
        "build_ft_battles_from_metadata",
        lambda *args, **kwargs: _bad_battles(),
    )

    battles_path, emcees_path = rebuild_processed(
        raw_dir=RAW_DATA_DIR, processed_dir=tmp_path, validate=False
    )
    assert battles_path.exists()
    assert emcees_path.exists()
    assert (tmp_path / "battle_participants.csv").exists()


def test_main_writes_audit_outputs_by_default(tmp_path, monkeypatch):
    calls = []

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

    refresh_mod.main(["--processed-dir", str(tmp_path), "--debug-dir", str(tmp_path / "debug")])

    assert [name for name, _ in calls] == ["rebuild", "audit"]
    assert calls[1][1]["debug_dir"] == tmp_path / "debug"


def test_main_no_audit_skips_audit_outputs(tmp_path, monkeypatch):
    calls = []

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

    refresh_mod.main(["--processed-dir", str(tmp_path), "--no-audit"])

    assert [name for name, _ in calls] == ["rebuild"]


def test_main_audit_flag_remains_accepted(tmp_path, monkeypatch):
    calls = []

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

    refresh_mod.main(["--processed-dir", str(tmp_path), "--audit"])

    assert [name for name, _ in calls] == ["rebuild", "audit"]
