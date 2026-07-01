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

    assert battles_path.exists()
    assert emcees_path.exists()
    assert battles_path.name == "df_battles.json"
    assert emcees_path.name == "emcees.csv"


def test_rebuild_outputs_are_consistent(tmp_path):
    battles_path, emcees_path = rebuild_processed(
        raw_dir=RAW_DATA_DIR, processed_dir=tmp_path
    )

    battles = pd.read_json(battles_path, lines=True)
    emcees = pd.read_csv(emcees_path)

    # Every emcee in the battles table appears in the emcees table, and the
    # emcees table is the de-duplicated union of emcee1/emcee2.
    names_in_battles = set(battles["emcee1"]) | set(battles["emcee2"])
    assert set(emcees["emcee_name"]) == names_in_battles
    assert emcees["emcee_id"].is_unique


def _bad_battles():
    # duplicate battle id + missing columns -> should fail the gate
    return pd.DataFrame({"id": ["dup", "dup"], "emcee1": ["A", "C"], "emcee2": ["B", "D"]})


def test_rebuild_refuses_to_write_on_validation_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(refresh_mod, "build_df_battles", lambda **kw: _bad_battles())

    with pytest.raises(ValueError, match="failed validation"):
        rebuild_processed(raw_dir=RAW_DATA_DIR, processed_dir=tmp_path)

    # nothing was written - the gate ran before save
    assert not (tmp_path / "df_battles.json").exists()
    assert not (tmp_path / "emcees.csv").exists()


def test_rebuild_validate_false_bypasses_gate(tmp_path, monkeypatch):
    monkeypatch.setattr(refresh_mod, "build_df_battles", lambda **kw: _bad_battles())

    battles_path, emcees_path = rebuild_processed(
        raw_dir=RAW_DATA_DIR, processed_dir=tmp_path, validate=False
    )
    assert battles_path.exists()
    assert emcees_path.exists()
