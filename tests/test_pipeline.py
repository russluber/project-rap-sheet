"""Tests for the shared single-execution pipeline result."""

import pandas as pd
import pytest

import fliptop
from fliptop import RAW_DATA_DIR
from fliptop import pipeline as pipeline_mod
from fliptop.battles import build_battle_metadata
from fliptop.contracts import ContractViolation
from fliptop.pipeline import PipelineRun, build_pipeline_run


def test_package_exports_pipeline_run_api():
    assert fliptop.PipelineRun is PipelineRun
    assert fliptop.build_pipeline_run is build_pipeline_run


def test_pipeline_run_carries_stages_decisions_and_metadata():
    run = build_pipeline_run(RAW_DATA_DIR)

    assert not run.raw_uploads.empty
    assert not run.battle_metadata.empty
    assert set(run.excluded_uploads["id"]).isdisjoint(run.battle_metadata["id"].astype(str))
    assert {
        "raw_youtube",
        "drop_excluded_events",
        "parse_and_canonicalize_matchups",
        "finalize_battle_metadata",
    } <= set(run.stages)


def test_build_battle_metadata_is_pipeline_run_compatibility_view():
    run = build_pipeline_run(RAW_DATA_DIR)
    metadata = build_battle_metadata(RAW_DATA_DIR)

    pd.testing.assert_frame_equal(metadata, run.battle_metadata)


def test_pipeline_names_the_stage_that_broke(monkeypatch):
    original = pipeline_mod.prepare_uploads

    def drop_required_id(frame):
        return original(frame).drop(columns="id")

    monkeypatch.setattr(pipeline_mod, "prepare_uploads", drop_required_id)

    with pytest.raises(
        ContractViolation,
        match="pipeline stage prepare_uploads.*prepared uploads contract failed",
    ):
        build_pipeline_run(RAW_DATA_DIR)
