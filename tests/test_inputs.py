"""Tests for the explicit pipeline input snapshot."""

import fliptop
from fliptop import DATA_DIR, RAW_DATA_DIR
from fliptop.inputs import PipelineInputs, load_pipeline_inputs
from fliptop.pipeline import build_pipeline_run
from fliptop.release import build_candidate_artifacts


def test_package_exports_pipeline_inputs_api():
    assert fliptop.PipelineInputs is PipelineInputs
    assert fliptop.load_pipeline_inputs is load_pipeline_inputs


def test_load_pipeline_inputs_captures_every_file_backed_dependency():
    inputs = load_pipeline_inputs(RAW_DATA_DIR)

    assert not inputs.raw_uploads.empty
    assert not inputs.raw_events.empty
    assert inputs.rename_map
    assert inputs.manual_matchups
    assert inputs.title_exclusion_rules
    assert inputs.event_exclusion_rules
    assert not inputs.results.empty
    assert inputs.data_dir == DATA_DIR
    assert {path.name for path in inputs.source_files} >= {
        "youtube_videos.json",
        "matchup_events_metadata.csv",
        "battle_results.csv",
        "title_exclusions.csv",
    }


def test_explicit_empty_overrides_are_not_reloaded():
    inputs = load_pipeline_inputs(
        RAW_DATA_DIR,
        rename_map={},
        manual_matchups={},
        upload_decisions={},
        vt_event_dates={},
    )

    assert inputs.rename_map == {}
    assert inputs.manual_matchups == {}
    assert inputs.upload_decisions == {}
    assert inputs.vt_event_dates == {}
    assert {path.name for path in inputs.source_files}.isdisjoint(
        {
            "emcee_aliases.csv",
            "manual_matchups.csv",
            "upload_decisions.csv",
            "versetracker_event_dates.csv",
        }
    )


def test_loaded_snapshot_needs_no_hidden_file_reads(monkeypatch):
    inputs = load_pipeline_inputs(RAW_DATA_DIR)

    def unexpected_read(*args, **kwargs):
        raise AssertionError("pipeline attempted a hidden file read")

    monkeypatch.setattr("fliptop.events.load_location_aliases", unexpected_read)
    monkeypatch.setattr("fliptop.events.load_event_location_overrides", unexpected_read)
    monkeypatch.setattr("fliptop.events.load_event_location_patterns", unexpected_read)
    monkeypatch.setattr("fliptop.events.load_event_date_overrides", unexpected_read)
    monkeypatch.setattr("fliptop.uploads.load_title_exclusion_rules", unexpected_read)
    monkeypatch.setattr("fliptop.structures.load_manual_matchups", unexpected_read)
    monkeypatch.setattr("fliptop.structures.load_rename_map", unexpected_read)
    monkeypatch.setattr("fliptop.annotations.load_results", unexpected_read)

    run = build_pipeline_run(inputs=inputs)
    candidate = build_candidate_artifacts(run)

    assert candidate.releasable
