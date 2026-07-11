"""Tests for the explicit pipeline input snapshot."""

import fliptop
from fliptop import DATA_DIR, RAW_DATA_DIR
from fliptop.inputs import PipelineInputs, load_pipeline_inputs


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
