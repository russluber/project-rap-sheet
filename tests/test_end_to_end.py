"""
End-to-end tests that build df_battles from the committed raw data.

These assert *invariants* that must always hold, not exact row counts (which
grow as new battles are uploaded). They are the smoke test that lets us
regenerate df_battles.json with confidence that nothing silently shifted.
"""

from __future__ import annotations

import pandas as pd

from fliptop import annotations as ann
from fliptop.battles import write_df_battles

EXPECTED_COLUMNS = [
    "id",
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

METADATA_COLUMNS = [
    "id",
    "title",
    "description",
    "upload_date",
    "duration_seconds",
    "duration_hms",
    "emcee1",
    "emcee2",
    "matchup",
    "event_name",
    "event_date",
    "event_date_source",
    "event_location",
    "url",
]

# COVID window used by attach_event_metadata to clear obfuscated event dates.
COVID_START = pd.Timestamp("2020-05-01")
COVID_END = pd.Timestamp("2022-04-27")


def test_columns_present_and_ordered(df_battles):
    assert list(df_battles.columns) == EXPECTED_COLUMNS


def test_metadata_columns_present_and_ordered(battle_metadata):
    assert list(battle_metadata.columns) == METADATA_COLUMNS


def test_no_null_emcees(df_battles):
    assert df_battles["emcee1"].notna().all()
    assert df_battles["emcee2"].notna().all()


def test_excluded_event_categories_are_absent(df_battles):
    excluded = df_battles["event_name"].astype("string").str.contains(
        r"Process of Illumination|tryout",
        case=False,
        na=False,
    )
    assert not excluded.any()


def test_final_ids_are_scalar(df_battles):
    assert not df_battles["id"].apply(lambda x: isinstance(x, list)).any()


def test_metadata_id_and_url_are_lists_iff_multipart(battle_metadata):
    # The rich metadata keeps source ids/urls as lists for multi-part uploads.
    id_is_list = battle_metadata["id"].apply(lambda x: isinstance(x, list))
    url_is_list = battle_metadata["url"].apply(lambda x: isinstance(x, list))
    assert id_is_list.equals(url_is_list)


def test_scalar_ids_are_unique(df_battles):
    assert not df_battles["id"].duplicated().any()


def test_all_annotations_reference_a_final_battle(df_battles):
    battle_keys = set(df_battles["id"])
    results = ann.load_results()
    assert set(results["id"]) <= battle_keys


def test_final_table_has_result_columns(df_battles):
    assert df_battles["battle_type"].notna().all()
    assert set(df_battles["battle_type"].unique()) <= {"judged", "promo"}
    assert df_battles["winner"].notna().all()


def test_known_draw_annotations_remain_valid():
    results = ann.load_results().set_index("id")

    for battle_id in ("5mYgPAwGqf8", "Vz5SzkRo5Fc"):
        row = results.loc[battle_id].to_dict()
        assert row["battle_type"] == "judged"
        assert row["winner"] == ann.NA
        assert ann.validate_result_row({"id": battle_id, **row}) == []


def test_covid_window_dates_masked_before_imputation(raw_data_dir):
    # With the VerseTracker imputation disabled, attach_event_metadata's COVID
    # mask still clears every in-window event_date (the obfuscated-date period).
    from fliptop.battles import build_battle_metadata

    masked = build_battle_metadata(raw_dir=raw_data_dir, vt_event_dates={})
    in_window = masked["upload_date"].between(COVID_START, COVID_END)
    assert in_window.any()
    assert masked.loc[in_window, "event_date"].isna().all()


def test_versetracker_imputation_fills_all_event_dates(battle_metadata):
    # The default build imputes the COVID-masked dates from VerseTracker; every
    # quarantine event is covered, so no battle is left without an event_date.
    assert battle_metadata["event_date"].notna().all()


def test_event_date_source_is_tagged(battle_metadata):
    # Every dated battle carries a provenance tag from a known vocabulary, and
    # the COVID imputation + the hand-pin override both leave their mark.
    src = battle_metadata["event_date_source"]
    assert src.notna().all()
    assert set(src.unique()) <= {"website", "description", "versetracker", "manual"}
    assert (src == "versetracker").sum() > 0  # COVID-era events were imputed
    assert (src == "manual").sum() >= 1       # the Nikki vs K-Ram hand-pin


def test_emcee1_and_emcee2_differ(df_battles):
    assert (df_battles["emcee1"] != df_battles["emcee2"]).all()


def test_write_and_reload_round_trips(df_battles, tmp_path):
    out_path = tmp_path / "df_battles.json"
    # Build + write straight to a temp dir; never touches data/processed.
    from fliptop import RAW_DATA_DIR

    written = write_df_battles(out_path=out_path, raw_dir=RAW_DATA_DIR, fmt="json")
    assert written.exists()

    reloaded = pd.read_json(written, lines=True)
    # Same number of battles and same columns as the in-memory build.
    assert len(reloaded) == len(df_battles)
    assert list(reloaded.columns) == EXPECTED_COLUMNS
