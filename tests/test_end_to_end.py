"""
End-to-end tests that build df_battles from the committed raw data.

These assert *invariants* that must always hold, not exact row counts (which
grow as new battles are uploaded). They are the smoke test that lets us
regenerate df_battles.json with confidence that nothing silently shifted.
"""

from __future__ import annotations

import pandas as pd

from fliptop.battles import write_df_battles

EXPECTED_COLUMNS = [
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


def test_no_null_emcees(df_battles):
    assert df_battles["emcee1"].notna().all()
    assert df_battles["emcee2"].notna().all()


def test_id_and_url_are_lists_iff_multipart(df_battles):
    # id and url are lists for consolidated multi-part battles and scalars
    # otherwise; both columns must agree on which rows are multi-part.
    id_is_list = df_battles["id"].apply(lambda x: isinstance(x, list))
    url_is_list = df_battles["url"].apply(lambda x: isinstance(x, list))
    assert id_is_list.equals(url_is_list)


def test_scalar_ids_are_unique(df_battles):
    scalar_ids = df_battles.loc[
        df_battles["id"].apply(lambda x: isinstance(x, str)), "id"
    ]
    assert not scalar_ids.duplicated().any()


def test_covid_window_dates_masked_before_imputation(raw_data_dir):
    # With the VerseTracker imputation disabled, attach_event_metadata's COVID
    # mask still clears every in-window event_date (the obfuscated-date period).
    from fliptop.battles import build_df_battles

    masked = build_df_battles(raw_dir=raw_data_dir, vt_event_dates={})
    in_window = masked["upload_date"].between(COVID_START, COVID_END)
    assert in_window.any()
    assert masked.loc[in_window, "event_date"].isna().all()


def test_versetracker_imputation_fills_all_event_dates(df_battles):
    # The default build imputes the COVID-masked dates from VerseTracker; every
    # quarantine event is covered, so no battle is left without an event_date.
    assert df_battles["event_date"].notna().all()


def test_event_date_source_is_tagged(df_battles):
    # Every dated battle carries a provenance tag from a known vocabulary, and
    # the COVID imputation + the hand-pin override both leave their mark.
    src = df_battles["event_date_source"]
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
