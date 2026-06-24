"""
Tests for consolidate_battle_parts: the trickiest transform in the pipeline.

Multi-part uploads ("pt. 1", "pt. 2", ...) are collapsed into a single row
whose ``id`` and ``url`` become ordered lists, ``duration_seconds`` is summed,
``upload_date`` is the earliest part, and ``duration_hms`` is recomputed.
Single-part battles pass through unchanged (scalar ``id`` / ``url``).
"""

from __future__ import annotations

import pandas as pd

from fliptop.data_cleaning import consolidate_battle_parts


def _frame():
    return pd.DataFrame(
        {
            "yt_raw_title": [
                "FlipTop - Dello vs Batas pt. 1",
                "FlipTop - Dello vs Batas pt. 2",
                "FlipTop - A vs B",
            ],
            "title": ["FlipTop - Dello vs Batas", "FlipTop - Dello vs Batas", "FlipTop - A vs B"],
            "id": ["id1", "id2", "id3"],
            "url": ["u1", "u2", "u3"],
            "upload_date": pd.to_datetime(["2020-01-02", "2020-01-01", "2019-05-05"]),
            "duration_seconds": [100.0, 200.0, 50.0],
            "emcee1": ["Dello", "Dello", "A"],
            "emcee2": ["Batas", "Batas", "B"],
        }
    )


def test_consolidation_collapses_parts_into_one_row():
    out = consolidate_battle_parts(_frame())
    # 2 parts -> 1 row, plus the standalone battle = 2 rows total
    assert len(out) == 2


def test_consolidation_lists_ids_and_urls_in_part_order():
    out = consolidate_battle_parts(_frame())
    multi = out[out["id"].apply(lambda x: isinstance(x, list))].iloc[0]
    assert multi["id"] == ["id1", "id2"]
    assert multi["url"] == ["u1", "u2"]


def test_consolidation_sums_duration_and_takes_earliest_upload():
    out = consolidate_battle_parts(_frame())
    multi = out[out["id"].apply(lambda x: isinstance(x, list))].iloc[0]
    assert multi["duration_seconds"] == 300.0
    assert multi["upload_date"] == pd.Timestamp("2020-01-01")
    assert multi["duration_hms"] == "00:05:00"  # recomputed from summed seconds


def test_consolidation_passes_single_battles_through_as_scalars():
    out = consolidate_battle_parts(_frame())
    single = out[out["id"] == "id3"].iloc[0]
    assert single["url"] == "u3"
    assert single["duration_seconds"] == 50.0


def test_consolidation_without_parts_is_stable():
    df = pd.DataFrame(
        {
            "yt_raw_title": ["FlipTop - A vs B", "FlipTop - C vs D"],
            "id": ["id1", "id2"],
            "duration_seconds": [60.0, 120.0],
        }
    )
    out = consolidate_battle_parts(df)
    assert len(out) == 2
    assert set(out["id"]) == {"id1", "id2"}
