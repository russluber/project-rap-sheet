"""
Tests for fliptop.publish: the final ft_battles publishing layer.
"""

from __future__ import annotations

import pandas as pd

import fliptop
from fliptop.annotations import make_result_row
from fliptop.battles import METADATA_COLUMNS
from fliptop.contracts import FT_BATTLES
from fliptop.publish import (
    FINAL_COLUMNS,
    FINAL_OUTPUT_FORBIDDEN_COLUMNS,
    build_ft_battles,
    build_ft_battles_from_metadata,
)


def test_package_root_exports_publish_builders():
    assert fliptop.build_ft_battles is build_ft_battles
    assert fliptop.build_ft_battles_from_metadata is build_ft_battles_from_metadata


def test_final_schema_is_separate_from_metadata_schema():
    assert list(FT_BATTLES.columns) == FINAL_COLUMNS
    assert FINAL_COLUMNS[-1] == "url"
    assert set(FINAL_OUTPUT_FORBIDDEN_COLUMNS).isdisjoint(FINAL_COLUMNS)
    assert {"description", "duration_hms", "event_date_source"} <= set(METADATA_COLUMNS)
    assert {"battle_type", "winner", "votes_winner", "votes_loser"} <= set(FINAL_COLUMNS)


def test_build_ft_battles_from_metadata_publishes_scalar_final_table():
    metadata = pd.DataFrame(
        {
            "id": [["aaaaaaaaaaa", "bbbbbbbbbbb"]],
            "title": ["A vs B"],
            "description": ["rich metadata only"],
            "upload_date": pd.to_datetime(["2020-01-01"]),
            "duration_seconds": [120.0],
            "duration_hms": ["00:02:00"],
            "emcee1": ["A"],
            "emcee2": ["B"],
            "matchup": ["A vs B"],
            "event_name": ["Event 1"],
            "event_date": pd.to_datetime(["2020-01-01"]),
            "event_date_source": ["website"],
            "event_location": ["Manila"],
            "url": [["u1", "u2"]],
        },
        columns=METADATA_COLUMNS,
    )
    results = pd.DataFrame(
        [
            make_result_row(
                id="aaaaaaaaaaa",
                winner="A",
                votes_winner=5,
                votes_loser=0,
                votes_nv=0,
                votes_ot=0,
                overtime="no",
            )
        ]
    )

    out = build_ft_battles_from_metadata(metadata, results=results)

    assert list(out.columns) == FINAL_COLUMNS
    assert out.loc[0, "id"] == "aaaaaaaaaaa"
    assert out.loc[0, "winner"] == "A"
    assert out.columns[-1] == "url"
    assert "event_date_source" not in out.columns
