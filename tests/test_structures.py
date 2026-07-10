"""
Tests for the ft_battles-derived structures in fliptop.structures:

    build_battle_network
    build_battle_participants
    build_emcees_table
"""

from __future__ import annotations

import pandas as pd
import pytest

from fliptop.structures import (
    build_battle_network,
    build_battle_participants,
    build_emcees_table,
)


def _battles():
    return pd.DataFrame(
        {
            "emcee1": ["Loonie", "Loonie", "Abra", "Shehyee", None, "X"],
            "emcee2": ["Abra", "Abra", "Shehyee", "Loonie", "Y", "X"],
        }
    )


def _dated_battles():
    return pd.DataFrame(
        {
            "id": ["normal", "noshow"],
            "matchup": ["A vs B", "Makii vs Fongger"],
            "event_name": ["Event 1", "Gubat 3"],
            "event_date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "upload_date": pd.to_datetime(["2020-01-03", "2020-01-04"]),
            "emcee1": ["A", "Makii"],
            "emcee2": ["B", "Fongger"],
        }
    )


def _manual_no_show():
    return {
        "noshow": {
            "emcee1": "Makii",
            "emcee2": "Fongger",
            "helper_emcee": "Aelekz",
            "emcee1_status": "appeared",
            "emcee2_status": "no_show",
            "helper_status": "appeared",
            "note": "watched",
        }
    }


# ---------------------------------------------------------------------------
# build_battle_network
# ---------------------------------------------------------------------------

def test_network_edge_weight_counts_repeat_matchups():
    G = build_battle_network(_battles())
    # Loonie vs Abra appears twice -> weight 2
    assert G["Loonie"]["Abra"]["weight"] == 2


def test_network_node_battle_count():
    G = build_battle_network(_battles())
    # Loonie: 2x vs Abra + 1x vs Shehyee = 3 appearances
    assert G.nodes["Loonie"]["battle_count"] == 3


def test_network_skips_nan_and_self_matchups():
    G = build_battle_network(_battles())
    # The (None, "Y") row and the ("X", "X") self-match are both dropped.
    assert "Y" not in G.nodes
    assert "X" not in G.nodes


def test_network_is_undirected():
    G = build_battle_network(_battles())
    assert G.has_edge("Abra", "Loonie")  # order-independent


def test_network_requires_emcee_columns():
    with pytest.raises(ValueError):
        build_battle_network(pd.DataFrame({"foo": [1]}))


def test_network_uses_participants_when_battle_ids_are_available():
    G = build_battle_network(_dated_battles(), manual_matchups=_manual_no_show())

    assert G.nodes["Makii"]["battle_count"] == 1
    assert "Aelekz" not in G.nodes
    assert "Fongger" not in G.nodes
    assert not G.has_edge("Makii", "Fongger")


# ---------------------------------------------------------------------------
# build_battle_participants
# ---------------------------------------------------------------------------

def test_battle_participants_marks_regular_battles_as_appeared():
    participants = build_battle_participants(
        _dated_battles().iloc[[0]],
        manual_matchups={},
        rename_map={},
    )

    assert participants["emcee"].tolist() == ["A", "B"]
    assert participants["participation_status"].tolist() == ["appeared", "appeared"]
    assert participants["appearance_credit"].tolist() == [True, True]
    assert participants["battle_credit"].tolist() == [True, True]


def test_battle_participants_models_no_show_and_helper():
    participants = build_battle_participants(
        _dated_battles().iloc[[1]],
        manual_matchups=_manual_no_show(),
        rename_map={},
    ).set_index("participant_slot")

    assert participants.loc["emcee1", "emcee"] == "Makii"
    assert participants.loc["emcee1", "participation_status"] == "appeared"
    assert bool(participants.loc["emcee1", "appearance_credit"]) is True
    assert bool(participants.loc["emcee1", "battle_credit"]) is True

    assert participants.loc["emcee2", "emcee"] == "Fongger"
    assert participants.loc["emcee2", "participation_status"] == "no_show"
    assert bool(participants.loc["emcee2", "appearance_credit"]) is False
    assert bool(participants.loc["emcee2", "battle_credit"]) is False

    assert participants.loc["helper", "emcee"] == "Aelekz"
    assert participants.loc["helper", "participation_status"] == "appeared"
    assert bool(participants.loc["helper", "appearance_credit"]) is True
    assert bool(participants.loc["helper", "battle_credit"]) is False


# ---------------------------------------------------------------------------
# build_emcees_table
# ---------------------------------------------------------------------------

def test_emcees_table_is_sorted_unique_with_ids():
    df = build_emcees_table(_battles())
    # distinct names across both columns (None excluded): Abra, Loonie, Shehyee, X, Y
    assert df["emcee_name"].tolist() == ["Abra", "Loonie", "Shehyee", "X", "Y"]
    assert df["emcee_id"].tolist() == [1, 2, 3, 4, 5]


def test_emcees_table_can_include_helper_participants():
    participants = build_battle_participants(
        _dated_battles().iloc[[1]],
        manual_matchups=_manual_no_show(),
        rename_map={},
    )
    df = build_emcees_table(_dated_battles().iloc[[1]], participants=participants)

    assert "Aelekz" in set(df["emcee_name"])
