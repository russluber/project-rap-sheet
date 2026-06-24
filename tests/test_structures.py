"""
Tests for the df_battles-derived structures in fliptop.structures:

    build_battle_network
    build_emcees_table
"""

from __future__ import annotations

import pandas as pd
import pytest

from fliptop.structures import build_battle_network, build_emcees_table


def _battles():
    return pd.DataFrame(
        {
            "emcee1": ["Loonie", "Loonie", "Abra", "Shehyee", None, "X"],
            "emcee2": ["Abra", "Abra", "Shehyee", "Loonie", "Y", "X"],
        }
    )


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


# ---------------------------------------------------------------------------
# build_emcees_table
# ---------------------------------------------------------------------------

def test_emcees_table_is_sorted_unique_with_ids():
    df = build_emcees_table(_battles())
    # distinct names across both columns (None excluded): Abra, Loonie, Shehyee, X, Y
    assert df["emcee_name"].tolist() == ["Abra", "Loonie", "Shehyee", "X", "Y"]
    assert df["emcee_id"].tolist() == [1, 2, 3, 4, 5]
