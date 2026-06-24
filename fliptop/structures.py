"""
fliptop.structures

Structures derived from the battle-level table (df_battles).

These are reusable, deterministic building blocks - they shape df_battles into
analysis-ready form but do not themselves perform analysis (that lives in
notebooks). Two structures live here today:

  - the emcee table  (one row per emcee, with a stable id)
  - the battle network (an undirected weighted graph of who battled whom)

Future analysis-ready structures (e.g. a per-emcee career table for survival
analysis) belong here too.

Typical usage:

    from fliptop import RAW_DATA_DIR
    from fliptop.data_cleaning import build_df_battles
    from fliptop.structures import build_emcees_table, build_battle_network

    df_battles = build_df_battles(raw_dir=RAW_DATA_DIR)
    df_emcees = build_emcees_table(df_battles)
    G = build_battle_network(df_battles)
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Iterable

import networkx as nx
import pandas as pd

PathLike = str | Path


# ---------------------------------------------------------------------------
# Emcee table
# ---------------------------------------------------------------------------

def build_emcees_table(df_battles: pd.DataFrame) -> pd.DataFrame:
    """
    Build an emcee table with stable ids.

    Collects every distinct name across emcee1 and emcee2, sorts them, and
    assigns a 1-based emcee_id.
    """
    emcees1 = set(df_battles["emcee1"].dropna().unique())
    emcees2 = set(df_battles["emcee2"].dropna().unique())

    emcees = sorted(emcees1.union(emcees2))

    return pd.DataFrame(
        {
            "emcee_id": range(1, len(emcees) + 1),
            "emcee_name": emcees,
        }
    )


def write_emcees_table(df_battles: pd.DataFrame, out_path: PathLike) -> None:
    """Build the emcee table from df_battles and write it to CSV."""
    build_emcees_table(df_battles).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Battle network
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = ("emcee1", "emcee2")


def _normalize_pair(emcee1: str, emcee2: str) -> tuple[str, str]:
    """Return a stable undirected matchup key for two emcee names."""
    return tuple(sorted((emcee1, emcee2)))


def _validate_columns(df_battles: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in df_battles.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"df_battles is missing required columns: {missing_str}")


def build_battle_network(df_battles: pd.DataFrame) -> nx.Graph:
    """
    Build an undirected weighted battle network from df_battles.

    Nodes are emcees. An edge indicates that two emcees have battled.
    Edge weight is the number of battles between the two emcees.

    Node attributes:
      - battle_count: total number of battle appearances

    Edge attributes:
      - weight: number of battles between the two emcees
    """
    _validate_columns(df_battles, REQUIRED_COLUMNS)

    pairs: list[tuple[str, str]] = []
    battle_counts: Counter[str] = Counter()

    for row in df_battles.loc[:, REQUIRED_COLUMNS].itertuples(index=False):
        emcee1, emcee2 = row

        if pd.isna(emcee1) or pd.isna(emcee2):
            continue

        emcee1 = str(emcee1).strip()
        emcee2 = str(emcee2).strip()

        if not emcee1 or not emcee2:
            continue

        if emcee1 == emcee2:
            continue

        battle_counts[emcee1] += 1
        battle_counts[emcee2] += 1
        pairs.append(_normalize_pair(emcee1, emcee2))

    edge_weights = Counter(pairs)

    graph = nx.Graph()

    for emcee, count in battle_counts.items():
        graph.add_node(emcee, battle_count=count)

    for (emcee1, emcee2), weight in edge_weights.items():
        graph.add_edge(emcee1, emcee2, weight=weight)

    return graph
