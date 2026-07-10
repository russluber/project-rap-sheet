"""
fliptop.structures

Structures derived from the battle-level table (ft_battles).

These are reusable, deterministic building blocks - they shape ft_battles into
analysis-ready form but do not themselves perform analysis (that lives in
notebooks). Three structures live here today:

  - the emcee table  (one row per emcee, with a stable id)
  - the battle participants table (one row per emcee participation)
  - the battle network (an undirected weighted graph of who battled whom)

Future analysis-ready structures (e.g. a per-emcee career table for survival
analysis) belong here too.

Typical usage:

    from fliptop import RAW_DATA_DIR
    from fliptop.battles import build_ft_battles
    from fliptop.structures import (
        build_battle_participants,
        build_emcees_table,
        build_battle_network,
    )

    ft_battles = build_ft_battles(raw_dir=RAW_DATA_DIR)
    participants = build_battle_participants(ft_battles)
    df_emcees = build_emcees_table(ft_battles)
    G = build_battle_network(ft_battles)
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from pathlib import Path

import networkx as nx
import pandas as pd

from .overrides import load_manual_matchups
from .rename_map import load_rename_map

PathLike = str | Path
ManualMatchupMap = dict[str, dict[str, str | None]]


# ---------------------------------------------------------------------------
# Emcee table
# ---------------------------------------------------------------------------

def build_emcees_table(
    ft_battles: pd.DataFrame,
    participants: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build an emcee table with stable ids.

    Collects every distinct name across emcee1 and emcee2, plus optional
    long-format participant rows, sorts them, and assigns a 1-based emcee_id.
    """
    emcees1 = set(ft_battles["emcee1"].dropna().unique())
    emcees2 = set(ft_battles["emcee2"].dropna().unique())

    emcees = sorted(emcees1.union(emcees2))
    if participants is not None and "emcee" in participants.columns:
        participant_emcees = set(participants["emcee"].dropna().unique())
        emcees = sorted(set(emcees).union(participant_emcees))

    return pd.DataFrame(
        {
            "emcee_id": range(1, len(emcees) + 1),
            "emcee_name": emcees,
        }
    )


def write_emcees_table(
    ft_battles: pd.DataFrame,
    out_path: PathLike,
    participants: pd.DataFrame | None = None,
) -> None:
    """Build the emcee table from ft_battles and write it to CSV."""
    build_emcees_table(ft_battles, participants=participants).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Battle participants
# ---------------------------------------------------------------------------

PARTICIPANT_COLUMNS = [
    "battle_id",
    "matchup",
    "event_name",
    "event_date",
    "upload_date",
    "emcee",
    "participant_slot",
    "participant_role",
    "participation_status",
    "appearance_credit",
    "battle_credit",
]


def _canonical_name(name: str | None, rename_map: dict[str, str]) -> str | None:
    if name is None or pd.isna(name):
        return None
    name = str(name).strip()
    if not name:
        return None
    return rename_map.get(name, name)


def _battle_key(value) -> str | None:
    if pd.isna(value):
        return None
    return str(value)


def _participant_common(row: pd.Series) -> dict[str, object]:
    return {
        "battle_id": _battle_key(row.get("id")),
        "matchup": row.get("matchup", pd.NA),
        "event_name": row.get("event_name", pd.NA),
        "event_date": row.get("event_date", pd.NaT),
        "upload_date": row.get("upload_date", pd.NaT),
    }


def _participant_row(
    row: pd.Series,
    *,
    emcee: str | None,
    slot: str,
    role: str,
    status: str,
    battle_credit: bool,
) -> dict[str, object] | None:
    if emcee is None or pd.isna(emcee):
        return None
    emcee = str(emcee).strip()
    if not emcee:
        return None

    out = _participant_common(row)
    out.update(
        {
            "emcee": emcee,
            "participant_slot": slot,
            "participant_role": role,
            "participation_status": status,
            "appearance_credit": status == "appeared",
            "battle_credit": battle_credit,
        }
    )
    return out


def build_battle_participants(
    ft_battles: pd.DataFrame,
    manual_matchups: ManualMatchupMap | None = None,
    rename_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Build a long participant table for event-history / survival analysis.

    Regular battles emit two appeared scheduled participants. Manual no-show
    battles emit the scheduled emcee who appeared, the scheduled no-show emcee
    without appearance credit, and the helper/substitute with appearance credit
    but no official battle credit.
    """
    _validate_columns(ft_battles, ("id", "emcee1", "emcee2"))
    if manual_matchups is None:
        manual_matchups = load_manual_matchups()
    if rename_map is None:
        rename_map = load_rename_map()

    rows: list[dict[str, object]] = []
    for _, battle in ft_battles.iterrows():
        battle_id = _battle_key(battle.get("id"))
        manual = manual_matchups.get(str(battle_id)) if battle_id is not None else None

        if manual and manual.get("emcee1") and manual.get("emcee2"):
            candidates = [
                _participant_row(
                    battle,
                    emcee=battle.get("emcee1"),
                    slot="emcee1",
                    role="scheduled",
                    status=str(manual["emcee1_status"]),
                    battle_credit=manual["emcee1_status"] == "appeared",
                ),
                _participant_row(
                    battle,
                    emcee=battle.get("emcee2"),
                    slot="emcee2",
                    role="scheduled",
                    status=str(manual["emcee2_status"]),
                    battle_credit=manual["emcee2_status"] == "appeared",
                ),
            ]
            helper = _canonical_name(manual.get("helper_emcee"), rename_map)
            if helper is not None:
                candidates.append(
                    _participant_row(
                        battle,
                        emcee=helper,
                        slot="helper",
                        role="helper",
                        status=str(manual["helper_status"]),
                        battle_credit=False,
                    )
                )
        else:
            candidates = [
                _participant_row(
                    battle,
                    emcee=battle.get("emcee1"),
                    slot="emcee1",
                    role="scheduled",
                    status="appeared",
                    battle_credit=True,
                ),
                _participant_row(
                    battle,
                    emcee=battle.get("emcee2"),
                    slot="emcee2",
                    role="scheduled",
                    status="appeared",
                    battle_credit=True,
                ),
            ]

        rows.extend(row for row in candidates if row is not None)

    return pd.DataFrame(rows, columns=PARTICIPANT_COLUMNS)


def write_battle_participants_table(
    ft_battles: pd.DataFrame,
    out_path: PathLike,
    manual_matchups: ManualMatchupMap | None = None,
    rename_map: dict[str, str] | None = None,
) -> None:
    """Build the participant table from ft_battles and write it to CSV."""
    build_battle_participants(
        ft_battles,
        manual_matchups=manual_matchups,
        rename_map=rename_map,
    ).to_csv(out_path, index=False)


# ---------------------------------------------------------------------------
# Battle network
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = ("emcee1", "emcee2")
PARTICIPANT_NETWORK_COLUMNS = ("battle_id", "emcee", "battle_credit")


def _normalize_pair(emcee1: str, emcee2: str) -> tuple[str, str]:
    """Return a stable undirected matchup key for two emcee names."""
    return tuple(sorted((emcee1, emcee2)))


def _validate_columns(ft_battles: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in ft_battles.columns]
    if missing:
        missing_str = ", ".join(missing)
        raise ValueError(f"ft_battles is missing required columns: {missing_str}")


def _build_network_from_participants(participants: pd.DataFrame) -> nx.Graph:
    _validate_columns(participants, PARTICIPANT_NETWORK_COLUMNS)

    graph = nx.Graph()
    credited = participants[participants["battle_credit"].eq(True)].copy()
    battle_counts = Counter(credited["emcee"])

    for emcee, count in battle_counts.items():
        graph.add_node(emcee, battle_count=count)

    pairs: list[tuple[str, str]] = []
    for _, group in credited.groupby("battle_id"):
        emcees = sorted(set(group["emcee"].dropna().astype(str).str.strip()))
        emcees = [emcee for emcee in emcees if emcee]
        if len(emcees) != 2:
            continue
        pairs.append(_normalize_pair(emcees[0], emcees[1]))

    for (emcee1, emcee2), weight in Counter(pairs).items():
        graph.add_edge(emcee1, emcee2, weight=weight)

    return graph


def build_battle_network(
    ft_battles: pd.DataFrame,
    *,
    participants: pd.DataFrame | None = None,
    manual_matchups: ManualMatchupMap | None = None,
) -> nx.Graph:
    """
    Build an undirected weighted battle network from ft_battles.

    Nodes are emcees. An edge indicates that two emcees have battled.
    Edge weight is the number of battles between the two emcees.

    Node attributes:
      - battle_count: total number of battle appearances

    Edge attributes:
      - weight: number of battles between the two emcees
    """
    if participants is not None:
        return _build_network_from_participants(participants)

    if "id" in ft_battles.columns:
        participants = build_battle_participants(
            ft_battles,
            manual_matchups=manual_matchups,
        )
        return _build_network_from_participants(participants)

    _validate_columns(ft_battles, REQUIRED_COLUMNS)
    pairs: list[tuple[str, str]] = []
    battle_counts: Counter[str] = Counter()

    for row in ft_battles.loc[:, REQUIRED_COLUMNS].itertuples(index=False):
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
