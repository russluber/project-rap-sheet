"""
fliptop.emcee_table

Utilities for building and writing emcee-level datasets from the battle-level dataset df_battles.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd

PathLike = str | Path


def build_emcees_table(df_battles: pd.DataFrame) -> pd.DataFrame:
    """
    Build an emcee table with stable IDs.
    """

    emcees1 = set(df_battles["emcee1"].dropna().unique())
    emcees2 = set(df_battles["emcee2"].dropna().unique())

    emcees = sorted(emcees1.union(emcees2))

    df_emcees = pd.DataFrame({
        "emcee_id": range(1, len(emcees) + 1),
        "emcee_name": emcees
    })

    return df_emcees


def write_emcees_table(df_battles, out_path):
    """
    Write emcee table to CSV.
    """
    df_emcees = build_emcees_table(df_battles)
    df_emcees.to_csv(out_path, index=False)