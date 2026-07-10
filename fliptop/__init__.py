"""
fliptop

Utilities for scraping, cleaning, and analyzing FlipTop rap battle data.

This package is meant to be imported from the project root, e.g.:

    from fliptop import RAW_DATA_DIR
    from fliptop import build_ft_battles, build_battle_network

so that notebooks and scripts can share the same paths and pipeline.

The main entry points are re-exported here and imported lazily, so a bare
``import fliptop`` stays cheap (it does not eagerly pull in pandas/networkx);
the heavy import happens the first time you actually use one.
"""

from pathlib import Path
from typing import TYPE_CHECKING

# Paths relative to the package location
PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

__all__ = [
    "PACKAGE_ROOT",
    "PROJECT_ROOT",
    "DATA_DIR",
    "RAW_DATA_DIR",
    "PROCESSED_DATA_DIR",
    "build_battle_metadata",
    "build_ft_battles",
    "build_ft_battles_from_metadata",
    "build_excluded_uploads",
    "build_upload_lineage",
    "build_manual_matchup_review_uploads",
    "build_battle_participants",
    "write_battle_participants_table",
    "build_emcees_table",
    "write_emcees_table",
    "build_battle_network",
    "merge_results",
    "validate_battle_metadata",
    "validate_ft_battles",
]

# Lazy public API: name -> (submodule, attribute). Imported on first access so
# `import fliptop` does not eagerly load pandas/networkx.
_LAZY = {
    "build_battle_metadata": (".battles", "build_battle_metadata"),
    "build_ft_battles": (".battles", "build_ft_battles"),
    "build_ft_battles_from_metadata": (".battles", "build_ft_battles_from_metadata"),
    "build_excluded_uploads": (".battles", "build_excluded_uploads"),
    "build_upload_lineage": (".battles", "build_upload_lineage"),
    "build_manual_matchup_review_uploads": (
        ".battles",
        "build_manual_matchup_review_uploads",
    ),
    "build_battle_participants": (".structures", "build_battle_participants"),
    "write_battle_participants_table": (
        ".structures",
        "write_battle_participants_table",
    ),
    "build_emcees_table": (".structures", "build_emcees_table"),
    "write_emcees_table": (".structures", "write_emcees_table"),
    "build_battle_network": (".structures", "build_battle_network"),
    "merge_results": (".annotations", "merge_results"),
    "validate_battle_metadata": (".validate", "validate_battle_metadata"),
    "validate_ft_battles": (".validate", "validate_ft_battles"),
}

if TYPE_CHECKING:  # for type checkers / IDEs only, no runtime import cost
    from .annotations import merge_results
    from .battles import (
        build_battle_metadata,
        build_excluded_uploads,
        build_ft_battles,
        build_ft_battles_from_metadata,
        build_manual_matchup_review_uploads,
        build_upload_lineage,
    )
    from .structures import (
        build_battle_network,
        build_battle_participants,
        build_emcees_table,
        write_battle_participants_table,
        write_emcees_table,
    )
    from .validate import validate_battle_metadata, validate_ft_battles


def __getattr__(name: str):
    """Lazily import re-exported entry points on first access."""
    target = _LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import importlib

    module = importlib.import_module(target[0], __name__)
    return getattr(module, target[1])
