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
    "PipelineRun",
    "build_pipeline_run",
    "PipelineInputs",
    "load_pipeline_inputs",
    "CandidateArtifacts",
    "build_candidate_artifacts",
    "ReleaseBlockedError",
    "require_releasable",
    "ContractViolation",
    "TableContract",
    "build_battle_metadata",
    "build_ft_battles",
    "build_ft_battles_from_metadata",
    "build_excluded_uploads",
    "build_upload_lineage",
    "build_manual_matchup_review_uploads",
    "build_pipeline_stage_summary",
    "build_pipeline_stage_drops",
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
    "PipelineRun": (".pipeline", "PipelineRun"),
    "build_pipeline_run": (".pipeline", "build_pipeline_run"),
    "PipelineInputs": (".inputs", "PipelineInputs"),
    "load_pipeline_inputs": (".inputs", "load_pipeline_inputs"),
    "CandidateArtifacts": (".release", "CandidateArtifacts"),
    "build_candidate_artifacts": (".release", "build_candidate_artifacts"),
    "ReleaseBlockedError": (".release", "ReleaseBlockedError"),
    "require_releasable": (".release", "require_releasable"),
    "ContractViolation": (".contracts", "ContractViolation"),
    "TableContract": (".contracts", "TableContract"),
    "build_battle_metadata": (".battles", "build_battle_metadata"),
    "build_ft_battles": (".publish", "build_ft_battles"),
    "build_ft_battles_from_metadata": (".publish", "build_ft_battles_from_metadata"),
    "build_excluded_uploads": (".lineage", "build_excluded_uploads"),
    "build_upload_lineage": (".lineage", "build_upload_lineage"),
    "build_manual_matchup_review_uploads": (
        ".lineage",
        "build_manual_matchup_review_uploads",
    ),
    "build_pipeline_stage_summary": (".lineage", "build_pipeline_stage_summary"),
    "build_pipeline_stage_drops": (".lineage", "build_pipeline_stage_drops"),
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
    from .battles import build_battle_metadata
    from .contracts import ContractViolation, TableContract
    from .inputs import PipelineInputs, load_pipeline_inputs
    from .lineage import (
        build_excluded_uploads,
        build_manual_matchup_review_uploads,
        build_pipeline_stage_drops,
        build_pipeline_stage_summary,
        build_upload_lineage,
    )
    from .pipeline import PipelineRun, build_pipeline_run
    from .publish import build_ft_battles, build_ft_battles_from_metadata
    from .release import (
        CandidateArtifacts,
        ReleaseBlockedError,
        build_candidate_artifacts,
        require_releasable,
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
