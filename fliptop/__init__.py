"""
fliptop

Utilities for scraping, cleaning, and analyzing FlipTop rap battle data.

This package is meant to be imported from the project root, e.g.:

    from fliptop import RAW_DATA_DIR
    from fliptop.data_cleaning import build_df_battles

so that notebooks and scripts can share the same paths and pipeline.
"""

from pathlib import Path

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
]