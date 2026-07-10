"""
Shared pytest fixtures for the fliptop test suite.

Most unit tests build their own tiny inline DataFrames; the fixtures here are
for the end-to-end test that exercises the real pipeline against the committed
raw data under ``data/raw``.
"""

from __future__ import annotations

import pytest

from fliptop import RAW_DATA_DIR
from fliptop.battles import build_battle_metadata
from fliptop.publish import build_ft_battles


@pytest.fixture(scope="session")
def raw_data_dir():
    """Path to the committed raw data directory."""
    return RAW_DATA_DIR


@pytest.fixture(scope="session")
def battle_metadata(raw_data_dir):
    """
    The full battle metadata table built once from the committed raw data.

    Session-scoped so the (relatively slow) pipeline runs a single time and is
    shared across every end-to-end assertion.
    """
    return build_battle_metadata(raw_dir=raw_data_dir)


@pytest.fixture(scope="session")
def ft_battles(raw_data_dir):
    """
    The final result-enriched ft_battles table built once from committed data.
    """
    return build_ft_battles(raw_dir=raw_data_dir)
