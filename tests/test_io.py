"""Tests for crash-safe file replacement."""

import pytest

from fliptop.io import atomic_output_path


def test_atomic_output_replaces_destination_after_success(tmp_path):
    destination = tmp_path / "data.csv"
    destination.write_text("old", encoding="utf-8")

    with atomic_output_path(destination) as temporary:
        temporary.write_text("new", encoding="utf-8")
        assert destination.read_text(encoding="utf-8") == "old"

    assert destination.read_text(encoding="utf-8") == "new"


def test_atomic_output_preserves_destination_after_failure(tmp_path):
    destination = tmp_path / "data.csv"
    destination.write_text("old", encoding="utf-8")

    with (
        pytest.raises(RuntimeError, match="serialization failed"),
        atomic_output_path(destination) as temporary,
    ):
        temporary.write_text("partial", encoding="utf-8")
        raise RuntimeError("serialization failed")

    assert destination.read_text(encoding="utf-8") == "old"
    assert list(tmp_path.glob(".data.csv.*.tmp")) == []
