"""Tests for shared table contracts."""

import pandas as pd
import pytest

import fliptop
from fliptop.contracts import CONTRACT_REGISTRY, ContractViolation, TableContract, contract_versions


def test_package_exports_contract_api():
    assert fliptop.ContractViolation is ContractViolation
    assert fliptop.TableContract is TableContract


def test_contract_reports_multiple_problems_together():
    contract = TableContract(
        name="example",
        columns=("id", "status", "count"),
        allow_extra_columns=False,
        unique_by=("id",),
        non_blank=("id",),
        kinds=(("id", "string"), ("count", "numeric")),
        allowed_values=(("status", frozenset({"ready", "blocked"})),),
    )
    frame = pd.DataFrame(
        {
            "id": ["same", "same"],
            "status": ["ready", "mystery"],
            "count": [1, "not-a-number"],
        }
    )

    with pytest.raises(ContractViolation) as exc_info:
        contract.require(frame, source="example.csv")

    message = str(exc_info.value)
    assert "example.csv" in message
    assert "duplicate key [id]" in message
    assert "not numeric" in message
    assert "'mystery'" in message


def test_contract_rejects_schema_drift_before_row_checks():
    contract = TableContract(
        name="example",
        columns=("id", "value"),
        allow_extra_columns=False,
        ordered_columns=True,
    )

    with pytest.raises(ContractViolation, match="missing required columns: value"):
        contract.require(pd.DataFrame({"id": ["one"], "surprise": [1]}))


def test_contract_returns_the_original_valid_frame():
    contract = TableContract(
        name="example",
        columns=("id", "when", "tags"),
        allow_empty=False,
        unique_by=("id",),
        non_blank=("id",),
        kinds=(("id", "string"), ("when", "datetime"), ("tags", "list")),
    )
    frame = pd.DataFrame(
        {"id": ["one"], "when": ["2026-01-02"], "tags": [["battle"]]}
    )

    assert contract.require(frame) is frame


def test_header_contract_can_validate_without_loading_rows():
    contract = TableContract(
        name="example",
        columns=("first", "second"),
        allow_extra_columns=False,
        ordered_columns=True,
    )

    with pytest.raises(ContractViolation, match="columns are out of order"):
        contract.require_columns(["second", "first"])


def test_contract_registry_has_stable_version_for_every_boundary():
    versions = contract_versions()

    assert set(versions) == set(CONTRACT_REGISTRY)
    assert versions["raw.youtube_uploads"] == 1
    assert versions["pipeline.finalize_battle_metadata"] == 1
