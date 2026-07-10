"""
Tests for CSV-backed exclusion rule loading.
"""

from __future__ import annotations

import pytest

from fliptop.rules import (
    compile_exclusion_pattern,
    first_matching_rule,
    load_event_exclusion_rules,
    load_exclusion_rules,
    load_title_exclusion_rules,
)


def test_default_rule_tables_load_expected_rules():
    title_rules = load_title_exclusion_rules()
    event_rules = load_event_exclusion_rules()

    assert "title_beatbox" in {rule.rule_id for rule in title_rules}
    assert "event_process_of_illumination" in {rule.rule_id for rule in event_rules}


def test_rule_loader_preserves_order_and_skips_inactive_rows(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text(
        "\n".join(
            [
                "rule_id,pattern,match_type,excluded_reason,exit_category,note,active",
                "first,abc,substring,reason,not_battle,first note,true",
                "inactive,zzz,substring,reason,not_battle,inactive note,false",
                "second,a.c,regex,reason,not_battle,second note,true",
            ]
        ),
        encoding="utf-8",
    )

    rules = load_exclusion_rules(path)

    assert [rule.rule_id for rule in rules] == ["first", "second"]
    assert compile_exclusion_pattern(rules).search("ABC")
    assert first_matching_rule("xx a-c yy", rules)[0].rule_id == "second"


def test_rule_loader_rejects_duplicate_ids(tmp_path):
    path = tmp_path / "rules.csv"
    path.write_text(
        "\n".join(
            [
                "rule_id,pattern,match_type,excluded_reason,exit_category,note,active",
                "same,abc,substring,reason,not_battle,note,true",
                "same,def,substring,reason,not_battle,note,true",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate rule_id"):
        load_exclusion_rules(path)
