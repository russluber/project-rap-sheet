"""
fliptop.rules

Loaders for reviewable pipeline rule tables under ``data/rules/``.

These rules are part of the core wrangling definition, not ad hoc debug data:
they decide which uploads are excluded by title keyword and which event
categories are outside the project's analysis scope. Keeping them in CSV makes
the decisions auditable without editing Python code.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from re import Pattern

from . import DATA_DIR
from .contracts import EXCLUSION_RULES_CSV

PathLike = str | Path

RULES_DIR = DATA_DIR / "rules"

TITLE_EXCLUSIONS_CSV = RULES_DIR / "title_exclusions.csv"
EVENT_EXCLUSIONS_CSV = RULES_DIR / "event_exclusions.csv"

MATCH_TYPES = {"substring", "regex"}
TRUE_VALUES = {"1", "true", "yes", "y"}
FALSE_VALUES = {"0", "false", "no", "n"}
NEVER_MATCH_RE = re.compile(r"a\A")


@dataclass(frozen=True)
class ExclusionRule:
    """One active title/event exclusion rule loaded from ``data/rules``."""

    rule_id: str
    pattern: str
    match_type: str
    excluded_reason: str
    exit_category: str
    note: str


def _parse_active(value: str | None, *, path: Path, lineno: int) -> bool:
    value = (value or "").strip().casefold()
    if value in TRUE_VALUES:
        return True
    if value in FALSE_VALUES:
        return False
    allowed = ", ".join(sorted(TRUE_VALUES | FALSE_VALUES))
    raise ValueError(f"{path}:{lineno}: active must be one of: {allowed}")


def _compile_rule_regex(rule: ExclusionRule) -> Pattern[str]:
    pattern = re.escape(rule.pattern) if rule.match_type == "substring" else rule.pattern
    return re.compile(pattern, flags=re.IGNORECASE)


def load_exclusion_rules(path: PathLike) -> list[ExclusionRule]:
    """
    Load active exclusion rules from a CSV table, preserving row order.

    Required columns:
    ``rule_id, pattern, match_type, excluded_reason, exit_category, note, active``.
    Blank rows are skipped. Inactive rows are allowed and ignored, so a rule can
    be reviewed without deleting its history.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"required exclusion rules file not found: {path}")

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        EXCLUSION_RULES_CSV.require_columns(reader.fieldnames, source=path)
        required = set(EXCLUSION_RULES_CSV.columns)

        rules: list[ExclusionRule] = []
        seen_ids: set[str] = set()
        for lineno, row in enumerate(reader, start=2):
            values = {col: (row.get(col) or "").strip() for col in required}
            if not any(values.values()):
                continue

            rule_id = values["rule_id"]
            pattern = values["pattern"]
            match_type = values["match_type"].casefold()
            excluded_reason = values["excluded_reason"]
            exit_category = values["exit_category"]
            note = values["note"]
            active = _parse_active(values["active"], path=path, lineno=lineno)

            missing = [
                col
                for col, value in {
                    "rule_id": rule_id,
                    "pattern": pattern,
                    "match_type": match_type,
                    "excluded_reason": excluded_reason,
                    "exit_category": exit_category,
                    "note": note,
                }.items()
                if not value
            ]
            if missing:
                raise ValueError(f"{path}:{lineno}: missing required fields: {missing}")
            if rule_id in seen_ids:
                raise ValueError(f"{path}:{lineno}: duplicate rule_id {rule_id!r}")
            if match_type not in MATCH_TYPES:
                allowed = ", ".join(sorted(MATCH_TYPES))
                raise ValueError(f"{path}:{lineno}: match_type must be one of: {allowed}")

            rule = ExclusionRule(
                rule_id=rule_id,
                pattern=pattern,
                match_type=match_type,
                excluded_reason=excluded_reason,
                exit_category=exit_category,
                note=note,
            )
            _compile_rule_regex(rule)
            seen_ids.add(rule_id)
            if active:
                rules.append(rule)

    return rules


def load_title_exclusion_rules(
    path: PathLike = TITLE_EXCLUSIONS_CSV,
) -> list[ExclusionRule]:
    """Load title-keyword exclusion rules from ``data/rules/title_exclusions.csv``."""
    return load_exclusion_rules(path)


def load_event_exclusion_rules(
    path: PathLike = EVENT_EXCLUSIONS_CSV,
) -> list[ExclusionRule]:
    """Load event-category exclusion rules from ``data/rules/event_exclusions.csv``."""
    return load_exclusion_rules(path)


def compile_exclusion_pattern(rules: list[ExclusionRule]) -> Pattern[str]:
    """Compile active rules into one case-insensitive regex for vector filters."""
    parts = [
        re.escape(rule.pattern) if rule.match_type == "substring" else f"(?:{rule.pattern})"
        for rule in rules
    ]
    if not parts:
        return NEVER_MATCH_RE
    return re.compile("|".join(parts), flags=re.IGNORECASE)


def first_matching_rule(
    value: object,
    rules: list[ExclusionRule],
) -> tuple[ExclusionRule, str] | None:
    """Return ``(rule, matched_text)`` for the first rule matching ``value``."""
    if not isinstance(value, str):
        return None

    for rule in rules:
        match = _compile_rule_regex(rule).search(value)
        if match:
            return rule, match.group(0)
    return None
