"""Declarative table contracts for pipeline inputs and stage boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

PathLike = str | Path
ColumnKind = Literal["string", "numeric", "datetime", "list"]


class ContractViolation(ValueError):
    """Raised when a table does not satisfy its declared contract."""

    def __init__(
        self,
        contract_name: str,
        problems: list[str],
        *,
        source: PathLike | None = None,
    ) -> None:
        self.contract_name = contract_name
        self.problems = problems
        self.source = Path(source) if source is not None else None
        location = f"{self.source}: " if self.source is not None else ""
        details = "\n".join(f"  - {problem}" for problem in problems)
        super().__init__(f"{location}{contract_name} contract failed:\n{details}")


@dataclass(frozen=True)
class TableContract:
    """A small, immutable rulebook for one dataframe-shaped table."""

    name: str
    columns: tuple[str, ...]
    allow_extra_columns: bool = True
    ordered_columns: bool = False
    allow_empty: bool = True
    unique_by: tuple[str, ...] = ()
    non_blank: tuple[str, ...] = ()
    kinds: tuple[tuple[str, ColumnKind], ...] = ()
    allowed_values: tuple[tuple[str, frozenset[str]], ...] = ()

    def column_problems(self, actual_columns) -> list[str]:
        """Return schema-only problems for a dataframe or CSV header."""
        actual = [] if actual_columns is None else list(actual_columns)
        missing = [column for column in self.columns if column not in actual]
        unexpected = [column for column in actual if column not in self.columns]
        problems: list[str] = []

        if missing:
            problems.append(f"missing required columns: {', '.join(missing)}")
        if unexpected and not self.allow_extra_columns:
            problems.append(f"unexpected columns: {', '.join(unexpected)}")
        if (
            self.ordered_columns
            and not missing
            and not unexpected
            and actual != list(self.columns)
        ):
            problems.append("columns are out of order")
        if problems:
            problems.insert(0, f"expected columns: {', '.join(self.columns)}")
        return problems

    def problems(self, frame: pd.DataFrame) -> list[str]:
        """Return every structural problem found in ``frame``."""
        problems = self.column_problems(frame.columns)
        if problems:
            return problems
        if frame.empty and not self.allow_empty:
            problems.append("table is empty")
            return problems

        for column in self.non_blank:
            values = frame[column]
            blank = values.isna() | values.astype("string").str.strip().eq("")
            count = int(blank.sum())
            if count:
                problems.append(f"{column} has {count} blank value(s)")

        if self.unique_by:
            duplicate = frame.duplicated(subset=list(self.unique_by), keep=False)
            count = int(duplicate.sum())
            if count:
                key = ", ".join(self.unique_by)
                problems.append(f"{count} row(s) have duplicate key [{key}]")

        for column, kind in self.kinds:
            values = frame[column]
            present = values[values.notna()]
            if kind == "string":
                invalid = ~present.map(lambda value: isinstance(value, str))
            elif kind == "numeric":
                invalid = pd.to_numeric(present, errors="coerce").isna()
            elif kind == "datetime":
                invalid = pd.to_datetime(
                    present,
                    errors="coerce",
                    format="mixed",
                    utc=True,
                ).isna()
            elif kind == "list":
                invalid = ~present.map(lambda value: isinstance(value, list))
            else:  # pragma: no cover - the type annotation prevents this
                raise ValueError(f"unsupported contract kind: {kind}")
            count = int(invalid.sum())
            if count:
                problems.append(f"{column} has {count} value(s) that are not {kind}")

        for column, allowed in self.allowed_values:
            values = frame[column]
            present = values[values.notna()].astype(str).str.strip()
            invalid_values = sorted(set(present) - set(allowed))
            if invalid_values:
                shown = ", ".join(repr(value) for value in invalid_values[:5])
                suffix = "" if len(invalid_values) <= 5 else " ..."
                problems.append(
                    f"{column} contains invalid value(s): {shown}{suffix}; "
                    f"allowed: {', '.join(sorted(allowed))}"
                )

        return problems

    def require(
        self,
        frame: pd.DataFrame,
        *,
        source: PathLike | None = None,
    ) -> pd.DataFrame:
        """Return ``frame`` unchanged, or raise with all contract problems."""
        problems = self.problems(frame)
        if problems:
            raise ContractViolation(self.name, problems, source=source)
        return frame

    def require_columns(
        self,
        actual_columns,
        *,
        source: PathLike | None = None,
    ) -> None:
        """Validate only a CSV header without first constructing a dataframe."""
        problems = self.column_problems(actual_columns)
        if problems:
            raise ContractViolation(self.name, problems, source=source)


RAW_YOUTUBE_COLUMNS = (
    "id",
    "title",
    "description",
    "upload_date",
    "view_count",
    "duration",
    "url",
    "likeCount",
    "commentCount",
    "tags",
)
RAW_YOUTUBE_UPLOADS = TableContract(
    name="raw YouTube uploads",
    columns=RAW_YOUTUBE_COLUMNS,
    allow_extra_columns=False,
    allow_empty=False,
    unique_by=("id",),
    non_blank=("id", "title", "upload_date", "duration", "url"),
    kinds=(
        ("id", "string"),
        ("title", "string"),
        ("description", "string"),
        ("upload_date", "datetime"),
        ("view_count", "numeric"),
        ("duration", "string"),
        ("url", "string"),
        ("likeCount", "numeric"),
        ("commentCount", "numeric"),
        ("tags", "list"),
    ),
)

RAW_EVENT_COLUMNS = ("matchup", "event_name", "event_description", "video_id")
RAW_EVENT_METADATA = TableContract(
    name="raw FlipTop event metadata",
    columns=RAW_EVENT_COLUMNS,
    allow_extra_columns=False,
    allow_empty=False,
    unique_by=("video_id",),
    non_blank=RAW_EVENT_COLUMNS,
    kinds=tuple((column, "string") for column in RAW_EVENT_COLUMNS),
)

VERSETRACKER_COLUMNS = ("event_name", "event_date", "source_url")
VERSETRACKER_EVENT_DATES = TableContract(
    name="VerseTracker event dates",
    columns=VERSETRACKER_COLUMNS,
    allow_extra_columns=False,
    unique_by=("event_name",),
    non_blank=VERSETRACKER_COLUMNS,
    kinds=(
        ("event_name", "string"),
        ("event_date", "datetime"),
        ("source_url", "string"),
    ),
)

ALIASES_COLUMNS = ("alias", "canonical")
EMCEE_ALIASES = TableContract(
    name="emcee aliases",
    columns=ALIASES_COLUMNS,
    allow_extra_columns=False,
)

EVENT_LOCATION_COLUMNS = ("event_name", "event_location", "note")
EVENT_LOCATION_PATTERN_COLUMNS = ("contains", "event_location", "note")
LOCATION_ALIAS_COLUMNS = ("location", "canonical", "note")
EVENT_DATE_OVERRIDE_COLUMNS = ("id", "event_date", "note")
MANUAL_MATCHUP_COLUMNS = (
    "id",
    "emcee1",
    "emcee2",
    "helper_emcee",
    "emcee1_status",
    "emcee2_status",
    "helper_status",
    "note",
)
UPLOAD_DECISION_COLUMNS = ("id", "decision", "reason", "note", "active")
EXCLUSION_RULE_COLUMNS = (
    "rule_id",
    "pattern",
    "match_type",
    "excluded_reason",
    "exit_category",
    "note",
    "active",
)
RESULTS_COLUMNS = (
    "id",
    "battle_type",
    "winner",
    "votes_winner",
    "votes_loser",
    "votes_nv",
    "votes_ot",
    "overtime",
    "notes",
)


def exact_csv_contract(name: str, columns: tuple[str, ...]) -> TableContract:
    """Build the strict header contract used by maintained CSV tables."""
    return TableContract(
        name=name,
        columns=columns,
        allow_extra_columns=False,
        ordered_columns=True,
    )


EMCEE_ALIASES_CSV = exact_csv_contract("emcee aliases", ALIASES_COLUMNS)
EVENT_LOCATIONS_CSV = exact_csv_contract("event location overrides", EVENT_LOCATION_COLUMNS)
EVENT_LOCATION_PATTERNS_CSV = exact_csv_contract(
    "event location patterns",
    EVENT_LOCATION_PATTERN_COLUMNS,
)
LOCATION_ALIASES_CSV = exact_csv_contract("location aliases", LOCATION_ALIAS_COLUMNS)
EVENT_DATES_CSV = exact_csv_contract("event date overrides", EVENT_DATE_OVERRIDE_COLUMNS)
MANUAL_MATCHUPS_CSV = exact_csv_contract("manual matchups", MANUAL_MATCHUP_COLUMNS)
UPLOAD_DECISIONS_CSV = exact_csv_contract("upload decisions", UPLOAD_DECISION_COLUMNS)
EXCLUSION_RULES_CSV = exact_csv_contract("exclusion rules", EXCLUSION_RULE_COLUMNS)
RESULTS_CSV = exact_csv_contract("battle results", RESULTS_COLUMNS)


PREPARED_UPLOADS = TableContract(
    name="prepared uploads",
    columns=(
        "id",
        "title",
        "description",
        "upload_date",
        "duration_seconds",
        "duration_hms",
        "url",
        "yt_raw_title",
    ),
    unique_by=("id",),
    non_blank=("id", "title", "upload_date", "duration_seconds", "url", "yt_raw_title"),
    kinds=(
        ("id", "string"),
        ("title", "string"),
        ("description", "string"),
        ("upload_date", "datetime"),
        ("duration_seconds", "numeric"),
        ("url", "string"),
        ("yt_raw_title", "string"),
    ),
)

PARSED_MATCHUPS = TableContract(
    name="parsed matchups",
    columns=PREPARED_UPLOADS.columns + ("matchup", "emcee1", "emcee2", "matchup_clean"),
    unique_by=("id",),
    non_blank=PREPARED_UPLOADS.non_blank + ("matchup", "emcee1", "emcee2", "matchup_clean"),
    kinds=PREPARED_UPLOADS.kinds
    + (
        ("matchup", "string"),
        ("emcee1", "string"),
        ("emcee2", "string"),
        ("matchup_clean", "string"),
    ),
)

EVENT_ENRICHED_UPLOADS = TableContract(
    name="event-enriched uploads",
    columns=PARSED_MATCHUPS.columns
    + ("event_name", "event_date", "event_location_clean", "event_date_source"),
    unique_by=("id",),
    non_blank=PARSED_MATCHUPS.non_blank + ("event_name", "event_location_clean"),
    kinds=PARSED_MATCHUPS.kinds
    + (
        ("event_name", "string"),
        ("event_date", "datetime"),
        ("event_location_clean", "string"),
        ("event_date_source", "string"),
    ),
    allowed_values=(("event_date_source", frozenset({"website", "description"})),),
)
